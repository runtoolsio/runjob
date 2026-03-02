import logging
from abc import ABC, abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum, auto
from pathlib import Path
from threading import Lock, Condition
from typing import Dict, Optional, List, Callable, override

from runtools.runcore import plugins, paths, connector, db, util
from runtools.runcore.connector import EnvironmentConnector, LocalConnectorLayout, StandardLocalConnectorLayout, \
    ensure_component_dir
from runtools.runcore.criteria import SortOption
from runtools.runcore.db import sqlite, PersistingObserver, PERSISTING_OBSERVER_PRIORITY, DuplicateInstanceError
from runtools.runcore.env import EnvironmentConfigUnion, LocalEnvironmentConfig, \
    InProcessEnvironmentConfig, get_env_config, EnvironmentNotFoundError, \
    DEFAULT_LOCAL_ENVIRONMENT
from runtools.runcore.output import DEFAULT_TAIL_BUFFER_SIZE
from runtools.runcore.err import InvalidStateError, run_isolated_collect_exceptions
from runtools.runcore.job import JobRun, JobInstance, InstanceObservableNotifications, InstanceNotifications, \
    InstanceLifecycleEvent, InstancePhaseEvent, InstanceOutputEvent, InstanceControlEvent, InstanceStatusEvent, \
    JobInstanceDelegate, InstanceLifecycleObserver
from runtools.runcore.paths import ConfigFileNotFoundError
from runtools.runcore.plugins import Plugin
from runtools.runcore.util import to_tuple, lock
from runtools.runcore.util.socket import DatagramSocketClient
from runtools.runjob import instance, output
from runtools.runjob.events import EventDispatcher
from runtools.runcore.run import Stage
from runtools.runjob.output import OutputRouter, InMemoryTailBuffer
from runtools.runjob.server import LocalInstanceServer

log = logging.getLogger(__name__)


def _create_plugins(names):
    plugins.load_modules(names)
    fetched_plugins = Plugin.fetch_plugins(names)
    # TODO complete


class EnvironmentNode(EnvironmentConnector, ABC):

    @abstractmethod
    def create_instance(self, instance_id, root_phase, **kwargs):
        pass

    @abstractmethod
    def lock(self, lock_id):
        """TODO to separate type"""


class Feature(ABC):

    def on_open(self):
        pass

    def on_close(self):
        pass

    @abstractmethod
    def on_instance_added(self, job_instance):
        pass

    @abstractmethod
    def on_instance_removed(self, job_instance):
        pass


class _InstanceState(Enum):
    NONE = auto()
    STARTED = auto()
    DETACHING = auto()
    DETACHED = auto()


# noinspection PyProtectedMember
class JobInstanceManaged(JobInstanceDelegate):

    def __init__(self, env: 'EnvironmentNodeBase', wrapped: JobInstance):
        super().__init__(wrapped)
        self._env: 'EnvironmentNodeBase' = env
        self._state: _InstanceState = _InstanceState.NONE

    def run(self, in_background=False):
        with self._env._lock:
            if self._state != _InstanceState.NONE:
                raise AlreadyStarted
            if self._env._closing:
                raise EnvironmentClosed
            self._state = _InstanceState.STARTED

        if in_background:
            return self._env._run_in_executor(self._run_and_detach)
        else:
            return self._run_and_detach()

    def run_in_background(self):
        return self.run(True)

    def _run_and_detach(self):
        try:
            return super().run()
        finally:
            self._env._detach_instance(self.id, self._env._transient)

    def _allows_env_closing(self):
        return self._state in (_InstanceState.NONE, _InstanceState.DETACHED)

class EnvironmentNodeBase(EnvironmentNode, ABC):
    """
    Implementation details
    ----------------------
    Lock:
     - Ensures the container is opened once
     - Prevents adding already contained instances (by instance ID)
     - Ensures an instance cannot be added after the container initiated closing
     - Thread-safe instances list creation
     - Thread-safe removal (manual and on-finished removals are safe to use together)
     - Close hook is executed only when all instances are detached
     - Close/exit method can be called multiple times
       (returns immediately if closing by another thread or already closed)
    Condition:
     - Close/exit operations wait for all instances to be detached
    """
    OBSERVERS_PRIORITY = 1000

    def __init__(self, env_id, persistence, output_stores=(), tail_buffer_size=DEFAULT_TAIL_BUFFER_SIZE,
                 features=(), transient=True):
        self._env_id = env_id
        self._persistence = persistence
        self._output_stores = tuple(output_stores)
        self._tail_buffer_size = tail_buffer_size
        self._features = features
        self._transient = transient
        self._lock = Lock()
        self._detached_condition = Condition(self._lock)
        # Fields guarded by lock below:
        self._managed_instances: Dict[str, JobInstanceManaged] = {}
        self._opened = False
        self._closing = False
        self._executor = None

    @property
    def env_id(self):
        return self._env_id

    @property
    @override
    def output_backends(self):
        return self._output_stores

    def _run_in_executor(self, fnc):
        with self._lock:
            if not self._executor:
                self._executor = ThreadPoolExecutor()

        return self._executor.submit(fnc)

    def open(self):
        """
        Open the environment container and execute open hooks of all added features.
        """
        with self._lock:
            if self._opened:
                raise InvalidStateError("Environment has been already opened")
            self._opened = True

        for feature in self._features:
            feature.on_open()

    def create_instance(self, instance_id, root_phase, *, output_sink=None, status_tracker=None, user_params=None) \
            -> JobInstanceManaged:
        """
        Create a new job instance within this environment.

        Args:
            instance_id (InstanceID): Instance identifier
            root_phase: Root phase of the job instance
            output_sink: Optional output sink (for text parsing, use OutputSink with ParsingPreprocessor)
            status_tracker: Optional status tracker for the job
            user_params: Optional user-defined parameters

        Returns:
            JobInstance: The created job instance

        Raises:
            InvalidStateError: If the environment is not opened or is already closed
            ValueError: If job_id is empty or None
        """
        writers = [store.create_writer(instance_id) for store in self._output_stores]
        tail_buffer = InMemoryTailBuffer(max_bytes=self._tail_buffer_size) if self._tail_buffer_size else None
        output_router = OutputRouter(tail_buffer=tail_buffer, storages=writers)
        inst = instance.create(
            instance_id, self, root_phase,
            activate=False,
            output_sink=output_sink,
            output_router=output_router,
            status_tracker=status_tracker,
            **(user_params or {})
        )
        return self._add_instance(inst)

    @property
    def instances(self):
        """
        Mutable list copy of all job instances currently managed by this container.
        Note that this returns only instances inside of this container, not necessarily all instances
        in the environment.

        Returns:
            A list of all job instances currently managed by this container.
        """
        with self._lock:
            return list(self._managed_instances.values())

    def _add_instance(self, job_instance) -> JobInstanceManaged:
        """
        Add a job instance to the environment.

        Args:
            job_instance (JobInstance): The job instance to be added.

        Returns:
            JobInstance: The added job instance.

        Raises:
            InvalidStateError: If the context is not opened or is already closed.
            DuplicateInstanceError: If the instance ID already exists in the environment or in persistence.
        """
        with self._lock:
            if not self._opened:
                raise InvalidStateError("Cannot add job instance: environment container not opened")
            if self._closing:
                raise InvalidStateError("Cannot add job instance: environment container already closed")
            if job_instance.id in self._managed_instances:
                raise DuplicateInstanceError(job_instance.id)

            job_instance = JobInstanceManaged(self, job_instance)
            self._managed_instances[job_instance.id] = job_instance

        try:
            self._persistence.init_job_run(job_instance.id, job_instance.metadata.user_params)
            self._on_added(job_instance)
            for feature in self._features:
                feature.on_instance_added(job_instance)
            job_instance.activate()
        except Exception:
            with self._lock:
                self._managed_instances.pop(job_instance.id, None)
            raise

        return job_instance

    def _on_added(self, job_instance):
        pass

    def _detach_instance(self, job_instance_id, remove):
        """
        Implementation note: A race condition can cause this method to be executed twice with the same ID
        """
        detach = False
        removed = False
        with self._lock:
            job_instance = self._managed_instances.get(job_instance_id)
            if not job_instance:
                return None

            # noinspection PyProtectedMember
            if job_instance._state != _InstanceState.DETACHING:
                job_instance._state = _InstanceState.DETACHING
                detach = True
            if remove:
                del self._managed_instances[job_instance_id]
                removed = True

        if removed:
            self._on_removed(job_instance)
            for feature in self._features:
                feature.on_instance_removed(job_instance)

        if detach:
            with self._detached_condition:
                job_instance._state = _InstanceState.DETACHED
                self._detached_condition.notify()

        return job_instance

    def _on_removed(self, job_instance):
        pass

    def _shutdown_executor(self):
        if self._executor:
            self._executor.shutdown()

    def close(self):
        interrupt_received = False
        with self._detached_condition:
            # TODO Consider adding a timeout to prevent indefinite blocking if an instance never reaches DETACHED
            if self._closing:
                return

            self._closing = True
            # noinspection PyProtectedMember
            while not all((i._allows_env_closing() for i in self._managed_instances.values())):
                try:
                    self._detached_condition.wait()
                except KeyboardInterrupt:
                    interrupt_received = True
                    break

        log.debug(f"[closing_environment] environment=[{self}]")
        run_isolated_collect_exceptions(
            "Errors on environment features closing",
            *(feature.on_close for feature in self._features),
            self._shutdown_executor,
            suppress=interrupt_received
        )

        if interrupt_received:
            raise KeyboardInterrupt


RETENTION_OBSERVER_PRIORITY = PERSISTING_OBSERVER_PRIORITY + 10  # After persistence so the run is stored first


class RetentionObserver(InstanceLifecycleObserver):
    """Enforces retention policy on both persistence and output stores after each run ends."""

    def __init__(self, persistence, output_stores, retention_policy):
        self._persistence = persistence
        self._output_stores = output_stores
        self._retention_policy = retention_policy

    def instance_lifecycle_update(self, event: InstanceLifecycleEvent):
        if event.new_stage != Stage.ENDED:
            return

        job_id = event.job_run.metadata.job_id

        def _enforce_persistence():
            self._persistence.enforce_retention(job_id, self._retention_policy)

        def _enforce_store(store):
            return lambda: store.enforce_retention(job_id, self._retention_policy)

        run_isolated_collect_exceptions(
            f"Retention errors for job {job_id}",
            _enforce_persistence,
            *(_enforce_store(s) for s in self._output_stores),
            suppress=True,
        )


def connect(env_id=None):
    """Connect to an environment node, falling back to default local config when no config is found.

    Unlike ``connector.connect()``, this always falls back to a local node so that
    ``@job`` decorated functions work zero-config.
    """
    try:
        return create(get_env_config(env_id))
    except (EnvironmentNotFoundError, ConfigFileNotFoundError):
        return create(LocalEnvironmentConfig(id=env_id or DEFAULT_LOCAL_ENVIRONMENT))


def in_process(env_id=None, persistence=None, *, lock_factory=None, features=None, transient=True) -> 'InProcessNode':
    """Create an in-process environment node for testing and development.

    Args:
        env_id (str): Environment identifier. Defaults to a unique generated ID.
        persistence (Persistence): Persistence backend. Defaults to in-memory SQLite.
        lock_factory: Factory for memory-based locks. Defaults to the standard memory lock factory.
        features: Features to attach to the node lifecycle.
        transient (bool): Whether instances are removed from the node after detaching. Defaults to True.
    """
    env_id = env_id or "in_process_" + util.unique_timestamp_hex()
    persistence = persistence or sqlite.create(env_id=env_id, database=':memory:')
    lock_factory = lock_factory or lock.default_memory_lock_factory()
    return InProcessNode(env_id, persistence, lock_factory, to_tuple(features), transient)


class InProcessNode(EnvironmentNodeBase):

    def __init__(self, env_id, persistence, lock_factory, features, transient=True):
        self._notifications = InstanceObservableNotifications()
        EnvironmentNodeBase.__init__(self, env_id, persistence, features=features, transient=transient)
        self._lock_factory = lock_factory
        self._persisting_observer = PersistingObserver(persistence)

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        return self._notifications

    @property
    def persistence_enabled(self) -> bool:
        return self._persistence.enabled

    def open(self):
        EnvironmentNodeBase.open(self)  # Always first
        self._persistence.open()

    def _on_added(self, job_instance):
        job_instance.notifications.add_observer_lifecycle(self._persisting_observer, PERSISTING_OBSERVER_PRIORITY)
        self._notifications.bind_to(job_instance.notifications, EnvironmentNodeBase.OBSERVERS_PRIORITY - 1)

    def _on_removed(self, job_instance):
        self._notifications.unbind_from(job_instance.notifications)
        job_instance.notifications.remove_observer_lifecycle(self._persisting_observer)

    def get_active_runs(self, run_match=None) -> List[JobRun]:
        return [i.snap() for i in self.get_instances(run_match)]

    def get_instance(self, instance_id) -> Optional[JobInstance]:
        for inst in self.instances:
            if inst.id == instance_id:
                return inst
        return None

    def get_instances(self, run_match=None) -> List[JobInstance]:
        return [i for i in self.instances if not run_match or run_match(i.snap())]

    def read_history_runs(self, run_match, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._persistence.read_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def iter_history_runs(self, run_match=None, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._persistence.iter_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def read_history_stats(self, run_match=None):
        return self._persistence.read_history_stats(run_match)

    def lock(self, lock_id):
        return self._lock_factory(lock_id)

    def close(self):
        run_isolated_collect_exceptions(
            "Errors during closing isolated environment",
            lambda: EnvironmentNodeBase.close(self),
            # <- Always execute first as the method is waiting until it can be closed
            self._persistence.close
        )


class LocalNodeLayout(LocalConnectorLayout, ABC):
    """Base class for local node layouts"""

    @property
    @abstractmethod
    def server_socket_path(self) -> Path:
        """Return the server RPC socket path"""
        pass

    @property
    @abstractmethod
    def listener_lifecycle_sockets_provider(self) -> Callable:
        """Return the provider for lifecycle listener sockets"""
        pass

    @property
    @abstractmethod
    def listener_phase_sockets_provider(self) -> Callable:
        """Return the provider for phase listener sockets"""
        pass

    @property
    @abstractmethod
    def listener_output_sockets_provider(self) -> Callable:
        """Return the provider for output listener sockets"""
        pass


class StandardLocalNodeLayout(StandardLocalConnectorLayout, LocalNodeLayout):
    """
    Standard implementation of a local node layout.

    Extends the connector layout with additional functionality specific to nodes,
    such as server sockets and additional listener providers.

    Example structure:
    /tmp/runtools/env/{env_id}/                      # Directory for the specific environment (env_dir)
    │
    └── node_abc123/                                 # Node directory (component_dir)
        ├── server-rpc.sock                          # Node's RPC server socket
        ├── client-rpc.sock                          # Node's RPC client socket
        ├── listener-events.sock                     # Node's events listener socket
        └── ...                                      # Other node-specific sockets
    """

    def __init__(self, env_dir: Path, component_name: str):
        """
        Initializes the node layout with environment directory and component name.

        Args:
            env_dir: Directory containing the environment structure.
            component_name: Name of the node subdirectory.
        """
        super().__init__(env_dir=env_dir, component_name=component_name)
        self._listener_lifecycle_socket_name = "listener-lifecycle.sock"
        self._listener_phase_socket_name = "listener-phase.sock"
        self._listener_output_socket_name = "listener-output.sock"
        self._listener_control_socket_name = "listener-control.sock"
        self._listener_status_socket_name = "listener-status.sock"

    @classmethod
    def create(cls, env_id: str, root_dir: Optional[Path] = None, component_prefix: str = "node_"):
        """
        Creates a layout for a new node with a unique component directory.

        Args:
            env_id: Identifier for the environment.
            root_dir: Root directory containing environments or uses the default one.
            component_prefix: Prefix for component directories.

        Returns (StandardLocalNodeLayout): Layout instance for a node.
        """
        return cls(*ensure_component_dir(env_id, component_prefix, root_dir))

    @classmethod
    def from_config(cls, env_config, component_prefix: str = "node_"):
        return cls.create(env_config.id, env_config.layout.root_dir, component_prefix)

    @property
    def server_socket_path(self) -> Path:
        """
        Returns:
            Path: Full path of server domain socket used for sending requests to RPC servers
        """
        return self._component_dir / self._server_socket_name

    def _provider_sockets_listener(self, socket_listener_name) -> Callable:
        """
        Helper method for creating socket providers for different listener types.

        Args:
            socket_listener_name: Socket filename to look for

        Returns:
            Callable: Provider function for the specified socket(s)
        """
        file_names = [self._listener_events_socket_name, socket_listener_name]
        return paths.files_in_subdir_provider(self._env_dir, file_names)

    @property
    def listener_lifecycle_sockets_provider(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to lifecycle listener socket files
        """
        return self._provider_sockets_listener(self._listener_lifecycle_socket_name)

    @property
    def listener_phase_sockets_provider(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to phase listener socket files
        """
        return self._provider_sockets_listener(self._listener_phase_socket_name)

    @property
    def listener_output_sockets_provider(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to output listener socket files
        """
        return self._provider_sockets_listener(self._listener_output_socket_name)

    @property
    def listener_control_sockets_provider(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to control listener socket files
        """
        return self._provider_sockets_listener(self._listener_control_socket_name)

    @property
    def listener_status_sockets_provider(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to status listener socket files
        """
        return self._provider_sockets_listener(self._listener_status_socket_name)


def create(env_config: EnvironmentConfigUnion) -> 'EnvironmentNodeBase':
    """Create an environment node from configuration.

    Args:
        env_config (EnvironmentConfigUnion): Environment configuration that determines the node type and settings.

    Returns:
        EnvironmentNodeBase: Configured node for the environment.
    """
    persistence = db.create_persistence(env_config.id, env_config.persistence)

    if isinstance(env_config, LocalEnvironmentConfig):
        layout = StandardLocalNodeLayout.from_config(env_config)
        output_stores = output.create_stores(env_config.id, env_config.output.storages)
        return _local(env_config.id, persistence, layout, output_stores,
                      tail_buffer_size=env_config.output.tail_buffer_size,
                      retention_policy=env_config.retention.to_policy())

    if isinstance(env_config, InProcessEnvironmentConfig):
        return in_process(env_config.id, persistence)

    raise AssertionError(f"Unsupported environment config: {type(env_config)}.")


def _local(env_id, persistence, node_layout, output_stores,
           *, tail_buffer_size=DEFAULT_TAIL_BUFFER_SIZE, retention_policy=None,
           lock_factory=None, features=None, transient=True):
    local_connector = connector._local(env_id, persistence, node_layout, output_stores)

    api = LocalInstanceServer(node_layout.server_socket_path)
    event_dispatcher = EventDispatcher(DatagramSocketClient(), {
        InstanceLifecycleEvent.EVENT_TYPE: node_layout.listener_lifecycle_sockets_provider,
        InstancePhaseEvent.EVENT_TYPE: node_layout.listener_phase_sockets_provider,
        InstanceOutputEvent.EVENT_TYPE: node_layout.listener_output_sockets_provider,
        InstanceControlEvent.EVENT_TYPE: node_layout.listener_control_sockets_provider,
        InstanceStatusEvent.EVENT_TYPE: node_layout.listener_status_sockets_provider,
    })
    lock_factory = lock_factory or lock.default_file_lock_factory()
    features = to_tuple(features)
    return LocalNode(env_id, local_connector, persistence, output_stores,
                     api, event_dispatcher, lock_factory, features, transient,
                     tail_buffer_size=tail_buffer_size, retention_policy=retention_policy)


class LocalNode(EnvironmentNodeBase):
    """
    Environment node implementation that uses composition to delegate environment connector functionality.
    """

    def __init__(self, env_id, local_connector, persistence, output_stores, rpc_server, event_dispatcher,
                 lock_factory, features, transient, *, tail_buffer_size=DEFAULT_TAIL_BUFFER_SIZE,
                 retention_policy=None):
        EnvironmentNodeBase.__init__(
            self, env_id, persistence, output_stores=output_stores, tail_buffer_size=tail_buffer_size,
            features=features, transient=transient)
        self._connector = local_connector
        self._rpc_server = rpc_server
        self._event_dispatcher = event_dispatcher
        self._lock_factory = lock_factory
        self._persisting_observer = PersistingObserver(persistence)
        self._retention_observer = (
            RetentionObserver(persistence, output_stores, retention_policy) if retention_policy else None
        )

    @property
    def persistence_enabled(self) -> bool:
        return self._connector.persistence_enabled

    def open(self):
        EnvironmentNodeBase.open(self)  # Execute first for opened only once check
        self._connector.open()
        self._rpc_server.start()

    def get_active_runs(self, run_match=None):
        return self._connector.get_active_runs(run_match)

    def get_instances(self, run_match=None):
        return self._connector.get_instances(run_match)

    def get_instance(self, instance_id):
        for inst in self.instances:
            if inst.id == instance_id:
                return inst

        return self._connector.get_instance(instance_id)

    def read_history_runs(self, run_match, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._connector.read_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def iter_history_runs(self, run_match=None, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._connector.iter_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def read_history_stats(self, run_match=None):
        return self._connector.read_history_stats(run_match)

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        return self._connector.notifications

    def _on_added(self, job_instance):
        job_instance.notifications.add_observer_lifecycle(self._persisting_observer, PERSISTING_OBSERVER_PRIORITY)
        if self._retention_observer:
            job_instance.notifications.add_observer_lifecycle(self._retention_observer, RETENTION_OBSERVER_PRIORITY)
        self._rpc_server.register_instance(job_instance)
        job_instance.notifications.add_observer_all_events(self._event_dispatcher)

    def _on_removed(self, job_instance):
        job_instance.notifications.remove_observer_all_events(self._event_dispatcher)
        self._rpc_server.unregister_instance(job_instance)
        if self._retention_observer:
            job_instance.notifications.remove_observer_lifecycle(self._retention_observer)
        job_instance.notifications.remove_observer_lifecycle(self._persisting_observer)

    def lock(self, lock_id):
        # TODO Method to separate type
        return self._lock_factory(paths.lock_path(f"{lock_id}.lock", True))

    def close(self):
        run_isolated_collect_exceptions(
            "Errors during closing runnable local environment",
            lambda: EnvironmentNodeBase.close(self),  # First: waits until all instances detached
            self._rpc_server.close,
            self._event_dispatcher.close,
            self._connector.close,  # Keep last as it deletes the node directory
        )


class AlreadyStarted(InvalidStateError):
    pass


class EnvironmentClosed(InvalidStateError):
    pass
