import logging
from abc import ABC, abstractmethod
from enum import Enum, auto
from threading import Lock, Condition
from typing import Dict, Optional, List

from runtools.runcore import JobRun, plugins, paths, connector
from runtools.runcore.common import InvalidStateError
from runtools.runcore.connector import EnvironmentConnector, LocalConnectorLayout, StandardLocalConnectorLayout, \
    create_layout_dirs, DEF_ENV_ID
from runtools.runcore.db import sqlite, PersistingObserver, SortCriteria
from runtools.runcore.job import JobInstance, JobInstanceNotifications, InstanceStageEvent, InstanceTransitionEvent, \
    InstanceOutputEvent, JobInstanceDelegate
from runtools.runcore.plugins import Plugin
from runtools.runcore.util import ensure_tuple_copy, lock
from runtools.runcore.util.err import run_isolated_collect_exceptions
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY
from runtools.runcore.util.socket import SocketClient
from runtools.runjob import instance, JobInstanceHook
from runtools.runjob.events import EventDispatcher
from runtools.runjob.server import RemoteCallServer

log = logging.getLogger(__name__)


def _create_plugins(names):
    plugins.load_modules(names)
    fetched_plugins = Plugin.fetch_plugins(names)
    # TODO complete


class Environment(EnvironmentConnector, ABC):

    @abstractmethod
    def create_instance(self, *args, **kwargs):
        """TODO Should the instance be auto-started to prevent environment waiting infinitely?"""
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


class _JobInstanceManaged(JobInstanceDelegate):

    def __init__(self, env, wrapped: JobInstance):
        super().__init__(wrapped)
        self.env = env
        self.state: _InstanceState = _InstanceState.NONE

    # noinspection PyProtectedMember
    def run(self):
        with self.env._lock:
            if self.env._closing:
                raise EnvironmentClosed
            self.state = _InstanceState.STARTED

        try:
            return super().run()
        finally:
            self.env._detach_instance(self.instance_id, self.env._transient)

    def allows_env_closing(self):
        return self.state in (_InstanceState.NONE, _InstanceState.DETACHED)


class EnvironmentBase(Environment, ABC):
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
     - Close/exit method can be called multiple times (returns immediately if closing by another thread or already closed)
    Condition:
     - Close/exit operations wait for all instances to be detached
    """
    OBSERVERS_PRIORITY = 1000

    def __init__(self, features=(), transient=True):
        self._features = features
        self._transient = transient
        self._lock = Lock()
        self._detached_condition = Condition(self._lock)
        # Fields guarded by lock below:
        self._managed_instances: Dict[str, _JobInstanceManaged] = {}
        self._opened = False
        self._closing = False

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

    def create_instance(self, job_id: str, phases, status_tracker=None, *,
                        instance_id=None, run_id=None, tail_buffer=None,
                        pre_run_hook: Optional[JobInstanceHook] = None,
                        post_run_hook: Optional[JobInstanceHook] = None,
                        user_params=None) -> JobInstance:
        """
        Create a new job instance within this environment.

        Args:
            job_id: Job identifier
            phases: Job execution phases to run
            status_tracker: Optional status tracker for the job
            instance_id: Optional instance identifier (auto-generated if not provided)
            run_id: Optional run identifier (defaults to instance_id if not provided)
            tail_buffer: Optional buffer for output tailing
            pre_run_hook: Optional hook called before running the instance
            post_run_hook: Optional hook called after running the instance
            user_params: Optional user-defined parameters

        Returns:
            JobInstance: The created job instance

        Raises:
            InvalidStateError: If the environment is not opened or is already closed
            ValueError: If job_id is empty or None
        """
        inst = instance.create(
            job_id=job_id,
            phases=phases,
            environment=self,
            status_tracker=status_tracker,
            instance_id=instance_id,
            run_id=run_id,
            tail_buffer=tail_buffer,
            pre_run_hook=pre_run_hook,
            post_run_hook=post_run_hook,
            **(user_params or {})
        )
        return self._add_instance(inst)

    @property
    def instances(self):
        """
        Mutable list copy of all job instances currently managed by this container.
        Note that this returns only instances inside of this container, not necessarily all instances in the environment.

        Returns:
            A list of all job instances currently managed by this container.
        """
        with self._lock:
            return list(self._managed_instances.values())

    def _add_instance(self, job_instance):
        """
        Add a job instance to the environment.

        Args:
            job_instance (JobInstance): The job instance to be added.

        Returns:
            JobInstance: The added job instance.

        Raises:
            InvalidStateError: If the context is not opened or is already closed.
            ValueError: If a job instance with the same ID already exists in the environment.
        """
        with self._lock:
            if not self._opened:
                raise InvalidStateError("Cannot add job instance: environment container not opened")
            if self._closing:
                raise InvalidStateError("Cannot add job instance: environment container already closed")
            if job_instance.instance_id in self._managed_instances:
                raise ValueError("Instance with ID already exists in environment")

            job_instance = _JobInstanceManaged(self, job_instance)
            self._managed_instances[job_instance.instance_id] = job_instance

        # TODO Exception must be caught here to prevent inconsistent state and possibility in get stuck in close method:
        self._on_added(job_instance)
        for feature in self._features:
            feature.on_instance_added(job_instance)

        # #  TODO optional plugins
        # if cfg.plugins_enabled and cfg.plugins_load:
        #     plugins.register_new_job_instance(job_instance, cfg.plugins_load,
        #                                       plugin_module_prefix=EXT_PLUGIN_MODULE_PREFIX)

        # IMPORTANT:
        #   1. Add observer first and only then check for the termination to prevent termination miss by the race condition
        #   2. Priority should be set to be the lowest from all observers, however the current implementation
        #      will work regardless of the priority as the removal of the observers doesn't affect
        #      iteration/notification (`Notification` class)

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

            if job_instance.state != _InstanceState.DETACHING:
                job_instance.state = _InstanceState.DETACHING
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
                job_instance.stat = _InstanceState.DETACHED
                self._detached_condition.notify()

        return job_instance

    def _on_removed(self, job_instance):
        pass

    def close(self):
        interrupt_received = False
        with self._detached_condition:
            if self._closing:
                return

            self._closing = True
            while not all((i.allows_env_closing() for i in self._managed_instances.values())):
                try:
                    self._detached_condition.wait()
                except KeyboardInterrupt:
                    interrupt_received = True
                    break

        log.debug(f"[closing_environment] environment=[{self}]")
        run_isolated_collect_exceptions(
            "Errors on environment features closing",
            *(feature.on_close for feature in self._features),
            suppress=interrupt_received
        )

        if interrupt_received:
            raise KeyboardInterrupt


def isolated(persistence=None, *, lock_factory=None, features=None, transient=True) -> Environment:
    persistence = persistence or sqlite.create(':memory:')
    lock_factory = lock_factory or lock.default_memory_lock_factory()
    return _IsolatedEnvironment(persistence, lock_factory, ensure_tuple_copy(features), transient)


class _IsolatedEnvironment(JobInstanceNotifications, EnvironmentBase):

    def __init__(self, persistence, lock_factory, features, transient=True):
        JobInstanceNotifications.__init__(self)
        EnvironmentBase.__init__(self, features, transient=transient)
        self._persistence = persistence
        self._lock_factory = lock_factory
        self._persisting_observer = PersistingObserver(persistence)

    def open(self):
        EnvironmentBase.open(self)  # Always first
        self._persistence.open()

    def _on_added(self, job_instance):
        job_instance.add_observer_stage(self._persisting_observer)
        job_instance.add_observer_stage(self._stage_notification.observer_proxy,
                                        EnvironmentBase.OBSERVERS_PRIORITY - 1)
        job_instance.add_observer_transition(self._transition_notification.observer_proxy,
                                             EnvironmentBase.OBSERVERS_PRIORITY - 1)
        job_instance.add_observer_output(self._output_notification.observer_proxy,
                                         EnvironmentBase.OBSERVERS_PRIORITY - 1)

    def _on_removed(self, job_instance):
        job_instance.remove_observer_output(self._output_notification.observer_proxy)
        job_instance.remove_observer_transition(self._transition_notification.observer_proxy)
        job_instance.remove_observer_stage(self._stage_notification.observer_proxy)
        job_instance.remove_observer_stage(self._persisting_observer)

    def get_active_runs(self, run_match=None) -> List[JobRun]:
        return [i.snapshot() for i in self.get_instances(run_match)]

    def get_instances(self, run_match=None) -> List[JobInstance]:
        return [i for i in self.instances if not run_match or run_match(i.snapshot())]

    def read_history_runs(self, run_match, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._persistence.read_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def read_history_stats(self, run_match=None):
        return self._persistence.read_history_stats(run_match)

    def lock(self, lock_id):
        return self._lock_factory(lock_id)

    def close(self):
        run_isolated_collect_exceptions(
            "Errors during closing isolated environment",
            lambda: EnvironmentBase.close(self),
            # <- Always execute first as the method is waiting until it can be closed
            self._persistence.close
        )


class LocalNodeLayout(LocalConnectorLayout, ABC):

    @property
    @abstractmethod
    def socket_server_rpc(self):
        pass

    @property
    @abstractmethod
    def provider_sockets_listener_phase(self):
        pass


class StandardLocalNodeLayout(StandardLocalConnectorLayout, LocalNodeLayout):

    @classmethod
    def create(cls, env_id, root_dir=None):
        return cls(*create_layout_dirs(env_id, root_dir, cls.NODE_DIR_PREFIX))

    def __init__(self, env_dir, node_dir):
        super().__init__(env_dir, node_dir)

    @property
    def socket_server_rpc(self):
        return self.component_dir / self.socket_name_server_rpc

    @property
    def socket_name_listener_events(self):
        return 'listener-events.sock'

    @property
    def socket_name_listener_stage(self):
        return 'listener-stage.sock'

    @property
    def socket_name_listener_phase(self):
        return 'listener-phase.sock'

    @property
    def socket_name_listener_output(self):
        return 'listener-output.sock'

    def _provider_sockets_listener(self, socket_name_listener):
        file_names = [self.socket_name_listener_events, socket_name_listener]
        return paths.files_in_subdir_provider(self.env_dir, file_names)

    @property
    def provider_sockets_listener_stage(self):
        return self._provider_sockets_listener(self.socket_name_listener_stage)

    @property
    def provider_sockets_listener_phase(self):
        return self._provider_sockets_listener(self.socket_name_listener_phase)

    @property
    def provider_sockets_listener_output(self):
        return self._provider_sockets_listener(self.socket_name_listener_output)


def local(env_id=DEF_ENV_ID, persistence=None, node_layout=None, *, lock_factory=None, features=None, transient=True):
    layout = node_layout or StandardLocalNodeLayout.create(env_id)
    persistence = persistence or sqlite.create(':memory:')  # TODO Load correct database
    local_connector = connector.local(env_id, persistence, layout)

    api = RemoteCallServer(layout.socket_server_rpc)
    event_dispatcher = EventDispatcher(SocketClient(), {
        InstanceStageEvent.EVENT_TYPE: layout.provider_sockets_listener_stage,
        InstanceTransitionEvent.EVENT_TYPE: layout.provider_sockets_listener_phase,
        InstanceOutputEvent.EVENT_TYPE: layout.provider_sockets_listener_output,
    })
    lock_factory = lock_factory or lock.default_file_lock_factory()
    features = ensure_tuple_copy(features)
    return LocalNode(env_id, local_connector, persistence, api, event_dispatcher, lock_factory, features, transient)


class LocalNode(EnvironmentBase):
    """
    Environment node implementation that uses composition to delegate environment connector functionality.
    """

    def __init__(self, env_id, local_connector, persistence, rpc_server, event_dispatcher, lock_factory, features,
                 transient):
        EnvironmentBase.__init__(self, features, transient=transient)
        self._env_id = env_id
        self._connector = local_connector
        self._rpc_server = rpc_server
        self._event_dispatcher = event_dispatcher
        self._lock_factory = lock_factory
        self._persisting_observer = PersistingObserver(persistence)

    def open(self):
        EnvironmentBase.open(self)  # Execute first for opened only once check
        self._connector.open()
        self._rpc_server.start()

    def get_active_runs(self, run_match=None):
        return self._connector.get_active_runs(run_match)

    def get_instances(self, run_match=None):
        return self._connector.get_instances(run_match)

    def get_instance(self, instance_id):
        for inst in self.instances:
            if inst.instance_id == instance_id:
                return inst

        return self._connector.get_instance(instance_id)

    def read_history_runs(self, run_match, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._connector.read_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def read_history_stats(self, run_match=None):
        return self._connector.read_history_stats(run_match)

    def add_observer_all_events(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._connector.add_observer_all_events(observer, priority)

    def remove_observer_all_events(self, observer):
        self._connector.remove_observer_all_events(observer)

    def add_observer_stage(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._connector.add_observer_stage(observer, priority)

    def remove_observer_stage(self, observer):
        self._connector.remove_observer_stage(observer)

    def add_observer_transition(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._connector.add_observer_transition(observer, priority)

    def remove_observer_transition(self, observer):
        self._connector.remove_observer_transition(observer)

    def add_observer_output(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._connector.add_observer_output(observer, priority)

    def remove_observer_output(self, observer):
        self._connector.remove_observer_output(observer)

    def _on_added(self, job_instance):
        job_instance.add_observer_stage(self._persisting_observer)
        self._rpc_server.register_instance(job_instance)
        job_instance.add_observer_all_events(self._event_dispatcher)

    def _on_removed(self, job_instance):
        job_instance.remove_observer_all_events(self._event_dispatcher)
        self._rpc_server.unregister_instance(job_instance)
        job_instance.remove_observer_stage(self._persisting_observer)

    def lock(self, lock_id):
        # TODO Method to separate type
        return self._lock_factory(paths.lock_path(f"{lock_id}.lock", True))

    def close(self):
        run_isolated_collect_exceptions(
            "Errors during closing runnable local environment",
            lambda: EnvironmentBase.close(self),  # Always execute first as the method is waiting until it can be closed
            self._rpc_server.close,
            self._event_dispatcher.close,
            self._connector.close,  # Keep last as it deletes the node directory
        )


class EnvironmentClosed(InvalidStateError):
    pass
