import logging
import time
from abc import ABC, abstractmethod
from functools import partial
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum, auto
from threading import Condition, Event, Lock, Thread
from typing import Dict, Optional, List, Tuple, assert_never, override

from runtools.runcore import util
from runtools.runcore.connector import EnvironmentConnector
from runtools.runcore.db import HEARTBEAT_INTERVAL, sqlite
from runtools.runcore.env import EnvironmentKind, LocalEnvironmentConfig, PostgresEnvironmentConfig, \
    EnvironmentEntry, EnvironmentNotFoundError, resolve_env_ref, ensure_environment
from runtools.runcore.err import InvalidStateError, run_isolated_collect_exceptions
from runtools.runcore.job import JobRun, JobInstance, InstanceObservableNotifications, InstanceNotifications, \
    JobInstanceDelegate, InstanceID, \
    DuplicateStrategy, normalize_tags
from runtools.runcore.matching import SortOption
from runtools.runcore.output import DEFAULT_TAIL_BUFFER_SIZE
from runtools.runcore.db.persister import RunStatePersister
from runtools.runcore.transport import InstanceDirectory
from runtools.runcore.plugins import Plugin
from runtools.runcore.util import to_tuple, lock, unique_timestamp_hex
from runtools.runjob import instance, output
from runtools.runjob.output import OutputRouter, InMemoryTailBuffer
from runtools.runjob.transport import InstanceAccessPoint

log = logging.getLogger(__name__)


class EnvironmentNode(EnvironmentConnector, ABC):

    @abstractmethod
    def create_instance(self, job_id, run_id, root_phase, **kwargs):
        pass

    @abstractmethod
    def lock(self, lock_id):
        """Obtain a named exclusive lock for job coordination within this environment."""


class _InstanceState(Enum):
    NONE = auto()
    STARTED = auto()
    DETACHING = auto()
    DETACHED = auto()


# noinspection PyProtectedMember
class JobInstanceManaged(JobInstanceDelegate):

    def __init__(self, env: 'EnvironmentNodeBase', wrapped):
        super().__init__(wrapped)
        self._env: 'EnvironmentNodeBase' = env
        self._state: _InstanceState = _InstanceState.NONE

    def notify_created(self):
        self._wrapped.notify_created()
        return self

    def run(self, in_background=False):
        with self._env._lock:
            if self._state == _InstanceState.STARTED:
                raise AlreadyStarted
            if self._state != _InstanceState.NONE:
                return None
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
            try:
                self._env._finalize_run(self)
            except Exception:
                log.error("Failed to finalize run", extra={"instance": str(self.id)}, exc_info=True)
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
    PERSIST_FLUSH_INTERVAL = 0.25  # Default seconds between coalesced active-state flushes

    def __init__(self, env_id, env_db, output_stores=(), tail_buffer_size=DEFAULT_TAIL_BUFFER_SIZE,
                 features=(), transient=True, persist_flush_interval=PERSIST_FLUSH_INTERVAL,
                 heartbeat_interval=HEARTBEAT_INTERVAL):
        self._env_id = env_id
        self._db = env_db
        self._persister = RunStatePersister(env_db)
        self._persist_flush_interval = persist_flush_interval
        self._heartbeat_interval = heartbeat_interval
        self._persister_stop = Event()
        self._persister_thread = None
        self._output_stores = tuple(output_stores)
        self._tail_buffer_size = tail_buffer_size
        self._features = features
        self._feature_names = tuple(f.name or type(f).__name__ for f in features)
        self._transient = transient
        self._lock = Lock()
        self._idle_condition = Condition(self._lock)
        # Fields guarded by lock below:
        self._reserved_runs: List[Tuple[str, str]] = []
        self._managed_instances: Dict[InstanceID, JobInstanceManaged] = {}
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

    def _flush_persister_loop(self):
        # Heartbeats ride the flush thread on purpose: they attest the persistence lane consumers
        # depend on, so a wedged flush thread reads as lost instead of alive-but-frozen
        next_heartbeat = time.monotonic()  # first touch on the first tick
        while not self._persister_stop.wait(self._persist_flush_interval):
            try:
                self._persister.flush()
            except Exception:
                log.warning("Persister flush failed; retrying next interval", exc_info=True)
            if time.monotonic() >= next_heartbeat:
                try:
                    self._db.touch_heartbeats([i.id for i in self.instances])
                    next_heartbeat = time.monotonic() + self._heartbeat_interval
                except Exception:
                    # next_heartbeat unchanged -> retry on the next flush tick, not in a full interval
                    log.warning("Heartbeat touch failed; retrying next interval", exc_info=True)

    def _close_persister(self):
        self._persister_stop.set()
        if self._persister_thread:
            self._persister_thread.join()
        self._persister.close()  # Final flush of any remaining dirty snapshots

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
        self._open()
        self._persister_thread = Thread(target=self._flush_persister_loop, name="run-persister", daemon=True)
        self._persister_thread.start()  # Started strictly last when everything above succeed

    def _open(self):
        """Subclass transport/database startup. Runs after feature hooks, before the flush loop."""
        # TODO Cleanup hardening: if _open() (or an on_open hook) fails partway, already-opened
        #  feature/transport resources are leaked — failed open should run the matching close hooks.
        # Note: db open lives per-subclass — the per-kind connect functions must open the db to read
        #  the config before composing, while InProcessNode creates its own and opens it here.
        pass

    def _admit_instance(self, job_id, run_id, created_at, user_params, *,
                        tags=(), auto_increment=False):
        with self._lock:
            if not self._opened:
                raise InvalidStateError("Cannot add job instance: environment container not opened")
            if self._closing:
                raise InvalidStateError("Cannot add job instance: environment container already closed")
            self._reserved_runs.append((job_id, run_id))

        try:
            return self._db.init_run(
                job_id, run_id, user_params,
                created_at=created_at, tags=tags, auto_increment=auto_increment)
        except BaseException:
            with self._idle_condition:
                self._reserved_runs.remove((job_id, run_id))
                self._idle_condition.notify()
            raise

    def create_instance(self, job_id, run_id=None, root_phase=None, *, duplicate_strategy=DuplicateStrategy.RAISE,
                        output_processors=(), output_capture=None, status_tracker=None,
                        user_params=None, tags=()) -> JobInstanceManaged:
        """
        Create a new job instance within this environment.

        Args:
            job_id: Job identifier.
            run_id: Run identifier (generated if None).
            root_phase: Root phase of the job instance.
            duplicate_strategy: How to handle duplicates. Defaults to RAISE.
            output_processors (Sequence[OutputProcessor]): Processors for the output chain.
            output_capture: Callable for capturing external output (e.g., log_capture). None to disable.
            status_tracker: Optional status tracker for the job.
            user_params: Optional user-defined parameters.
            tags: User-set labels for grouping/filtering. Normalized at the
                boundary (lowercase, ``#`` stripped, deduped) so a malformed
                tag fails fast before the DB row or instance is constructed.
        """
        run_id = run_id or unique_timestamp_hex()
        reserved = (job_id, run_id)
        # Single source of truth for the run's creation moment: the root phase, from
        # which JobRun.lifecycle.created_at also derives. Threading this same value
        # into the partial DB row and into per-store writer metadata keeps DB,
        # final lifecycle, and S3 retention timestamps consistent.
        created_at = root_phase.created_at
        # Normalize once at the boundary — fail fast on bad input, then re-pass
        # the canonical tuple downstream (DB write + metadata are idempotent).
        normalized_tags = normalize_tags(tags) if tags else ()
        instance_id = self._admit_instance(
            job_id, run_id, created_at, user_params,
            tags=normalized_tags,
            auto_increment=(duplicate_strategy != DuplicateStrategy.RAISE))
        job_instance = None
        try:
            writers = [store.create_sink(instance_id) for store in self._output_stores]
            tail_buffer = InMemoryTailBuffer(max_bytes=self._tail_buffer_size) if self._tail_buffer_size else None
            output_router = OutputRouter(tail_buffer=tail_buffer, sinks=writers)
            create_kwargs = dict(
                output_router=output_router,
                output_processors=output_processors,
                status_tracker=status_tracker,
                features=self._feature_names,
                tags=normalized_tags,
                **(user_params or {})
            )
            if output_capture is not None:
                create_kwargs['output_capture'] = output_capture
            inst = instance.create(instance_id, self, root_phase, **create_kwargs)

            with self._lock:
                self._reserved_runs.remove(reserved)
                job_instance = JobInstanceManaged(self, inst)
                self._managed_instances[job_instance.id] = job_instance

            self._on_added(job_instance)
            for feature in self._features:
                feature.on_instance_added(job_instance)

            self._persister.attach(job_instance.notifications)  # Before CREATED — first flush completes the init row
            job_instance.notify_created()

            if instance_id.ordinal > 1 and duplicate_strategy.stop_reason:
                job_instance.stop(duplicate_strategy.stop_reason)
                try:
                    self._finalize_run(job_instance)
                except Exception:
                    log.error("Failed to finalize run", extra={"instance": str(job_instance.id)}, exc_info=True)
                self._detach_instance(job_instance.id, self._transient)
        except:
            with self._idle_condition:
                if job_instance:
                    self._managed_instances.pop(job_instance.id, None)
                else:
                    self._reserved_runs.remove(reserved)
                self._idle_condition.notify()
            raise

        return job_instance

    def _finalize_run(self, job_instance):
        """Persist the final run snapshot. Called after router close so output locations are finalized."""
        snap = job_instance.snap()
        if snap.lifecycle.is_ended:
            self._db.store_runs(snap)

    def _on_added(self, job_instance):
        pass

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
            try:
                run_isolated_collect_exceptions(
                    "Errors during instance removal",
                    partial(self._on_removed, job_instance),
                    *(partial(feature.on_instance_removed, job_instance) for feature in self._features),
                )
            except ExceptionGroup:
                log.exception("Errors during instance removal: %s", job_instance_id)

        if detach:
            with self._idle_condition:
                job_instance._state = _InstanceState.DETACHED
                self._idle_condition.notify()
            self._persister.detach(job_instance.notifications)

        return job_instance

    def _on_removed(self, job_instance):
        pass

    def read_runs(self, run_match=None, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._db.read_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def iter_runs(self, run_match=None, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._db.iter_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def read_run_stats(self, run_match=None):
        return self._db.read_run_stats(run_match)

    def remove_history_runs(self, run_match):
        active = self.get_active_runs(run_match)
        if active:
            raise ValueError(f"Cannot remove active runs: {', '.join(str(r.instance_id) for r in active)}")
        removed_ids = self._db.remove_runs(run_match)
        for backend in self._output_stores:
            backend.delete_output(*removed_ids)
        return removed_ids

    def _shutdown_executor(self):
        if self._executor:
            self._executor.shutdown()

    def close(self):
        interrupt_received = False
        with self._idle_condition:
            # TODO Consider adding a timeout to prevent indefinite blocking if an instance never reaches DETACHED
            if self._closing:
                return

            self._closing = True
            # noinspection PyProtectedMember
            while self._reserved_runs or not all(
                    i._allows_env_closing() for i in self._managed_instances.values()):
                try:
                    self._idle_condition.wait()
                except KeyboardInterrupt:
                    interrupt_received = True
                    break

        log.debug("Closing environment", extra={"env": self._env_id})
        run_isolated_collect_exceptions(
            "Errors on environment features closing",
            self._close_persister,
            *(feature.on_close for feature in self._features),
            self._shutdown_executor,
            # Output stores own resources (e.g. boto3 S3 client connection pools)
            # they created in create_store. The connector closes read-side backends;
            # this is the parallel write-side cleanup.
            *(store.close for store in self._output_stores),
            suppress=interrupt_received
        )

        if interrupt_received:
            raise KeyboardInterrupt


def connect(env_ref: EnvironmentEntry | str | None = None, *,
            disable_output: tuple[str, ...] = (), tail_buffer_size=None):
    """Connect to an environment node.

    Args:
        env_ref: Environment entry, env_id string, or None for built-in local.
        disable_output: Output storage types to disable for this session (e.g. ("file",), ("all",)).
        tail_buffer_size: Override the default tail buffer size from config.
    """
    entry = resolve_env_ref(env_ref)
    ensure_environment(entry)
    match entry.kind:
        case EnvironmentKind.LOCAL:
            return _connect_local(entry, disable_output=disable_output, tail_buffer_size=tail_buffer_size)
        case EnvironmentKind.POSTGRES:
            return _connect_postgres(entry, disable_output=disable_output, tail_buffer_size=tail_buffer_size)
        case _:
            assert_never(entry.kind)  # new kind added but not wired here


def in_process(env_id=None, env_db=None, *, lock_provider=None, features=None, transient=True) -> 'InProcessNode':
    """Create an in-process environment node for testing and development.

    Args:
        env_id: Environment identifier. Defaults to a unique generated ID.
        env_db: Environment database. Defaults to in-memory SQLite.
        lock_provider: Provider of coordination locks. Defaults to the in-memory provider.
        features: Features to attach to the node lifecycle.
        transient: Whether instances are removed from the node after detaching. Defaults to True.
    """
    env_id = env_id or "in_process_" + util.unique_timestamp_hex()
    env_db = env_db or sqlite.create_memory(env_id)
    lock_provider = lock_provider or lock.MemoryLockProvider()
    return InProcessNode(env_id, env_db, lock_provider, to_tuple(features), transient)


class InProcessNode(EnvironmentNodeBase):

    def __init__(self, env_id, env_db, lock_provider, features, transient=True):
        self._notifications = InstanceObservableNotifications()
        EnvironmentNodeBase.__init__(self, env_id, env_db, features=features, transient=transient)
        self._lock_provider = lock_provider

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        return self._notifications

    def _open(self):
        self._db.open()

    def _on_added(self, job_instance):
        self._notifications.bind_to(job_instance.notifications, EnvironmentNodeBase.OBSERVERS_PRIORITY - 1)

    def _on_removed(self, job_instance):
        self._notifications.unbind_from(job_instance.notifications)

    def get_active_runs(self, run_match=None) -> List[JobRun]:
        return [i.snap() for i in self.get_instances(run_match)]

    def get_instance(self, instance_id) -> Optional[JobInstance]:
        for inst in self.instances:
            if inst.id == instance_id:
                return inst
        return None

    def get_instances(self, run_match=None) -> List[JobInstance]:
        return [i for i in self.instances if not run_match or run_match(i.snap())]

    def lock(self, lock_id):
        return self._lock_provider.lock(lock_id)

    def close(self):
        run_isolated_collect_exceptions(
            "Errors during closing isolated environment",
            lambda: EnvironmentNodeBase.close(self),
            # <- Always execute first as the method is waiting until it can be closed
            self._db.close
        )


class _ComposedNode(EnvironmentNodeBase):
    """Generic environment node composed from its per-kind runtime parts.

    The node owns its job instances plus the sibling-facing :class:`InstanceDirectory`
    (the consumer view of the rest of the environment). The access point exposes
    instances to the env (register / unregister); live reads merge the node's own
    instances with the directory's view, own instances winning (the directory may lag
    for them on polled kinds or duplicate them as proxies on wire kinds).
    """

    def __init__(self, env_id, env_db, access_point: InstanceAccessPoint, directory: InstanceDirectory,
                 lock_provider, output_stores, features, transient,
                 *, tail_buffer_size=DEFAULT_TAIL_BUFFER_SIZE):
        EnvironmentNodeBase.__init__(
            self, env_id, env_db, output_stores=output_stores, tail_buffer_size=tail_buffer_size,
            features=features, transient=transient)
        self._access_point = access_point
        self._directory = directory
        self._lock_provider = lock_provider

    def _open(self):
        self._directory.open()
        self._access_point.start()

    def get_active_runs(self, run_match=None):
        return [i.snap() for i in self.get_instances(run_match)]

    def get_instances(self, run_match=None):
        instances = {i.id: i for i in self._directory.get_instances(run_match)}
        for own in self.instances:
            if not run_match or run_match(own.snap()):
                instances[own.id] = own
        return list(instances.values())

    def get_instance(self, instance_id):
        for inst in self.instances:
            if inst.id == instance_id:
                return inst

        return self._directory.get_instance(instance_id)

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        return self._directory.notifications

    def _on_added(self, job_instance):
        self._access_point.register_instance(job_instance)

    def _on_removed(self, job_instance):
        self._access_point.unregister_instance(job_instance)

    def lock(self, lock_id):
        return self._lock_provider.lock(lock_id)

    def close(self):
        run_isolated_collect_exceptions(
            "Errors during closing environment node",
            lambda: EnvironmentNodeBase.close(self),  # waits for instances to detach
            self._access_point.close,                 # node-only wire resources
            self._directory.close,                    # sibling view (may read the db - close first)
            self._db.close,
        )


def compose(env_id, env_db, access_point: InstanceAccessPoint, directory: InstanceDirectory,
            lock_provider, output_stores, features, transient,
            *, tail_buffer_size=DEFAULT_TAIL_BUFFER_SIZE) -> EnvironmentNode:
    """Internal framework plumbing: construct a concrete node from its runtime parts.

    Consumed by the per-kind connect functions. Returns the abstract
    ``EnvironmentNode`` so callers depend on the interface, not the concrete class.

    Not part of the public API — :func:`connect` or :func:`in_process` are the supported
    entry points for callers that just want a node.
    """
    return _ComposedNode(env_id, env_db, access_point, directory, lock_provider, output_stores, features, transient,
                         tail_buffer_size=tail_buffer_size)


def _connect_local(entry: EnvironmentEntry, *,
                   disable_output: tuple[str, ...] = (), tail_buffer_size=None) -> 'EnvironmentNodeBase':
    """Node for a ``local`` environment: sqlite + unix-socket transport pair + file locks."""
    from runtools.runjob.transport import unix_socket
    # Check before open: opening a missing sqlite file would silently provision a fresh schema
    if not sqlite.exists(entry):
        raise EnvironmentNotFoundError(f"Database for environment '{entry.id}' not found", {entry.id})
    env_db = sqlite.create(entry)
    env_db.open()
    try:
        config = LocalEnvironmentConfig.model_validate(env_db.load_config(entry.id))
        if "all" in disable_output:
            storages = []
        else:
            storages = [s for s in config.output.storages if s.type not in disable_output]
        # Cheap, in-memory builds first; transports allocate component dirs and flocks.
        output_stores = output.create_stores(entry.id, storages)
        effective_tail_buffer_size = tail_buffer_size if tail_buffer_size is not None \
            else config.output.default_tail_buffer_size
        plugin_features = Plugin.create_all(config.plugins) if config.plugins else ()
        lock_provider = lock.FileLockProvider(entry.id)

        sibling_directory, access_point = unix_socket.create_node_transports(entry.id, config.root_dir)
        return compose(entry.id, env_db, access_point, sibling_directory, lock_provider,
                       output_stores, to_tuple(plugin_features), transient=True,
                       tail_buffer_size=effective_tail_buffer_size)
    except BaseException:
        env_db.close()
        raise


def _connect_postgres(entry: EnvironmentEntry, *,
                      disable_output: tuple[str, ...] = (), tail_buffer_size=None) -> 'EnvironmentNodeBase':
    """Node for a ``postgres`` environment: postgres storage + polling directory + advisory locks.

    Instances are exposed by the run-state persister's snapshots, which remote polling
    directories read; inbound commands arrive through the signals mailbox, applied by the
    access point's reconciler (design point 5).
    """
    from runtools.runcore.db import postgres
    from runtools.runcore.proxy import SnapshotJobInstanceProxy
    from runtools.runcore.transport.db import PollingInstanceDirectory
    from runtools.runjob.transport.postgres import PostgresInstanceAccessPoint

    env_db = postgres.create(entry)
    # No exists pre-check: open() is validate-only (never DDL) and raises the more precise
    # EnvironmentStoreNotProvisionedError for a missing store
    env_db.open()
    try:
        config = PostgresEnvironmentConfig.model_validate(env_db.load_config(entry.id))
        if "all" in disable_output:
            storages = []
        else:
            storages = [s for s in config.output.storages if s.type not in disable_output]
        output_stores = output.create_stores(entry.id, storages)
        effective_tail_buffer_size = tail_buffer_size if tail_buffer_size is not None \
            else config.output.default_tail_buffer_size
        plugin_features = Plugin.create_all(config.plugins) if config.plugins else ()
        lock_provider = postgres.create_lock_provider(entry)

        return compose(entry.id, env_db, PostgresInstanceAccessPoint(env_db),
                       PollingInstanceDirectory(env_db, lambda run: SnapshotJobInstanceProxy(run, env_db)),
                       lock_provider, output_stores, to_tuple(plugin_features), transient=True,
                       tail_buffer_size=effective_tail_buffer_size)
    except BaseException:
        env_db.close()
        raise



class AlreadyStarted(InvalidStateError):
    def __init__(self):
        super().__init__("Job instance already started")


class EnvironmentClosed(InvalidStateError):
    def __init__(self):
        super().__init__("Environment is closed")
