from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import Lock, Condition
from typing import Dict, Optional, List

from runtools.runcore import JobRun, plugins, paths
from runtools.runcore.common import InvalidStateError
from runtools.runcore.db import sqlite, PersistingObserver
from runtools.runcore.environment import Environment, LocalEnvironment, PersistingEnvironment
from runtools.runcore.job import JobInstance, JobInstanceObservable, InstanceStageEvent
from runtools.runcore.plugins import Plugin
from runtools.runcore.run import Stage
from runtools.runcore.util import ensure_tuple_copy, lock
from runtools.runcore.util.err import run_isolated_collect_exceptions
from runtools.runjob import instance, JobInstanceHook
from runtools.runjob.events import TransitionDispatcher, OutputDispatcher
from runtools.runjob.server import RemoteCallServer


def _create_plugins(names):
    plugins.load_modules(names)
    fetched_plugins = Plugin.fetch_plugins(names)
    # TODO complete


class RunnableEnvironment(Environment, ABC):

    @abstractmethod
    def create_instance(self, *args, **kwargs):
        """TODO Should the instance be auto-started to prevent environment waiting infinitely?"""
        pass

    @abstractmethod
    def lock(self, lock_id):
        """TODO to separate type"""

    def signal_dispatch(self, instance_id, phase_id):
        """TODO TBD"""
        self.get_instance(instance_id).find_phase_control().signal_dispatch()


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


@dataclass
class _ManagedInstance:
    instance: JobInstance
    detaching: bool = False
    detached: bool = False


class RunnableEnvironmentBase(RunnableEnvironment, ABC):
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

     TODO: Do not wait in close() method for instances that haven't started
    """
    OBSERVERS_PRIORITY = 1000

    def __init__(self, features=(), transient=True):
        self._features = features
        self._transient = transient
        self._lock = Lock()
        self._detached_condition = Condition(self._lock)
        # Fields guarded by lock below:
        self._managed_instances: Dict[str, _ManagedInstance] = {}
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
            return list(managed.instance for managed in self._managed_instances.values())

    def _new_instance_stage(self, event: InstanceStageEvent):
        # TODO Consider using post_run_hook for instances created by this container
        if event.new_stage == Stage.ENDED:
            self._detach_instance(event.instance.instance_id, self._transient)

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

            managed_instance = _ManagedInstance(job_instance)
            self._managed_instances[job_instance.instance_id] = managed_instance

        job_instance.add_observer_stage(self._new_instance_stage, RunnableEnvironmentBase.OBSERVERS_PRIORITY)

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

        if job_instance.snapshot().phase.lifecycle.is_ended:
            self._detach_instance(job_instance.metadata.instance_id, self._transient)

        return job_instance

    def _on_added(self, job_instance):
        pass

    def _remove_instance(self, job_instance_id) -> Optional[JobInstance]:
        """
        Remove a job instance from the context using its ID.

        Args:
            job_instance_id (JobRunId): The ID of the job instance to remove.

        Returns:
            Optional[JobInstance]: The removed job instance if found, otherwise None.
        """
        return self._detach_instance(job_instance_id, True)

    def _detach_instance(self, job_instance_id, remove):
        """
        Implementation note: A race condition can cause this method to be executed twice with the same ID
        """
        detach = False
        removed = False
        with self._lock:
            managed_instance = self._managed_instances.get(job_instance_id)
            if not managed_instance:
                return None

            if not managed_instance.detaching:
                managed_instance.detaching = detach = True

            if remove:
                del self._managed_instances[job_instance_id]
                removed = True

        job_instance = managed_instance.instance

        if removed:
            self._on_removed(job_instance)
            for feature in self._features:
                feature.on_instance_removed(job_instance)

        if detach:
            job_instance.remove_observer_transition(self._new_instance_stage)
            with self._detached_condition:
                managed_instance.detached = True
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
            while not all((i.detached for i in self._managed_instances.values())):
                try:
                    self._detached_condition.wait()  # TODO Could block infinitely if an error prevented to run an instance
                except KeyboardInterrupt:
                    interrupt_received = True
                    break

        run_isolated_collect_exceptions(
            "Errors on environment features closing",
            *(feature.on_close for feature in self._features),
            suppress=interrupt_received
        )

        if interrupt_received:
            raise KeyboardInterrupt


def isolated(persistence=None, *, lock_factory=None, features=None, transient=True):
    persistence = persistence or sqlite.create(':memory:')
    lock_factory = lock_factory or lock.default_memory_lock_factory()
    return _IsolatedEnvironment(persistence, lock_factory, ensure_tuple_copy(features), transient)


class _IsolatedEnvironment(JobInstanceObservable, PersistingEnvironment, RunnableEnvironmentBase):

    def __init__(self, persistence, lock_factory, features, transient=True):
        JobInstanceObservable.__init__(self)
        RunnableEnvironmentBase.__init__(self, features, transient=transient)
        PersistingEnvironment.__init__(self, persistence)
        self._lock_factory = lock_factory
        self._persisting_observer = PersistingObserver(persistence)

    def open(self):
        RunnableEnvironmentBase.open(self)  # Always first
        PersistingEnvironment.open(self)

    def _on_added(self, job_instance):
        job_instance.add_observer_stage(self._persisting_observer)
        job_instance.add_observer_stage(self._stage_notification.observer_proxy,
                                             RunnableEnvironmentBase.OBSERVERS_PRIORITY - 1)
        job_instance.add_observer_transition(self._transition_notification.observer_proxy,
                                             RunnableEnvironmentBase.OBSERVERS_PRIORITY - 1)
        job_instance.add_observer_output(self._output_notification.observer_proxy,
                                         RunnableEnvironmentBase.OBSERVERS_PRIORITY - 1)

    def _on_removed(self, job_instance):
        job_instance.remove_observer_output(self._output_notification.observer_proxy)
        job_instance.remove_observer_transition(self._transition_notification.observer_proxy)
        job_instance.remove_observer_stage(self._stage_notification.observer_proxy)
        job_instance.remove_observer_stage(self._persisting_observer)

    def get_active_runs(self, run_match=None) -> List[JobRun]:
        return [i.snapshot() for i in self.get_instances(run_match)]

    def get_instances(self, run_match=None) -> List[JobInstance]:
        return [i for i in self.instances if not run_match or run_match(i)]

    def lock(self, lock_id):
        return self._lock_factory(lock_id)

    def close(self):
        run_isolated_collect_exceptions(
            "Errors during closing isolated environment",
            lambda: RunnableEnvironmentBase.close(self), # <- Always execute first as the method is waiting until it can be closed
            lambda: PersistingEnvironment.close(self)
        )


def local(persistence=None, *, lock_factory=None, features=None, transient=True):
    persistence = persistence or sqlite.create(':memory:')
    api = RemoteCallServer()
    transition_dispatcher = TransitionDispatcher()
    output_dispatcher = OutputDispatcher()
    lock_factory = lock_factory or lock.default_file_lock_factory()
    features = ensure_tuple_copy(features)
    return RunnableLocalEnvironment(persistence, api, transition_dispatcher, output_dispatcher, lock_factory,
                                    features, transient)


class RunnableLocalEnvironment(LocalEnvironment, RunnableEnvironmentBase):

    def __init__(self, persistence, api, transition_dispatcher, output_dispatcher, lock_factory, features, transient):
        RunnableEnvironmentBase.__init__(self, features, transient=transient)
        LocalEnvironment.__init__(self, persistence)
        self._api = api
        self._transition_dispatcher = transition_dispatcher
        self._output_dispatcher = output_dispatcher
        self._lock_factory = lock_factory
        self._persisting_observer = PersistingObserver(persistence)

    def open(self):
        RunnableEnvironmentBase.open(self)  # Execute first for opened only once check
        LocalEnvironment.open(self)

        self._api.start()

    def _on_added(self, job_instance):
        job_instance.add_observer_stage(self._persisting_observer)
        self._api.register_instance(job_instance)
        job_instance.add_observer_transition(self._transition_dispatcher)
        job_instance.add_observer_output(self._output_dispatcher)

    def _on_removed(self, job_instance):
        job_instance.remove_observer_output(self._output_dispatcher)
        job_instance.remove_observer_transition(self._transition_dispatcher)
        self._api.unregister_instance(job_instance)
        job_instance.remove_observer_stage(self._persisting_observer)

    def lock(self, lock_id):
        # TODO Method to separate type
        return self._lock_factory(paths.lock_path(f"{lock_id}.lock", True))

    def close(self):
        run_isolated_collect_exceptions(
            "Errors during closing runnable local environment",
            lambda: RunnableEnvironmentBase.close(self),
            # Always execute first as the method is waiting until it can be closed
            lambda: LocalEnvironment.close(self),
            self._api.close,
            self._output_dispatcher.close,
            self._transition_dispatcher.close
        )
