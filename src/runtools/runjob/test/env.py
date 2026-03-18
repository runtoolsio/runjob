"""
Lightweight fake environment for testing coordination phases (ExecutionQueue, MutualExclusionPhase, etc.)
without the overhead of a full InProcessNode.

Usage:
    env = FakeEnvironment()
    inst = env.create_instance('job', 'run1', root_phase=my_phase)
    inst.run(in_background=True)
    ...
    inst.stop()
"""
from contextlib import contextmanager
from threading import Event, Thread
from typing import List, Optional

from runtools.runcore.job import (
    JobInstance, JobInstanceDelegate, JobRun, InstanceObservableNotifications, InstanceNotifications, iid,
)
from runtools.runcore.run import StopReason
from runtools.runjob import instance


class FakeWatcher:
    """Controllable watcher stub for testing AwaitPhase."""
    __test__ = False

    def __init__(self):
        self._event = Event()
        self._satisfied = False
        self._timed_out = False
        self._cancelled = False

    @property
    def is_timed_out(self):
        return self._timed_out

    @property
    def is_cancelled(self):
        return self._cancelled

    def satisfy(self):
        """Signal that all criteria are satisfied."""
        self._satisfied = True
        self._event.set()

    def wait(self, *, timeout=None):
        self._event.wait(timeout)
        if self._cancelled:
            return False
        if self._satisfied:
            return True
        self._timed_out = True
        return False

    def cancel(self):
        self._cancelled = True
        self._event.set()


class FakeEnvironment:
    """
    Minimal environment stub for coordination phase tests.

    Provides the interface that coordination phases use from ctx.environment:
    - notifications: observer registration (required by ExecutionQueue)
    - lock(): no-op context manager
    - get_active_runs(): returns a configurable list (default empty)
    - get_instance(): returns managed instances by ID
    """

    def __init__(self, *, active_runs: Optional[List[JobRun]] = None):
        self._notifications = InstanceObservableNotifications()
        self._instances = {}
        self.active_runs: List[JobRun] = active_runs or []

    @property
    def env_id(self):
        return 'fake'

    @property
    def notifications(self) -> InstanceNotifications:
        return self._notifications

    @contextmanager
    def lock(self, lock_id):
        yield

    def get_active_runs(self, run_match=None) -> List[JobRun]:
        if run_match:
            return [r for r in self.active_runs if run_match(r)]
        return list(self.active_runs)

    def get_instance(self, instance_id) -> Optional[JobInstance]:
        return self._instances.get(instance_id)

    def watcher(self, *criteria, search_past=False) -> FakeWatcher:
        self._watcher = FakeWatcher()
        return self._watcher

    def create_instance(self, job_id, run_id, root_phase, **user_params) -> 'TestJobInstance':
        instance_id = iid(job_id, run_id)
        inst = instance.create(instance_id, self, root_phase, **user_params)
        managed = TestJobInstance(inst)
        self._instances[instance_id] = managed
        self._notifications.bind_to(inst.notifications)
        return managed


class TestJobInstance(JobInstanceDelegate):
    """JobInstance wrapper that adds run(in_background=True) for testing."""

    def run(self, in_background=False):
        if in_background:
            t = Thread(target=self._wrapped.run, daemon=True)
            t.start()
        else:
            self._wrapped.run()

    def stop(self, reason=StopReason.STOPPED):
        self._wrapped.stop(reason)
