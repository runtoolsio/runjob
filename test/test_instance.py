"""
Tests that :mod:`runjob` sends correct notification to state observers.
:class:`TestStateObserver` is used for verifying the behavior.
"""

import pytest

from runtools.runcore.job import InstanceLifecycleObserver, iid
from runtools.runcore.run import JobCompletionError, TerminationStatus, Stage
from runtools.runcore.test.observer import TestLifecycleObserver
from runtools.runjob import instance
from runtools.runjob.test.phase import TestPhase


@pytest.fixture
def observer():
    return TestLifecycleObserver()


EXEC = 'j1'


def test_passed_args(observer: TestLifecycleObserver):
    job_instance = instance.create(iid('j1'), None, TestPhase(EXEC), activate=False)
    job_instance.notifications.add_observer_lifecycle(observer)
    job_instance.activate().notify_created()
    job_instance.run()

    assert observer.job_runs[0].metadata.job_id == 'j1'
    assert observer.stages == [Stage.CREATED, Stage.RUNNING, Stage.ENDED]


def test_raise_exc(observer: TestLifecycleObserver):
    job_instance = instance.create(iid('j1'), None, TestPhase(raise_exc=Exception), activate=False)
    job_instance.notifications.add_observer_lifecycle(observer)
    job_instance.activate().notify_created()
    with pytest.raises(JobCompletionError) as exc_info:
        job_instance.run()

    assert isinstance(exc_info.value.__cause__, Exception)
    assert exc_info.value.termination.status == TerminationStatus.ERROR
    assert observer.stages == [Stage.CREATED, Stage.RUNNING, Stage.ENDED]
    assert observer.job_runs[-1].lifecycle.termination.status == TerminationStatus.ERROR


def test_raise_exec_terminated(observer: TestLifecycleObserver):
    job_instance = instance.create(iid('j1'), None, TestPhase(fail=True), activate=False)
    job_instance.notifications.add_observer_lifecycle(observer)
    job_instance.activate().notify_created()
    with pytest.raises(JobCompletionError) as exc_info:
        job_instance.run()

    assert exc_info.value.termination.status == TerminationStatus.FAILED
    assert observer.stages == [Stage.CREATED, Stage.RUNNING, Stage.ENDED]
    assert observer.job_runs[-1].lifecycle.termination.status == TerminationStatus.FAILED


def test_observer_raises_exception():
    """
    All exception raised by observer must be captured by runjob and not to disrupt job execution
    """
    observer = ExceptionRaisingObserver(Exception('Should be captured by runjob'))
    execution = TestPhase()
    job_instance = instance.create(iid('j1'), None, execution, activate=False)
    job_instance.notifications.add_observer_lifecycle(observer)
    job_instance.activate().notify_created()
    job_instance.run()
    assert execution.completed
    assert job_instance.snap().lifecycle.termination.status == TerminationStatus.COMPLETED
    assert job_instance.snap().faults[0].category == instance.LIFECYCLE_OBSERVER_ERROR


class ExceptionRaisingObserver(InstanceLifecycleObserver):

    def __init__(self, raise_exc: Exception):
        self.raise_exc = raise_exc

    def instance_lifecycle_update(self, e):
        raise self.raise_exc
