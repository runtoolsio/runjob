"""
Tests that :mod:`runjob` sends correct notification to state observers.
:class:`TestStateObserver` is used for verifying the behavior.
"""

import pytest

from runtools.runcore.job import InstanceLifecycleObserver, iid
from runtools.runcore.run import TerminationStatus, Stage
from runtools.runcore.test.observer import TestLifecycleObserver
from runtools.runjob import instance
from runtools.runjob.test.phase import TestPhase


@pytest.fixture
def observer():
    return TestLifecycleObserver()


EXEC = 'j1'


def test_passed_args(observer: TestLifecycleObserver):
    instance.create(iid('j1'), None, TestPhase(EXEC), lifecycle_observers=[observer]).run()

    assert observer.job_runs[0].metadata.job_id == 'j1'
    assert observer.stages == [Stage.RUNNING, Stage.ENDED]  # TODO CREATED


def test_raise_exc(observer: TestLifecycleObserver):
    with pytest.raises(Exception):
        instance.create(iid('j1'), None, TestPhase(raise_exc=Exception), lifecycle_observers=[observer]).run()

    assert observer.stages == [Stage.RUNNING, Stage.ENDED]  # TODO CREATED
    assert observer.job_runs[-1].lifecycle.termination.status == TerminationStatus.ERROR


def test_raise_exec_terminated(observer: TestLifecycleObserver):
    (instance.create(iid('j1'), None, TestPhase(fail=True), lifecycle_observers=[observer])
     .run())

    assert observer.stages == [Stage.RUNNING, Stage.ENDED]  # TODO CREATED
    assert observer.job_runs[-1].lifecycle.termination.status == TerminationStatus.FAILED


def test_observer_raises_exception():
    """
    All exception raised by observer must be captured by runjob and not to disrupt job execution
    """
    observer = ExceptionRaisingObserver(Exception('Should be captured by runjob'))
    execution = TestPhase()
    job_instance = instance.create(iid('j1'), None, execution, lifecycle_observers=[observer])
    job_instance.run()
    assert execution.completed
    assert job_instance.snap().lifecycle.termination.status == TerminationStatus.COMPLETED
    assert job_instance.snap().faults[0].category == instance.LIFECYCLE_OBSERVER_ERROR


class ExceptionRaisingObserver(InstanceLifecycleObserver):

    def __init__(self, raise_exc: Exception):
        self.raise_exc = raise_exc

    def instance_lifecycle_update(self, e):
        raise self.raise_exc
