"""
Tests that :mod:`runjob` sends correct notification to state observers.
:class:`TestStateObserver` is used for verifying the behavior.
"""

import pytest

from runtools.runcore.job import InstanceStageObserver
from runtools.runcore.run import TerminationStatus, Stage
from runtools.runcore.test.observer import TestStageObserver
from runtools.runjob import instance, phase
from runtools.runjob.test.phase import TestPhase


@pytest.fixture
def observer():
    return TestStageObserver()


EXEC = 'j1'


def test_passed_args(observer: TestStageObserver):
    instance.create('j1', [TestPhase(EXEC)], stage_observers=[observer]).run()

    assert observer.job_runs[0].metadata.job_id == 'j1'
    assert observer.stages == [Stage.RUNNING, Stage.ENDED]  # TODO CREATED


def test_raise_exc(observer: TestStageObserver):
    with pytest.raises(Exception):
        instance.create('j1', [TestPhase(raise_exc=Exception)], stage_observers=[observer]).run()

    assert observer.stages == [Stage.RUNNING, Stage.ENDED]  # TODO CREATED
    assert observer.job_runs[-1].lifecycle.termination.fault.category == phase.UNCAUGHT_PHASE_EXEC_EXCEPTION


def test_raise_exec_terminated(observer: TestStageObserver):
    (instance.create(
        'j1',
        [TestPhase(fail=True)],
        stage_observers=[observer])
     .run())

    assert observer.stages == [Stage.RUNNING, Stage.ENDED]  # TODO CREATED
    assert observer.job_runs[-1].lifecycle.termination.status == TerminationStatus.FAILED


def test_observer_raises_exception():
    """
    All exception raised by observer must be captured by runjob and not to disrupt job execution
    """
    observer = ExceptionRaisingObserver(Exception('Should be captured by runjob'))
    execution = TestPhase()
    job_instance = instance.create('j1', [execution], stage_observers=[observer])
    job_instance.run()
    assert execution.completed
    assert job_instance.snapshot().lifecycle.termination.status == TerminationStatus.COMPLETED
    assert job_instance.snapshot().faults.transition_observer_faults[0].category == instance.TRANSITION_OBSERVER_ERROR


class ExceptionRaisingObserver(InstanceStageObserver):

    def __init__(self, raise_exc: Exception):
        self.raise_exc = raise_exc

    def new_instance_stage(self, e):
        raise self.raise_exc
