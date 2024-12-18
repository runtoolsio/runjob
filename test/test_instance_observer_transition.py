"""
Tests that :mod:`runjob` sends correct notification to state observers.
:class:`TestStateObserver` is used for verifying the behavior.
"""

import pytest

from runtools.runcore.job import JobRun, InstanceTransitionObserver
from runtools.runcore.run import TerminationStatus, RunState, FailedRun
from runtools.runcore.test.observer import TestTransitionObserver
from runtools.runjob import instance
from runtools.runjob.phaser import InitPhase, TerminalPhase
from runtools.runjob.test.phaser import TestPhase


@pytest.fixture
def observer():
    observer = TestTransitionObserver()
    instance.register_transition_observer(observer)
    yield observer
    instance.deregister_transition_observer(observer)


EXEC = 'j1'


def test_passed_args(observer: TestTransitionObserver):
    instance.create('j1', [TestPhase(EXEC)]).run()

    assert observer.job_runs[0].metadata.job_id == 'j1'
    assert observer.phases == [(InitPhase.ID, EXEC), (EXEC, TerminalPhase.ID)]
    assert observer.run_states == [RunState.EXECUTING, RunState.ENDED]


def test_raise_exc(observer: TestTransitionObserver):
    with pytest.raises(Exception):
        instance.create('j1', [TestPhase(raise_exc=Exception)]).run()

    assert observer.run_states == [RunState.EXECUTING, RunState.ENDED]
    assert observer.job_runs[-1].termination.error.category == 'Exception'


def test_raise_exec_exc(observer: TestTransitionObserver):
    instance.create('j1', [TestPhase(raise_exc=FailedRun('test_type', 'testing reason'))]).run()

    assert observer.run_states == [RunState.EXECUTING, RunState.ENDED]
    assert observer.job_runs[-1].termination.failure.category == 'test_type'


def test_observer_raises_exception():
    """
    All exception raised by observer must be captured by runjob and not to disrupt job execution
    """
    observer = ExceptionRaisingObserver(Exception('Should be captured by runjob'))
    execution = TestPhase()
    job_instance = instance.create('j1', [execution])
    job_instance.add_observer_transition(observer)
    job_instance.run()
    assert execution.completed
    assert job_instance.job_run().termination.status == TerminationStatus.COMPLETED


class ExceptionRaisingObserver(InstanceTransitionObserver):

    def __init__(self, raise_exc: Exception):
        self.raise_exc = raise_exc

    def new_instance_phase(self, job_run: JobRun, previous_phase, new_phase, changed):
        raise self.raise_exc
