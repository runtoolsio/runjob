"""
Tests that :mod:`runjob` sends correct notification to state observers.
:class:`TestStateObserver` is used for verifying the behavior.
"""

import pytest

from runtools import runjob
from runtools.runcore.job import JobRun, InstanceTransitionObserver
from runtools.runcore.run import TerminationStatus, RunState
from runtools.runcore.test.observer import TestTransitionObserver
from runtools.runjob import runner, ExecutingPhase
from runtools.runjob.execution import ExecutionException
from runtools.runjob.phaser import InitPhase, TerminalPhase
from runtools.runjob.test.execution import TestExecution


@pytest.fixture
def observer():
    observer = TestTransitionObserver()
    runner.register_transition_observer(observer)
    yield observer
    runner.deregister_transition_observer(observer)


EXEC = 'j1'


def test_passed_args(observer: TestTransitionObserver):
    runjob.run_uncoordinated('j1', TestExecution())

    assert observer.job_runs[0].metadata.job_id == 'j1'
    assert observer.phases == [(InitPhase.ID, EXEC), (EXEC, TerminalPhase.ID)]
    assert observer.run_states == [RunState.EXECUTING, RunState.ENDED]


def test_raise_exc(observer: TestTransitionObserver):
    with pytest.raises(Exception):
        runjob.run_uncoordinated('j1', TestExecution(raise_exc=Exception))

    assert observer.run_states == [RunState.EXECUTING, RunState.ENDED]
    assert observer.job_runs[-1].termination.error.category == 'Exception'


def test_raise_exec_exc(observer: TestTransitionObserver):
    runjob.run_uncoordinated('j1', TestExecution(raise_exc=ExecutionException))

    assert observer.run_states == [RunState.EXECUTING, RunState.ENDED]
    assert observer.job_runs[-1].termination.failure.category == 'ExecutionException'


def test_observer_raises_exception():
    """
    All exception raised by observer must be captured by runjob and not to disrupt job execution
    """
    observer = ExceptionRaisingObserver(Exception('Should be captured by runjob'))
    execution = TestExecution()
    job_instance = runjob.job_instance('j1', [ExecutingPhase('', execution)])
    job_instance.add_observer_transition(observer)
    job_instance.run()
    assert execution.executed_latch.is_set()
    assert job_instance.job_run().termination.status == TerminationStatus.COMPLETED


class ExceptionRaisingObserver(InstanceTransitionObserver):

    def __init__(self, raise_exc: Exception):
        self.raise_exc = raise_exc

    def new_instance_phase(self, job_run: JobRun, previous_phase, new_phase, changed):
        raise self.raise_exc
