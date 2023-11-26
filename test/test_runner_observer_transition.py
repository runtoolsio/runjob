"""
Tests that :mod:`runner` sends correct notification to state observers.
:class:`TestStateObserver` is used for verifying the behavior.
"""

import pytest
from tarotools.taro.job import JobRun, InstanceTransitionObserver
from tarotools.taro.run import TerminationStatus, RunState, PhaseNames
from tarotools.taro.test.observer import TestTransitionObserver

import runtoolsio.runjob
from runtoolsio.runjob import runner
from runtoolsio.runjob.execution import ExecutionException
from runtoolsio.runjob.test.execution import TestExecution


@pytest.fixture
def observer():
    observer = TestTransitionObserver()
    runner.register_transition_observer(observer)
    yield observer
    runner.deregister_transition_observer(observer)


def test_passed_args(observer: TestTransitionObserver):
    runtoolsio.runjob.run_uncoordinated('j1', TestExecution())

    assert observer.job_runs[0].metadata.job_id == 'j1'
    assert observer.phases == [(PhaseNames.INIT, PhaseNames.EXEC), (PhaseNames.EXEC, PhaseNames.TERMINAL)]
    assert observer.run_states == [RunState.EXECUTING, RunState.ENDED]


def test_raise_exc(observer: TestTransitionObserver):
    runtoolsio.runjob.run_uncoordinated('j1', TestExecution(raise_exc=Exception))

    assert observer.run_states == [RunState.EXECUTING, RunState.ENDED]
    assert observer.job_runs[-1].run.termination.error.category == 'Exception'


def test_raise_exec_exc(observer: TestTransitionObserver):
    runtoolsio.runjob.run_uncoordinated('j1', TestExecution(raise_exc=ExecutionException))

    assert observer.run_states == [RunState.EXECUTING, RunState.ENDED]
    assert observer.job_runs[-1].run.termination.failure.category == 'ExecutionException'


def test_observer_raises_exception():
    """
    All exception raised by observer must be captured by runner and not to disrupt job execution
    """
    observer = ExceptionRaisingObserver(Exception('Should be captured by runner'))
    execution = TestExecution()
    job_instance = runtoolsio.runjob.job_instance('j1', execution)
    job_instance.add_observer_transition(observer)
    job_instance.run()
    assert execution.executed_latch.is_set()
    assert job_instance.job_run_info().run.termination.status == TerminationStatus.COMPLETED


class ExceptionRaisingObserver(InstanceTransitionObserver):

    def __init__(self, raise_exc: Exception):
        self.raise_exc = raise_exc

    def new_instance_phase(self, job_run: JobRun, previous_phase, new_phase, changed):
        raise self.raise_exc
