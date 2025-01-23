from threading import Thread

import pytest

from runtools.runcore.common import InvalidStateError
from runtools.runcore.run import TerminationStatus, RunState, FailedRun, Fault
from runtools.runjob import phaser
from runtools.runjob.phaser import Phaser, InitPhase, TerminalPhase, WaitWrapperPhase, PhaseExecutionError
from runtools.runjob.test.phaser import TestPhase

INIT = InitPhase.ID
APPROVAL = 'approval'
EXEC = 'exec'
EXEC1 = 'exec1'
EXEC2 = 'exec2'
PROGRAM = 'program'
TERM = TerminalPhase.ID


@pytest.fixture
def sut():
    return Phaser([TestPhase(EXEC1), TestPhase(EXEC2)])


@pytest.fixture
def sut_approve():
    return Phaser([WaitWrapperPhase(TestPhase(APPROVAL, wait=True)), TestPhase(EXEC)])


def test_run_with_approval(sut_approve):
    sut_approve.prime()
    run_thread = Thread(target=sut_approve.run)
    run_thread.start()
    # The below code will be released once the run starts pending in the approval phase
    wait_wrapper = sut_approve.get_phase(APPROVAL)

    wait_wrapper.wait(1)
    snapshot = sut_approve.snapshot()
    assert snapshot.lifecycle.current_phase_id == APPROVAL
    assert snapshot.lifecycle.run_state == RunState.PENDING

    wait_wrapper.wrapped.wait.set()
    run_thread.join(1)
    assert (sut_approve.snapshot().lifecycle.phase_ids == [INIT, APPROVAL, EXEC, TERM])


def test_post_prime(sut):
    sut.prime()

    snapshot = sut.snapshot()
    assert snapshot.lifecycle.current_phase_id == INIT
    assert snapshot.lifecycle.run_state == RunState.CREATED


def test_empty_phaser():
    empty = Phaser([])
    empty.prime()
    assert empty.snapshot().lifecycle.phase_ids == [INIT]

    empty.run()

    snapshot = empty.snapshot()
    assert snapshot.lifecycle.phase_ids == [INIT, TERM]
    assert snapshot.termination.status == TerminationStatus.COMPLETED


def test_stop_before_prime(sut):
    sut.stop()

    snapshot = sut.snapshot()
    assert snapshot.lifecycle.phase_ids == [TERM]
    assert snapshot.termination.status == TerminationStatus.STOPPED


def test_stop_before_run(sut):
    sut.prime()
    sut.stop()

    snapshot = sut.snapshot()
    assert snapshot.lifecycle.phase_ids == [INIT, TERM]
    assert snapshot.termination.status == TerminationStatus.STOPPED


def test_stop_in_run(sut_approve):
    sut_approve.prime()
    run_thread = Thread(target=sut_approve.run)
    run_thread.start()
    # The below code will be released once the run starts pending in the approval phase
    sut_approve.get_phase(APPROVAL).wait(1)

    sut_approve.stop()
    run_thread.join(1)  # Let the run end

    run = sut_approve.snapshot()
    assert (run.lifecycle.phase_ids == [INIT, APPROVAL, TERM])
    assert run.termination.status == TerminationStatus.CANCELLED


def test_premature_termination(sut):
    sut.get_phase(EXEC1).fail = True
    sut.prime()
    sut.run()

    run = sut.snapshot()
    assert run.termination.status == TerminationStatus.FAILED
    assert (run.lifecycle.phase_ids == [INIT, EXEC1, TERM])


def test_transition_hook(sut):
    transitions = []

    def hook(*args):
        transitions.append(args)

    sut.transition_hook = hook

    sut.prime()

    assert len(transitions) == 1
    prev_run, new_run, ordinal = transitions[0]
    assert not prev_run
    assert new_run.phase_id == INIT
    assert ordinal == 1

    sut.run()

    assert len(transitions) == 4


def test_failed_run_exception(sut):
    failed_run = FailedRun(Fault('FaultType', 'reason'))
    sut.get_phase(EXEC1).failed_run = failed_run
    sut.prime()
    sut.run()

    snapshot = sut.snapshot()
    assert snapshot.termination.status == TerminationStatus.FAILED
    assert (snapshot.lifecycle.phase_ids == [INIT, EXEC1, TERM])

    assert snapshot.termination.failure == failed_run.fault


def test_exception(sut):
    exc = InvalidStateError('reason')
    sut.get_phase(EXEC1).exception = exc
    sut.prime()

    with pytest.raises(PhaseExecutionError) as exc_info:
        sut.run()

    assert exc_info.value.phase_id == EXEC1
    assert exc_info.value.__cause__ == exc

    snapshot = sut.snapshot()
    assert snapshot.termination.status == TerminationStatus.ERROR
    assert (snapshot.lifecycle.phase_ids == [INIT, EXEC1, TERM])

    assert snapshot.termination.error.category == phaser.UNCAUGHT_PHASE_RUN_EXCEPTION
    assert snapshot.termination.error.reason == 'InvalidStateError: reason'


def test_interruption(sut):
    sut.get_phase(EXEC1).exception = KeyboardInterrupt
    sut.prime()

    with pytest.raises(KeyboardInterrupt):
        sut.run()

    snapshot = sut.snapshot()
    assert snapshot.termination.status == TerminationStatus.INTERRUPTED
