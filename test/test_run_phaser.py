import pytest

from runtools.runcore.run import TerminationStatus
from runtools.runjob.phaser import SequentialPhase, PhaseCompletionError
from runtools.runjob.test.phaser import FakeContext, TestPhaseV2

APPROVAL = 'approval'
EXEC = 'exec'
EXEC1 = 'exec1'
EXEC2 = 'exec2'


@pytest.fixture
def ctx():
    return FakeContext()


@pytest.fixture
def sequential():
    """Basic sequential phase with two test phases"""
    return SequentialPhase('seq', [TestPhaseV2(EXEC1), TestPhaseV2(EXEC2)])


@pytest.fixture
def sequential_with_approval():
    """Sequential phase with an approval (waiting) phase followed by execution"""
    return SequentialPhase('seq_approve', [TestPhaseV2(APPROVAL, wait=True), TestPhaseV2(EXEC)])


def test_basic_sequence(sequential, ctx):
    """Test basic sequential execution flow"""
    sequential.run(ctx)

    assert sequential.termination.status == TerminationStatus.COMPLETED
    for child in sequential.children:
        assert child.termination.status == TerminationStatus.COMPLETED


def test_failing_phase(ctx):
    """Test behavior when a phase fails"""
    failing_phase = TestPhaseV2(EXEC1, fail=True)
    failing_phase.run(ctx)

    assert failing_phase.termination.status == TerminationStatus.FAILED


def test_sequential_stops_on_failure(ctx):
    """Test that sequential execution stops when a phase fails"""
    seq = SequentialPhase('seq_fail',
                          [TestPhaseV2(EXEC1), TestPhaseV2(EXEC2, fail=True), TestPhaseV2('exec3')])
    seq.run(ctx)

    assert seq.children[0].termination.status == TerminationStatus.COMPLETED
    assert seq.children[1].termination.status == TerminationStatus.FAILED
    assert not seq.children[2].termination

    assert seq.termination.status == TerminationStatus.FAILED

def test_sequential_stops_on_exception(ctx):
    """Test that sequential execution stops when a phase fails"""
    exc = Exception()
    seq = SequentialPhase('seq_fail',
                          [TestPhaseV2(EXEC1), TestPhaseV2(EXEC2, raise_exc=exc), TestPhaseV2('exec3')])
    with pytest.raises(PhaseCompletionError) as exc_info:
        seq.run(ctx)

    assert exc_info.value.__cause__.__cause__ == exc
    assert seq.children[0].termination.status == TerminationStatus.COMPLETED
    assert seq.children[1].termination.status == TerminationStatus.FAILED
    assert not seq.children[2].termination

    assert seq.termination.status == TerminationStatus.FAILED


def test_interruption(ctx):
    """Test handling of keyboard interruption"""
    phase = TestPhaseV2(EXEC1, raise_exc=KeyboardInterrupt)

    with pytest.raises(KeyboardInterrupt):
        phase.run(ctx)

    assert phase.termination.status == TerminationStatus.INTERRUPTED
