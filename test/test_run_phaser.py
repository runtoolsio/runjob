import pytest

from runtools.runcore.run import TerminationStatus
from runtools.runjob.phase import SequentialPhase
from runtools.runjob.test.phase import FakeContext, TestPhase

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
    return SequentialPhase('seq', [TestPhase(EXEC1), TestPhase(EXEC2)])


@pytest.fixture
def sequential_with_approval():
    """Sequential phase with an approval (waiting) phase followed by execution"""
    return SequentialPhase('seq_approve', [TestPhase(APPROVAL, wait=True), TestPhase(EXEC)])


def test_basic_sequence(sequential, ctx):
    """Test basic sequential execution flow"""
    sequential.run(ctx)

    assert sequential.termination.status == TerminationStatus.COMPLETED
    for child in sequential.children:
        assert child.termination.status == TerminationStatus.COMPLETED


def test_failing_phase(ctx):
    """Test behavior when a phase fails"""
    failing_phase = TestPhase(EXEC1, fail=True)
    failing_phase.run(ctx)

    assert failing_phase.termination.status == TerminationStatus.FAILED


def test_sequential_stops_on_failure(ctx):
    """Test that sequential execution stops when a phase fails"""
    seq = SequentialPhase('seq_fail',
                          [TestPhase(EXEC1), TestPhase(EXEC2, fail=True), TestPhase('exec3')])
    seq.run(ctx)

    assert seq.children[0].termination.status == TerminationStatus.COMPLETED
    assert seq.children[1].termination.status == TerminationStatus.FAILED
    assert not seq.children[2].termination

    assert seq.termination.status == TerminationStatus.FAILED

def test_sequential_stops_on_exception(ctx):
    """Test that sequential execution stops when a phase fails"""
    exc = Exception()
    seq = SequentialPhase('seq_fail',
                          [TestPhase(EXEC1), TestPhase(EXEC2, raise_exc=exc), TestPhase('exec3')])
    with pytest.raises(Exception) as exc_info:
        seq.run(ctx)

    assert exc_info.value is exc
    assert seq.children[0].termination.status == TerminationStatus.COMPLETED
    assert seq.children[1].termination.status == TerminationStatus.ERROR
    assert not seq.children[2].termination

    assert seq.termination.status == TerminationStatus.ERROR


def test_interruption(ctx):
    """Test handling of keyboard interruption"""
    phase = TestPhase(EXEC1, raise_exc=KeyboardInterrupt)

    with pytest.raises(KeyboardInterrupt):
        phase.run(ctx)

    assert phase.termination.status == TerminationStatus.INTERRUPTED
