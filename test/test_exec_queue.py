import time

from runtools.runcore.job import iid
from runtools.runcore.run import TerminationStatus
from runtools.runjob.coord import ExecutionQueue, ConcurrencyGroup, QueuedState
from runtools.runjob.test.env import FakeEnvironment
from runtools.runjob.test.phase import TestPhase

GROUP = ConcurrencyGroup('test_group', max_executions=1)


def _wait_for_state(phase, state, timeout=2):
    deadline = time.monotonic() + timeout
    while phase.state != state:
        if time.monotonic() > deadline:
            raise TimeoutError(f"Phase did not reach {state} within {timeout}s (current: {phase.state})")
        time.sleep(0.01)


def _wait_for_termination(phase, timeout=2):
    deadline = time.monotonic() + timeout
    while phase.termination is None:
        if time.monotonic() > deadline:
            raise TimeoutError(f"Phase did not terminate within {timeout}s")
        time.sleep(0.01)


def test_stopped_queue_reports_stopped_status():
    """A queued phase that is stopped before dispatch should terminate with STOPPED, not COMPLETED."""
    fake_env = FakeEnvironment()
    queue = ExecutionQueue('QUEUE', GROUP, TestPhase('exec'))
    inst = fake_env.create_instance(iid('job'), root_phase=queue)

    inst.run(in_background=True)
    _wait_for_state(queue, QueuedState.IN_QUEUE)

    inst.stop()
    _wait_for_termination(queue)

    assert queue.termination.status == TerminationStatus.STOPPED
