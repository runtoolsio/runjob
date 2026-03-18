import time

from runtools.runcore.run import TerminationStatus
from runtools.runjob.coord import AwaitPhase
from runtools.runjob.test.env import FakeEnvironment


def _wait_for_running(phase, timeout=2):
    deadline = time.monotonic() + timeout
    while phase.started_at is None:
        if time.monotonic() > deadline:
            raise TimeoutError("Phase did not start running")
        time.sleep(0.01)


def _wait_for_termination(phase, timeout=2):
    deadline = time.monotonic() + timeout
    while phase.termination is None:
        if time.monotonic() > deadline:
            raise TimeoutError("Phase did not terminate")
        time.sleep(0.01)


def test_await_completes_when_criteria_satisfied():
    """AwaitPhase should complete successfully when the watcher criteria are satisfied."""
    env = FakeEnvironment()
    await_phase = AwaitPhase()
    inst = env.create_instance('job', 'run1', await_phase)

    inst.run(in_background=True)
    _wait_for_running(await_phase)

    env._watcher.satisfy()
    _wait_for_termination(await_phase)

    assert await_phase.termination.status == TerminationStatus.COMPLETED


def test_await_times_out():
    """AwaitPhase should terminate with TIMEOUT when the deadline expires."""
    env = FakeEnvironment()
    await_phase = AwaitPhase(timeout=0.1)
    inst = env.create_instance('job', 'run1', await_phase)

    inst.run(in_background=True)
    _wait_for_termination(await_phase)

    assert await_phase.termination.status == TerminationStatus.TIMEOUT


def test_await_stopped_before_satisfied():
    """AwaitPhase should terminate with STOPPED when the instance is stopped."""
    env = FakeEnvironment()
    await_phase = AwaitPhase()
    inst = env.create_instance('job', 'run1', await_phase)

    inst.run(in_background=True)
    _wait_for_running(await_phase)

    inst.stop()
    _wait_for_termination(await_phase)

    assert await_phase.termination.status == TerminationStatus.STOPPED
