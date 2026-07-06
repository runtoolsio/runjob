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
    holder_child = TestPhase('holder_exec', wait=True)
    holder = ExecutionQueue('HOLDER', GROUP, holder_child)
    queue = ExecutionQueue('QUEUE', GROUP, TestPhase('exec'))

    fake_env.create_instance('holder_job', 'run1', holder).run(in_background=True)
    _wait_for_state(holder, QueuedState.DISPATCHED)  # the only slot is claimed

    inst = fake_env.create_instance('job', 'run1', queue)
    inst.run(in_background=True)
    _wait_for_state(queue, QueuedState.IN_QUEUE)

    inst.stop()
    _wait_for_termination(queue)

    assert queue.termination.status == TerminationStatus.STOPPED
    holder_child.release()


def test_uncontended_queue_dispatches_immediately():
    fake_env = FakeEnvironment()
    queue = ExecutionQueue('QUEUE', GROUP, TestPhase('exec'))

    fake_env.create_instance('job', 'run1', queue).run()

    assert queue.termination.status == TerminationStatus.COMPLETED


def test_capacity_limits_concurrent_children():
    """With one slot, the second instance stays queued until the holder's child completes."""
    fake_env = FakeEnvironment()
    holder_child = TestPhase('c1', wait=True)
    holder = ExecutionQueue('Q1', GROUP, holder_child)
    queued = ExecutionQueue('Q2', GROUP, TestPhase('c2'))

    fake_env.create_instance('job1', 'r1', holder).run(in_background=True)
    _wait_for_state(holder, QueuedState.DISPATCHED)

    fake_env.create_instance('job2', 'r1', queued).run(in_background=True)
    _wait_for_state(queued, QueuedState.IN_QUEUE)
    time.sleep(0.1)  # would-be dispatch window
    assert queued.state == QueuedState.IN_QUEUE

    holder_child.release()  # slot freed on holder completion -> wake-up -> claim
    _wait_for_termination(queued)
    assert queued.termination.status == TerminationStatus.COMPLETED


def _queued_run_snapshot(fake_env, job_id, queue_phase):
    """A JobRun snapshot whose QUEUE phase reports IN_QUEUE — an observation-lane fixture."""
    inst = fake_env.create_instance(job_id, 'r1', queue_phase)
    queue_phase._state = QueuedState.IN_QUEUE
    return inst.snap()


def test_claim_deferred_behind_visible_older_queued_instance():
    """A permanently visible older entry (e.g. dead node) delays dispatch by a stagger step, never blocks it."""
    fake_env = FakeEnvironment()
    ghost = ExecutionQueue('GHOST', GROUP, TestPhase('c1'))
    younger = ExecutionQueue('YOUNGER', GROUP, TestPhase('c2'))
    fake_env.active_runs.append(_queued_run_snapshot(fake_env, 'ghost_job', ghost))

    start = time.monotonic()
    fake_env.create_instance('younger_job', 'r1', younger).run()
    elapsed = time.monotonic() - start

    assert younger.termination.status == TerminationStatus.COMPLETED
    assert elapsed >= ExecutionQueue.CLAIM_STAGGER_INTERVAL


def test_does_not_defer_behind_younger_instance():
    fake_env = FakeEnvironment()
    mine = ExecutionQueue('MINE', GROUP, TestPhase('c1'))
    newer = ExecutionQueue('NEWER', GROUP, TestPhase('c2'))
    fake_env.active_runs.append(_queued_run_snapshot(fake_env, 'newer_job', newer))

    start = time.monotonic()
    fake_env.create_instance('my_job', 'r1', mine).run()
    elapsed = time.monotonic() - start

    assert mine.termination.status == TerminationStatus.COMPLETED
    assert elapsed < ExecutionQueue.CLAIM_STAGGER_INTERVAL


def test_older_waiter_claims_freed_slot_before_younger():
    """Saturated queue: when the slot frees, the visibly older waiter wins over the younger."""
    fake_env = FakeEnvironment()
    holder_child = TestPhase('hold', wait=True)
    holder = ExecutionQueue('HOLDER', GROUP, holder_child)
    older_child = TestPhase('c1', wait=True)
    older = ExecutionQueue('OLDER', GROUP, older_child)
    younger = ExecutionQueue('YOUNGER', GROUP, TestPhase('c2'))

    fake_env.create_instance('holder_job', 'r1', holder).run(in_background=True)
    _wait_for_state(holder, QueuedState.DISPATCHED)

    older_inst = fake_env.create_instance('older_job', 'r1', older)
    older_inst.run(in_background=True)
    _wait_for_state(older, QueuedState.IN_QUEUE)
    fake_env.active_runs.append(older_inst.snap())  # make the older waiter visible to the younger

    fake_env.create_instance('younger_job', 'r1', younger).run(in_background=True)
    _wait_for_state(younger, QueuedState.IN_QUEUE)

    holder_child.release()  # slot freed -> both wake; the older claims immediately, the younger defers

    _wait_for_state(older, QueuedState.DISPATCHED)
    assert younger.state == QueuedState.IN_QUEUE

    older_child.release()
    _wait_for_termination(younger)
    assert older.termination.status == TerminationStatus.COMPLETED
    assert younger.termination.status == TerminationStatus.COMPLETED


def test_multiple_slots_run_children_concurrently():
    group = ConcurrencyGroup('par_group', max_executions=2)
    fake_env = FakeEnvironment()
    child_a, child_b = TestPhase('c1', wait=True), TestPhase('c2', wait=True)
    queue_a = ExecutionQueue('Q1', group, child_a)
    queue_b = ExecutionQueue('Q2', group, child_b)

    fake_env.create_instance('j1', 'r1', queue_a).run(in_background=True)
    fake_env.create_instance('j2', 'r1', queue_b).run(in_background=True)

    _wait_for_state(queue_a, QueuedState.DISPATCHED)
    _wait_for_state(queue_b, QueuedState.DISPATCHED)  # both slots claimable concurrently

    child_a.release()
    child_b.release()
