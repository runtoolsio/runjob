"""MutualExclusionPhase — claim-then-act overlap behavior."""
import time

import pytest

from runtools.runcore.run import JobCompletionError, TerminationStatus
from runtools.runjob.coord import MutualExclusionPhase
from runtools.runjob.test.env import FakeEnvironment
from runtools.runjob.test.phase import TestPhase


def _wait_for_child_running(child, timeout=2):
    deadline = time.monotonic() + timeout
    while not child.started_at:
        if time.monotonic() > deadline:
            raise TimeoutError(f"Protected child did not start within {timeout}s")
        time.sleep(0.01)


def test_contender_overlaps_while_group_claim_held():
    fake_env = FakeEnvironment()
    protected = TestPhase('protected', wait=True)
    holder = MutualExclusionPhase('MUTEX_A', protected, exclusion_group='group')
    contender = MutualExclusionPhase('MUTEX_B', TestPhase('other'), exclusion_group='group')

    fake_env.create_instance('job_a', 'r1', holder).run(in_background=True)
    _wait_for_child_running(protected)  # child running => group claim held

    with pytest.raises(JobCompletionError):
        fake_env.create_instance('job_b', 'r1', contender).run()
    assert contender.termination.status == TerminationStatus.OVERLAP

    protected.release()


def test_claim_released_after_protected_child_completes():
    fake_env = FakeEnvironment()
    first = MutualExclusionPhase('MUTEX_A', TestPhase('p1'), exclusion_group='group')
    second = MutualExclusionPhase('MUTEX_B', TestPhase('p2'), exclusion_group='group')

    fake_env.create_instance('job_a', 'r1', first).run()
    fake_env.create_instance('job_b', 'r1', second).run()

    assert first.termination.status == TerminationStatus.COMPLETED
    assert second.termination.status == TerminationStatus.COMPLETED


def test_different_groups_do_not_contend():
    fake_env = FakeEnvironment()
    protected = TestPhase('p1', wait=True)
    holder = MutualExclusionPhase('MUTEX_A', protected, exclusion_group='group_a')
    other = MutualExclusionPhase('MUTEX_B', TestPhase('p2'), exclusion_group='group_b')

    fake_env.create_instance('job_a', 'r1', holder).run(in_background=True)
    _wait_for_child_running(protected)

    fake_env.create_instance('job_b', 'r1', other).run()
    assert other.termination.status == TerminationStatus.COMPLETED

    protected.release()


def test_exclusion_group_defaults_to_job_id():
    fake_env = FakeEnvironment()
    protected = TestPhase('p1', wait=True)
    holder = MutualExclusionPhase('MUTEX_A', protected)
    contender = MutualExclusionPhase('MUTEX_B', TestPhase('p2'))

    fake_env.create_instance('same_job', 'r1', holder).run(in_background=True)
    _wait_for_child_running(protected)

    with pytest.raises(JobCompletionError):
        fake_env.create_instance('same_job', 'r2', contender).run()
    assert contender.termination.status == TerminationStatus.OVERLAP

    protected.release()
