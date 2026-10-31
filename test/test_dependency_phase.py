from contextlib import suppress
from dataclasses import replace
from datetime import timedelta

from runtools.runcore.matching import JobRunCriteria
from runtools.runcore.run import JobCompletionError, TerminationStatus
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.util import utc_now
from runtools.runjob.coord import DependencyPhase
from runtools.runjob.test.env import FakeEnvironment


def _active_dependency(heartbeat_age):
    return replace(fake_job_run('dep', term_status=None), heartbeat_at=utc_now() - heartbeat_age)


def _run_dependent(env):
    phase = DependencyPhase(JobRunCriteria.job_match('dep'))
    with suppress(JobCompletionError):  # Raised for an unsuccessful run
        env.create_instance('dependent', 'r1', phase).run()
    return phase.termination.status


def test_running_dependency_satisfies():
    env = FakeEnvironment(active_runs=[_active_dependency(timedelta(seconds=1))])

    assert _run_dependent(env) == TerminationStatus.COMPLETED


def test_missing_dependency_is_unsatisfied():
    assert _run_dependent(FakeEnvironment()) == TerminationStatus.UNSATISFIED


def test_lost_dependency_is_unsatisfied():
    env = FakeEnvironment(active_runs=[_active_dependency(timedelta(minutes=5))])

    assert _run_dependent(env) == TerminationStatus.UNSATISFIED
