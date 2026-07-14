"""Postgres environment node — end-to-end producer slice over a real Postgres.

Covers the postgres kind's produce+observe contract: a node runs jobs whose state reaches
remote connectors through the persister -> polling directory lane, coordination excludes
across nodes via advisory locks, and duplicates are rejected by the DB unique constraint.
"""
import os
import time
from threading import Event

import pytest

from runtools.runcore import connector
from runtools.runcore.db import postgres
from runtools.runcore.env import EnvironmentEntry, EnvironmentKind, PostgresEnvironmentConfig
from runtools.runcore.job import DuplicateInstanceError
from runtools.runcore.run import JobCompletionError, TerminationStatus
from runtools.runcore.util import unique_timestamp_hex
from runtools.runjob import node
from runtools.runjob.coord import MutualExclusionPhase
from runtools.runjob.phase import BasePhase
from runtools.runjob.test.phase import TestPhase


class _BlockingPhase(BasePhase):
    """Blocks until released — unlike TestPhase(wait=True), whose wait self-expires after 2s,
    which is shorter than the observation lane's worst-case flush + idle-poll latency."""

    def __init__(self, phase_id):
        super().__init__(phase_id, 'TEST')
        self.block = Event()

    def release(self):
        self.block.set()

    def _run(self, ctx):
        self.block.wait(30)

    def _stop_running(self, reason):
        self.block.set()


def _docker_available():
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def pg_dsn():
    """A libpq DSN for a real Postgres — an existing one via RUNTOOLS_PG_TEST_DSN, else a
    throwaway testcontainers instance for the session."""
    dsn = os.getenv("RUNTOOLS_PG_TEST_DSN")
    if dsn:
        yield dsn
        return
    if not _docker_available():
        pytest.skip("Postgres tests need Docker (or RUNTOOLS_PG_TEST_DSN)")
    from testcontainers.postgres import PostgresContainer
    with PostgresContainer("postgres:16-alpine") as container:
        host, port = container.get_container_host_ip(), container.get_exposed_port(5432)
        yield f"postgresql://{container.username}:{container.password}@{host}:{port}/{container.dbname}"


@pytest.fixture
def pg_entry(pg_dsn):
    entry = EnvironmentEntry(id=f"pgnode_{unique_timestamp_hex()}", kind=EnvironmentKind.POSTGRES, location=pg_dsn)
    postgres.create_environment(entry, PostgresEnvironmentConfig())
    try:
        yield entry
    finally:
        postgres.delete(entry)


def _wait_until(condition, timeout=5, message="Condition not met"):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise TimeoutError(f"{message} within {timeout}s")
        time.sleep(0.05)


def test_node_runs_job_and_remote_connector_observes(pg_entry):
    child = _BlockingPhase('work')
    with node.connect(pg_entry) as env_node, connector.connect(pg_entry) as conn:
        inst = env_node.create_instance('pg_job', 'r1', child)
        inst.run(in_background=True)

        # State flows node -> persister flush -> runs table -> polling directory
        _wait_until(lambda: [r for r in conn.get_active_runs() if r.job_id == 'pg_job'],
                    message="Run not observed by remote connector")

        child.release()
        _wait_until(lambda: not conn.get_active_runs(), message="Run not evicted after completion")
        history = conn.read_runs()
        assert [r.job_id for r in history] == ['pg_job']
        assert history[0].lifecycle.is_ended


def test_node_reads_show_own_instance_immediately(pg_entry):
    child = TestPhase('work', wait=True)
    with node.connect(pg_entry) as env_node:
        inst = env_node.create_instance('own_job', 'r1', child)
        inst.run(in_background=True)

        # Own instances come from node memory, not the (lagging) polled view
        assert [r.job_id for r in env_node.get_active_runs()] == ['own_job']

        child.release()


def test_mutex_excludes_across_nodes(pg_entry):
    protected = TestPhase('protected', wait=True)
    with node.connect(pg_entry) as node_a, node.connect(pg_entry) as node_b:
        holder = node_a.create_instance('job_a', 'r1', MutualExclusionPhase('MUTEX', protected, exclusion_group='xg'))
        holder.run(in_background=True)
        _wait_until(lambda: protected.started_at, message="Protected child not started")

        contender_phase = MutualExclusionPhase('MUTEX', TestPhase('other'), exclusion_group='xg')
        contender = node_b.create_instance('job_b', 'r1', contender_phase)
        with pytest.raises(JobCompletionError):
            contender.run()
        assert contender_phase.termination.status == TerminationStatus.OVERLAP

        protected.release()


def test_duplicate_run_rejected_across_nodes(pg_entry):
    child = TestPhase('work', wait=True)
    with node.connect(pg_entry) as node_a, node.connect(pg_entry) as node_b:
        inst = node_a.create_instance('dup_job', 'r1', child)
        inst.run(in_background=True)

        with pytest.raises(DuplicateInstanceError):
            node_b.create_instance('dup_job', 'r1', TestPhase('other'))

        child.release()


def test_remote_stop_via_signal(pg_entry):
    control_events = []

    class ControlRecorder:
        def instance_control_update(self, event):
            control_events.append(event)

    child = _BlockingPhase('work')
    with node.connect(pg_entry) as env_node, connector.connect(pg_entry) as conn:
        conn.notifications.add_observer_control(ControlRecorder())
        env_node.create_instance('stoppable', 'r1', child).run(in_background=True)
        _wait_until(lambda: [i for i in conn.get_instances() if i.job_id == 'stoppable'],
                    message="Run not observed by remote connector")

        [proxy] = [i for i in conn.get_instances() if i.job_id == 'stoppable']
        proxy.stop()  # -> signal row -> node reconciler -> instance.stop -> recorded + state lane

        _wait_until(lambda: conn.read_runs(), timeout=15, message="Run did not stop")
        [ended] = conn.read_runs()
        [request] = ended.control_requests
        assert request.op == 'stop'
        _wait_until(lambda: control_events, message="Control event not synthesized for consumer")
        assert control_events[0].request.op == 'stop'


def test_remote_approve_via_signal(pg_entry):
    from runtools.runcore.matching import PhaseCriterion
    from runtools.runjob.coord import ApprovalPhase

    gated = ApprovalPhase('gate', TestPhase('protected'))
    with node.connect(pg_entry) as env_node, connector.connect(pg_entry) as conn:
        env_node.create_instance('approvable', 'r1', gated).run(in_background=True)
        _wait_until(lambda: [i for i in conn.get_instances() if i.job_id == 'approvable'],
                    message="Run not observed by remote connector")

        [proxy] = [i for i in conn.get_instances() if i.job_id == 'approvable']
        control = proxy.find_phase_control(PhaseCriterion(phase_id='gate'))
        control.approve()  # -> signal -> reconciler -> ApprovalPhase.approve() -> job proceeds

        _wait_until(lambda: conn.read_runs(), timeout=15, message="Run did not complete after approval")
        [ended] = conn.read_runs()
        assert ended.lifecycle.is_ended
        [request] = ended.control_requests
        assert (request.op, request.phase_id) == ('approve', 'gate')


def test_node_publishes_output_tail(pg_entry):
    from runtools.runcore.db import postgres

    child = TestPhase('work', output_text='tail me')
    with node.connect(pg_entry) as env_node, postgres.create(pg_entry) as env_db:
        inst = env_node.create_instance('tailed', 'r1', child)
        inst.run()

        # The publisher flushes on the node's persister cadence; the ended run's tail lingers
        _wait_until(lambda: env_db.read_output_tail(inst.id, max_lines=0),
                    message="Output tail not published")
        [line] = env_db.read_output_tail(inst.id, max_lines=0)
        assert line.message == 'tail me'
