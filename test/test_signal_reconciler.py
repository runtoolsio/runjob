"""Signal reconciler policy — driven by calling the reconcile/sweep bodies directly (the poll
thread is never started), backed by a real in-memory SQLite store standing in for any
EnvironmentDatabase."""
from datetime import timedelta

import pytest

from runtools.runcore.db import sqlite
from runtools.runcore.job import InstanceID
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.util import utc_now
from runtools.runjob.transport.postgres import PostgresInstanceAccessPoint


@pytest.fixture
def db():
    with sqlite.create_memory('test_env') as database:
        yield database


def _backdate_signals(db, job_id, minutes):
    backdated = (utc_now() - timedelta(minutes=minutes)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    db._conn.execute("UPDATE signals SET requested_at = ? WHERE job_id = ?", (backdated, job_id))


def _backdate_heartbeat(db, job_id, minutes):
    backdated = (utc_now() - timedelta(minutes=minutes)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    db._conn.execute("UPDATE runs SET heartbeat_at = ? WHERE job_id = ?", (backdated, job_id))


def _active_target(db, job_id):
    """Materialized active run (init + snapshot); rows are born attested — init sets heartbeat_at."""
    run = fake_job_run(job_id, created_at=utc_now(), term_status=None)
    iid = run.metadata.instance_id
    db.init_run(iid.job_id, iid.run_id, created_at=run.lifecycle.created_at)
    db.store_active_runs(run)
    return iid


def test_sweep_takes_only_aged_signals_without_attested_target(db):
    """A crashed node's runs stay non-ended forever — a stale heartbeat, not lifecycle state,
    is what marks their pending signals unappliable."""
    access_point = PostgresInstanceAccessPoint(db)
    alive = _active_target(db, 'alive')                       # owner attesting -> live target
    crashed = _active_target(db, 'crashed')                   # non-ended row, owner stopped heartbeating
    _backdate_heartbeat(db, 'crashed', minutes=5)
    ghost = InstanceID('ghost', 'r1', 1)                      # no run row at all
    db.send_signal(alive, 'stop')
    db.send_signal(crashed, 'stop')
    db.send_signal(ghost, 'stop')
    db.send_signal(ghost, 'approve', phase_id='gate')
    for job_id in ('alive', 'crashed', 'ghost'):
        _backdate_signals(db, job_id, minutes=5)
    fresh_ghost = InstanceID('ghost2', 'r1', 1)
    db.send_signal(fresh_ghost, 'stop')  # orphan-to-be, but inside the age race guard

    access_point._sweep_orphans()

    # Aged + attested target: kept. Aged + dead owner or no target: swept. Fresh: kept (for now).
    assert [s.op for s in db.read_signals([alive])] == ['stop']
    assert db.read_signals([crashed]) == []
    assert db.read_signals([ghost]) == []
    assert [s.op for s in db.read_signals([fresh_ghost])] == ['stop']
