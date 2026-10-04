"""Output publication and terminal-state ordering using the actual Postgres transport over SQLite."""
import pytest

from runtools.runcore.db import sqlite
from runtools.runcore.job import InstanceLifecycleEvent, InstanceOutputEvent
from runtools.runcore.proxy import SnapshotJobInstanceProxy
from runtools.runcore.run import Stage
from runtools.runcore.transport.db import PollingInstanceDirectory
from runtools.runcore.util.lock import MemoryLockProvider
from runtools.runjob import node
from runtools.runjob.test.phase import TestPhase
from runtools.runjob.transport.postgres import PostgresInstanceAccessPoint


@pytest.fixture
def runtime(monkeypatch):
    db = sqlite.create_memory('output_finalization')
    db.open()
    access = PostgresInstanceAccessPoint(db, tail_cap=100)
    directory = PollingInstanceDirectory(db, lambda run: SnapshotJobInstanceProxy(run, db, db))
    # Drive the consumer and publisher explicitly to exercise ordering without timer races.
    monkeypatch.setattr(access, 'start', lambda: None)
    monkeypatch.setattr(directory, 'open', lambda: None)
    env = node.compose('output_finalization', db, access, directory, MemoryLockProvider(), (), (), True)
    env._persist_flush_interval = 3600
    with env:
        yield env, db, directory


def test_final_output_is_readable_as_soon_as_terminal_state_is_stored(runtime, monkeypatch):
    env, db, directory = runtime
    events = []
    directory.notifications.add_observer_output(events.append)
    directory.notifications.add_observer_lifecycle(events.append)
    inst = env.create_instance('job', 'run', TestPhase(output_text='last words'))
    env._persister.flush()
    directory.reconcile()
    store = db.store_runs
    output_at_terminal_write = []

    def observe_terminal_write(*runs):
        output_at_terminal_write.extend(db.read_output_tail(inst.id, 0))
        store(*runs)
        directory.reconcile()

    monkeypatch.setattr(db, 'store_runs', observe_terminal_write)
    inst.run()

    assert [line.message for line in output_at_terminal_write] == ['last words']
    assert [e.output_line.message for e in events if isinstance(e, InstanceOutputEvent)] == ['last words']
    output_index = next(i for i, e in enumerate(events) if isinstance(e, InstanceOutputEvent))
    ended_index = next(i for i, e in enumerate(events)
                       if isinstance(e, InstanceLifecycleEvent) and e.new_stage == Stage.ENDED)
    assert output_index < ended_index
    assert directory.get_instance(inst.id) is None


def test_failed_output_flush_still_stores_terminal_state_and_retries_output(runtime, monkeypatch, caplog):
    env, db, directory = runtime
    inst = env.create_instance('job', 'run', TestPhase(output_text='last words'))
    env._persister.flush()

    def failed_append(lines):
        raise OSError('output storage unavailable')

    with monkeypatch.context() as patch:
        patch.setattr(db, 'append_output', failed_append)
        inst.run()

    assert len(db.read_runs()) == 1  # the cache failure never costs the history record
    assert db.read_active_runs() == []
    assert 'Access point finalization failed' in caplog.text

    env._access_point._tail_publisher.flush()  # the periodic retry, driven explicitly

    assert [line.message for line in db.read_output_tail(inst.id, 0)] == ['last words']
