"""Node-side output tail publisher — instance output events staged and flushed coalesced to
the environment db, driven by calling ``flush()`` directly (the access point's poll loop is
never started). Backed by a real in-memory SQLite store standing in for any OutputTailStorage."""
import pytest

from runtools.runcore.db import sqlite
from runtools.runcore.job import InstanceID, InstanceOutputEvent, JobInstanceMetadata
from runtools.runcore.output import OutputLine
from runtools.runcore.util import utc_now
from runtools.runjob.output.tail import OutputTailPublisher


@pytest.fixture
def db():
    with sqlite.create_memory('test_env') as database:
        yield database


def _publish(publisher, instance_id, n, message=None):
    """Feed one line through the publisher's real intake seam — the output observer method."""
    line = OutputLine(message or f"line {n}", n)
    publisher.instance_output_update(
        InstanceOutputEvent(JobInstanceMetadata(instance_id, {}, (), ()), line, utc_now()))


def test_flush_publishes_staged_lines_across_instances(db):
    publisher = OutputTailPublisher(db, cap=100)
    a, b = InstanceID('a_job', 'r1', 1), InstanceID('b_job', 'r1', 1)

    _publish(publisher, a, 1)
    _publish(publisher, b, 1)
    _publish(publisher, a, 2)
    publisher.flush()

    assert [line.ordinal for line in db.read_output_tail(a, max_lines=0)] == [1, 2]
    assert [line.ordinal for line in db.read_output_tail(b, max_lines=0)] == [1]


def test_flush_writes_only_newest_cap_lines_per_instance(db):
    publisher = OutputTailPublisher(db, cap=3)
    a = InstanceID('a_job', 'r1', 1)

    for n in range(1, 11):
        _publish(publisher, a, n)
    publisher.flush()  # anything below the cap would be pruned right away -> never written

    assert [line.ordinal for line in db.read_output_tail(a, max_lines=0)] == [8, 9, 10]


def test_failed_flush_retains_staged_lines(db, monkeypatch):
    publisher = OutputTailPublisher(db, cap=100)
    a = InstanceID('a_job', 'r1', 1)
    _publish(publisher, a, 1)

    original = db.append_output

    def failing(lines):
        raise IOError("db gone")

    monkeypatch.setattr(db, 'append_output', failing)
    with pytest.raises(IOError):
        publisher.flush()
    monkeypatch.setattr(db, 'append_output', original)

    publisher.flush()  # the batch was retained -> lands on the retry tick

    assert [line.ordinal for line in db.read_output_tail(a, max_lines=0)] == [1]


def test_prune_keeps_table_at_cap_across_flushes(db):
    publisher = OutputTailPublisher(db, cap=3)
    a = InstanceID('a_job', 'r1', 1)

    for n in (1, 2):
        _publish(publisher, a, n)
    publisher.flush()
    for n in (3, 4):
        _publish(publisher, a, n)
    publisher.flush()  # 4 lines appended since the last prune >= cap -> prunes to the newest 3

    assert [line.ordinal for line in db.read_output_tail(a, max_lines=0)] == [2, 3, 4]


def test_close_flushes_remaining_lines(db):
    publisher = OutputTailPublisher(db, cap=100)
    a = InstanceID('a_job', 'r1', 1)
    _publish(publisher, a, 1)

    publisher.close()

    assert [line.ordinal for line in db.read_output_tail(a, max_lines=0)] == [1]


def test_failed_prune_is_retried_on_next_flush(db, monkeypatch):
    publisher = OutputTailPublisher(db, cap=3)
    a = InstanceID('a_job', 'r1', 1)
    for n in (1, 2):
        _publish(publisher, a, n)
    publisher.flush()
    for n in (3, 4):
        _publish(publisher, a, n)

    original = db.prune_output_tail

    def failing(instance_id, keep):
        raise IOError("db gone")

    monkeypatch.setattr(db, 'prune_output_tail', failing)
    with pytest.raises(IOError):
        publisher.flush()  # lines appended, prune due but failed
    monkeypatch.setattr(db, 'prune_output_tail', original)

    publisher.flush()  # nothing staged -- the pending prune still runs

    assert [line.ordinal for line in db.read_output_tail(a, max_lines=0)] == [2, 3, 4]


def test_unregistered_instance_gets_final_prune(db):
    """An instance ending mid-accumulation gets a final prune at unregistration (finalize),
    so its tail does not stay over cap waiting for output that never comes."""
    publisher = OutputTailPublisher(db, cap=3)
    a = InstanceID('a_job', 'r1', 1)
    for n in (1, 2):
        _publish(publisher, a, n)
    publisher.flush()                      # 2 rows persisted, counter below cap
    for n in (3, 4):
        _publish(publisher, a, n)
    publisher.finalize(a)                  # instance unregistered with its final lines still staged

    publisher.flush()

    assert [line.ordinal for line in db.read_output_tail(a, max_lines=0)] == [2, 3, 4]
