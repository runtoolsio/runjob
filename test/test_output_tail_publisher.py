"""Node-side output tail publisher — instance output events staged and flushed coalesced to
the environment db, driven by calling ``flush()`` directly (the access point's poll loop is
never started). Backed by a real in-memory SQLite store standing in for any OutputTailStorage."""
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from threading import Event

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


def test_finalized_instance_gets_final_prune(db):
    """An instance ending mid-accumulation gets a final prune when finalized,
    so its tail does not stay over cap waiting for output that never comes."""
    publisher = OutputTailPublisher(db, cap=3)
    a = InstanceID('a_job', 'r1', 1)
    for n in (1, 2):
        _publish(publisher, a, n)
    publisher.flush()                      # 2 rows persisted, counter below cap
    for n in (3, 4):
        _publish(publisher, a, n)
    publisher.finalize(a)                  # instance finalized with its final lines still staged

    publisher.flush()

    assert [line.ordinal for line in db.read_output_tail(a, max_lines=0)] == [2, 3, 4]


def test_completion_flush_waits_for_periodic_write_in_flight(db, monkeypatch):
    publisher = OutputTailPublisher(db, cap=100)
    iid = InstanceID('job', 'r1', 1)
    _publish(publisher, iid, 1)
    writing, resume, completing = Event(), Event(), Event()
    append = db.append_output

    def paused_append(lines):
        writing.set()
        assert resume.wait(5)
        append(lines)

    def completion_flush():
        completing.set()
        publisher.flush()

    monkeypatch.setattr(db, 'append_output', paused_append)
    with ThreadPoolExecutor(max_workers=2) as executor:
        periodic = executor.submit(publisher.flush)
        try:
            assert writing.wait(5)
            completion = executor.submit(completion_flush)
            assert completing.wait(5)
            with pytest.raises(TimeoutError):
                completion.result(timeout=0.1)
        finally:
            resume.set()
        periodic.result(timeout=5)
        completion.result(timeout=5)

    assert [line.ordinal for line in db.read_output_tail(iid, 0)] == [1]


def test_finalize_during_in_flight_flush_still_forces_final_prune(db, monkeypatch):
    publisher = OutputTailPublisher(db, cap=3)
    iid = InstanceID('job', 'r1', 1)
    for n in (1, 2, 3):
        _publish(publisher, iid, n)
    publisher.flush()  # reaches cap: pruned, counter cleared
    for n in (4, 5):
        _publish(publisher, iid, n)
    writing, resume = Event(), Event()
    append = db.append_output

    def paused_append(lines):
        writing.set()
        assert resume.wait(5)
        append(lines)

    monkeypatch.setattr(db, 'append_output', paused_append)
    with ThreadPoolExecutor(max_workers=2) as executor:
        periodic = executor.submit(publisher.flush)
        assert writing.wait(5)  # lines 4-5 are mid-write: neither staged nor counted yet
        finalizing = executor.submit(publisher.finalize, iid)
        resume.set()
        periodic.result(timeout=5)
        finalizing.result(timeout=5)
    publisher.flush()  # the completion flush

    assert [line.ordinal for line in db.read_output_tail(iid, 0)] == [3, 4, 5]
