from datetime import datetime, UTC

from runtools.runjob.track import StatusTracker


def test_add_event():
    tracker = StatusTracker()

    tracker.event('e1')
    tracker.event('e2')

    assert tracker.to_status().last_event.text == 'e2'


def test_operation_updates():
    tracker = StatusTracker()
    tracker.operation('op1').update(1, 10, 'items')

    op1 = tracker.to_status().operations[0]
    assert op1.name == 'op1'
    assert op1.completed == 1
    assert op1.total == 10
    assert op1.unit == 'items'


def test_operation_incr_update():
    tracker = StatusTracker()
    tracker.operation('op1').update(1)
    tracker.operation('op1').update(1)

    op1 = tracker.to_status().operations[0]
    assert op1.name == 'op1'
    assert op1.completed == 2
    assert op1.total is None
    assert op1.unit == ''

    tracker.operation('op2').update(5, 10)
    tracker.operation('op2').update(4, 10)
    op1 = tracker.to_status().operations[1]
    assert op1.completed == 9
    assert op1.total == 10


def test_inactive_operations():
    tracker = StatusTracker()

    # Operation becomes inactive when finished
    op = tracker.operation("copy")
    op.update(10, 10, "files")  # Complete the operation
    assert tracker.to_status().operations[0].is_active  # Still active until event

    tracker.event("Done")  # Should deactivate finished operations
    assert not tracker.to_status().operations[0].is_active


def test_result_handling():
    tracker = StatusTracker()

    # Result can be set and retrieved
    tracker.result("Success")
    assert tracker.to_status().result.text == "Success"

    # Result persists after new operations/events
    tracker.operation("test").update(1, 10)
    tracker.event("Still working")
    assert tracker.to_status().result.text == "Success"


def test_multiple_operations_same_name():
    tracker = StatusTracker()

    # Same operation name should return same tracker
    op1 = tracker.operation("test")
    op2 = tracker.operation("test")
    assert op1 is op2

    # Updates should accumulate on same operation
    op1.update(5, 10)
    op2.update(3, 10)
    status = tracker.to_status()
    assert len(status.operations) == 1
    assert status.operations[0].completed == 8


def test_event_timestamp():
    tracker = StatusTracker()
    now = datetime.now(UTC)

    # Event with explicit timestamp
    tracker.event("test", timestamp=now)
    assert tracker.to_status().last_event.timestamp == now

    # Event without timestamp should use current time
    tracker.event("test2")
    assert tracker.to_status().last_event.timestamp is not None


def test_tracker_warnings():
    tracker = StatusTracker()

    # Test adding single warning
    tracker.warning("Low disk space")
    assert len(tracker.to_status().warnings) == 1
    assert tracker.to_status().warnings[0].text == "Low disk space"

    # Test adding multiple warnings
    tracker.warning("Network unstable")
    assert len(tracker.to_status().warnings) == 2
    assert tracker.to_status().warnings[1].text == "Network unstable"

    # Test warnings persist with other updates
    tracker.event("Processing")
    tracker.operation("Copy").update(45, 100, "files")
    assert len(tracker.to_status().warnings) == 2

    # Test warnings included in status string
    status = tracker.to_status()
    assert str(status) == "[Copy 45/100 files (45%)]...  Processing  (!Low disk space, Network unstable)"

    # Test warnings with result
    tracker.result("Failed")
    assert str(tracker.to_status()) == "Failed  (!Low disk space, Network unstable)"
