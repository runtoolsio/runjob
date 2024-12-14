from datetime import datetime

from runtools.runcore.util import KVParser, iso_date_time_parser
from runtools.runcore.output import OutputLine
from runtools.runjob.track import OutputToStatusTransformer, StatusTracker


def test_parse_event():
    tracker = StatusTracker()
    parser = OutputToStatusTransformer(tracker, parsers=[KVParser()])

    parser.new_output(OutputLine('no events here'))
    assert tracker.to_status().last_event is None

    parser.new_output(OutputLine('non_existing_field=[huh]'))
    assert tracker.to_status().last_event is None

    parser.new_output(OutputLine('event=[eventim_apollo] in hammersmith'))
    assert tracker.to_status().last_event.text == 'eventim_apollo'

    parser.new_output(OutputLine('second follows: event=[event_horizon]'))
    assert tracker.to_status().last_event.text == 'event_horizon'


def test_operation_without_name():
    tracker = StatusTracker()
    sut = OutputToStatusTransformer(tracker, parsers=[KVParser()])

    sut.new_output(OutputLine('operation without name completed=[5]'))
    assert tracker.to_status().last_event is None
    assert tracker.to_status().operations[0].completed == 5


def test_event_timestamps():
    tracker = StatusTracker()
    sut = OutputToStatusTransformer(tracker, parsers=[KVParser(post_parsers=[(iso_date_time_parser('timestamp'))])])

    sut.new_output(OutputLine('2020-10-01 10:30:30 event=[e1]'))
    assert tracker.to_status().last_event.timestamp == datetime.strptime('2020-10-01 10:30:30', "%Y-%m-%d %H:%M:%S")

    sut.new_output(OutputLine('2020-10-01T10:30:30.543 event=[e1]'))
    assert tracker.to_status().last_event.timestamp == datetime.strptime('2020-10-01 10:30:30.543',
                                                                         "%Y-%m-%d %H:%M:%S.%f")


def test_parse_progress():
    tracker = StatusTracker()
    sut = OutputToStatusTransformer(tracker, parsers=[KVParser(aliases={'count': 'completed'})])

    sut.new_output(OutputLine("event=[downloaded] count=[10] total=[100] unit=[files]"))
    task = tracker.to_status()
    op = task.operations[0]

    assert op.name == 'downloaded'
    assert op.completed == 10
    assert op.total == 100
    assert op.unit == 'files'


def test_multiple_parsers_and_tasks():
    def fake_parser(_):
        return {'timestamp': '2020-10-01 10:30:30'}

    tracker = StatusTracker()
    # Test multiple parsers can be used together to parse the same input
    sut = OutputToStatusTransformer(tracker,
                                    parsers=[KVParser(value_split=":"), KVParser(field_split="&"), fake_parser])

    sut.new_output(OutputLine('task:task1'))
    sut.new_output(OutputLine('?time=2.3&task=task2&event=e1'))
    status = tracker.to_status()
    assert status.last_event.text == 'e1'
    assert str(status.last_event.timestamp) == '2020-10-01 10:30:30'


def test_operation_when_progress():
    tracker = StatusTracker()
    sut = OutputToStatusTransformer(tracker, parsers=[KVParser()])
    sut.new_output(OutputLine("event=[upload]"))
    sut.new_output(OutputLine("event=[decoding] completed=[10]"))

    assert tracker.to_status().last_event.text == 'upload'


def test_event_deactivate_completed_operation():
    tracker = StatusTracker()
    sut = OutputToStatusTransformer(tracker, parsers=[KVParser()])

    sut.new_output(OutputLine("event=[encoding] completed=[10] total=[10]"))
    assert tracker.to_status().operations[0].finished
    assert tracker.to_status().operations[0].is_active

    sut.new_output(OutputLine("event=[new_event]"))
    assert tracker.to_status().operations[0].finished
    assert not tracker.to_status().operations[0].is_active


def test_task_started_and_updated_on_operation():
    tracker = StatusTracker()
    sut = OutputToStatusTransformer(tracker, parsers=[KVParser(), iso_date_time_parser('timestamp')])

    sut.new_output(OutputLine('2020-10-01 14:40:00 event=[op1] completed=[200]'))
    sut.new_output(OutputLine('2020-10-01 15:30:30 event=[op1] completed=[400]'))

    op = tracker.to_status().find_operation('op1')
    assert op.created_at == datetime(2020, 10, 1, 14, 40, 0)
    assert op.updated_at == datetime(2020, 10, 1, 15, 30, 30)


def test_op_end_date():
    tracker = StatusTracker()
    sut = OutputToStatusTransformer(tracker, parsers=[KVParser(), iso_date_time_parser('timestamp')])
    sut.new_output(OutputLine('2020-10-01 14:40:00 event=[op1] completed=[5] total=[10]'))
    assert not tracker.to_status().find_operation('op1').finished

    sut.new_output(OutputLine('2020-10-01 15:30:30 event=[op1] completed=[10] total=[10]'))
    assert tracker.to_status().find_operation('op1').updated_at == datetime(2020, 10, 1, 15, 30, 30)
    assert tracker.to_status().find_operation('op1').finished


def test_result():
    tracker = StatusTracker()
    sut = OutputToStatusTransformer(tracker, parsers=[KVParser()])

    sut.new_output(OutputLine('2020-10-01 10:30:30 event=[e1]'))
    sut.new_output(OutputLine('result=[res]'))
    assert tracker.to_status().result == 'res'


def test_error_output():
    tracker = StatusTracker()
    sut = OutputToStatusTransformer(tracker, parsers=[KVParser()])

    sut.new_output(OutputLine('event=[normal_event]', is_error=False))
    assert tracker.to_status().last_event.text == 'normal_event'

    sut.new_output(OutputLine('event=[error_event]', is_error=True))
    assert tracker.to_status().last_event.text == 'error_event'