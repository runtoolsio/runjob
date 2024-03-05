from datetime import datetime

import pytest

from runtools.runcore.track import TaskTrackerMem
from runtools.runcore.util import KVParser, iso_date_time_parser
from runtools.runner.task import OutputToTask, Fields


def test_parse_event():
    tracker = TaskTrackerMem('task')
    parser = OutputToTask(tracker, parsers=[KVParser()])

    parser.new_output('no events here')
    assert tracker.tracked_task.current_event is None

    parser.new_output('non_existing_field=[huh]')
    assert tracker.tracked_task.current_event is None

    parser.new_output('event=[eventim_apollo] we have first event here')
    assert tracker.tracked_task.current_event.text == 'eventim_apollo'

    parser.new_output('second follows: event=[event_horizon]')
    assert tracker.tracked_task.current_event.text == 'event_horizon'


def test_operation_without_name():
    tracker = TaskTrackerMem('task')
    sut = OutputToTask(tracker, parsers=[KVParser()])

    sut.new_output('operation without name completed=[5]')
    assert tracker.tracked_task.current_event is None
    assert tracker.tracked_task.operations[0].completed == 5


def test_parse_timestamps():
    tracker = TaskTrackerMem('task')
    sut = OutputToTask(tracker, parsers=[KVParser(post_parsers=[(iso_date_time_parser(Fields.TIMESTAMP.value))])])

    sut.new_output('2020-10-01 10:30:30 event=[e1]')
    assert tracker.tracked_task.current_event.timestamp == datetime.strptime('2020-10-01 10:30:30', "%Y-%m-%d %H:%M:%S")

    sut.new_output('2020-10-01T10:30:30.543 event=[e1]')
    assert tracker.tracked_task.current_event.timestamp == datetime.strptime('2020-10-01 10:30:30.543', "%Y-%m-%d %H:%M:%S.%f")


def test_parse_progress():
    tracker = TaskTrackerMem('task')
    sut = OutputToTask(tracker, parsers=[KVParser(aliases={'count': 'completed'})])

    sut.new_output("event=[downloaded] count=[10] total=[100] unit=[files]")
    task = tracker.tracked_task
    assert task.operations[0].name == 'downloaded'
    assert task.operations[0].completed == 10
    assert task.operations[0].total == 100
    assert task.operations[0].unit == 'files'


def test_multiple_parsers_and_tasks():
    def fake_parser(_):
        return {'timestamp': '2020-10-01 10:30:30'}

    tracker = TaskTrackerMem('main')
    # Test multiple parsers can be used together to parse the same input
    sut = OutputToTask(tracker, parsers=[KVParser(value_split=":"), KVParser(field_split="&"), fake_parser])

    sut.new_output('task:task1')
    sut.new_output('?time=2.3&task=task2&event=e1')
    task = tracker.tracked_task
    assert task.subtasks[0].name == 'task1'
    assert task.subtasks[1].name == 'task2'
    assert task.subtasks[1].current_event.text == 'e1'
    assert str(task.subtasks[1].current_event.timestamp) == '2020-10-01 10:30:30'
    assert not task.current_event


@pytest.mark.skip(reason="legacy")
def test_operation_resets_last_event():
    tracker = TaskTrackerMem()
    sut = OutputToTask(tracker, parsers=[KVParser()])
    sut.new_output("event=[upload]")
    sut.new_output("event=[decoding] completed=[10]")

    assert tracker.tracked_task.current_event is None


@pytest.mark.skip(reason="legacy")
def test_event_deactivate_completed_operation():
    tracker = TaskTrackerMem()
    sut = OutputToTask(tracker, parsers=[KVParser()])

    sut.new_output("event=[encoding] completed=[10] total=[10]")
    assert tracker.tracked_task.operations[0].finished

    sut.new_output("event=[new_event]")
    assert not tracker.tracked_task.operations[0].finished


def test_subtask_event():
    tracker = TaskTrackerMem()
    sut = OutputToTask(tracker, parsers=[KVParser()])

    sut.new_output("event=[event_in_main_task]")
    sut.new_output("event=[event_in_subtask] task=[subtask1]")
    assert tracker.tracked_task.subtasks[0].current_event.text == 'event_in_subtask'

    sut.new_output("event=[another_event_in_main_task]")
    assert tracker.tracked_task.current_event.text == 'another_event_in_main_task'


def test_task_started_and_update_on_event():
    tracker = TaskTrackerMem()
    sut = OutputToTask(tracker, parsers=[KVParser(), iso_date_time_parser(Fields.TIMESTAMP.value)])
    sut.new_output('2020-10-01 10:30:30 event=[e1]')
    sut.new_output('2020-10-01 11:45:00 event=[e2]')
    assert tracker.tracked_task.updated_at == datetime(2020, 10, 1, 11, 45, 0)


def test_task_started_and_updated_on_operation():
    tracker = TaskTrackerMem()
    sut = OutputToTask(tracker, parsers=[KVParser(), iso_date_time_parser(Fields.TIMESTAMP.value)])
    sut.new_output('2020-10-01 14:40:00 event=[op1] completed=[200]')
    sut.new_output('2020-10-01 15:30:30 event=[op1] completed=[400]')
    started_ts = datetime(2020, 10, 1, 14, 40, 0)
    updated_ts = datetime(2020, 10, 1, 15, 30, 30)
    assert tracker.tracked_task.find_operation('op1').created_at == started_ts
    assert tracker.tracked_task.updated_at == updated_ts
    assert tracker.tracked_task.find_operation('op1').updated_at == updated_ts


def test_op_end_date():
    tracker = TaskTrackerMem()
    sut = OutputToTask(tracker, parsers=[KVParser(), iso_date_time_parser(Fields.TIMESTAMP.value)])
    sut.new_output('2020-10-01 14:40:00 event=[op1] completed=[5] total=[10]')
    assert not tracker.tracked_task.find_operation('op1').finished

    sut.new_output('2020-10-01 15:30:30 event=[op1] completed=[10] total=[10]')
    assert tracker.tracked_task.find_operation('op1').updated_at == datetime(2020, 10, 1, 15, 30, 30)
    assert tracker.tracked_task.find_operation('op1').finished


def test_subtask_started_and_finished():
    tracker = TaskTrackerMem()
    sut = OutputToTask(tracker, parsers=[KVParser(), iso_date_time_parser(Fields.TIMESTAMP.value)])
    sut.new_output('2020-10-01 12:30:00 task=[t1]')
    sut.new_output('2020-10-01 13:50:00 task=[t1] event=[e1]')

    started_ts = datetime(2020, 10, 1, 12, 30, 0)
    updated_ts = datetime(2020, 10, 1, 13, 50, 0)
    assert tracker.tracked_task.find_subtask('t1').created_at == started_ts
    assert tracker.tracked_task.find_subtask('t1').updated_at == updated_ts
    assert tracker.tracked_task.finished


def test_timestamps():
    tracker = TaskTrackerMem('task')
    sut = OutputToTask(tracker, parsers=[KVParser()])

    sut.new_output('2020-10-01 10:30:30 event=[e1]')
    sut.new_output('result=[res]')
    assert tracker.tracked_task.result == 'res'
