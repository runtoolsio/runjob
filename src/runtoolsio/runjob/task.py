from enum import Enum

from tarotools.taro import util
from tarotools.taro.util import convert_if_number


class Fields(Enum):
    EVENT = 'event'
    OPERATION = 'operation'
    TASK = 'task'
    TIMESTAMP = 'timestamp'
    COMPLETED = 'completed'
    INCREMENT = 'increment'
    TOTAL = 'total'
    UNIT = 'unit'
    RESULT = 'result'


DEFAULT_PATTERN = ''


def field_conversion(parsed):
    converted = {
        Fields.EVENT: parsed.get(Fields.EVENT.value),
        Fields.TASK: parsed.get(Fields.TASK.value),
        Fields.TIMESTAMP: util.parse_datetime(parsed.get(Fields.TIMESTAMP.value)),
        Fields.COMPLETED: convert_if_number(parsed.get(Fields.COMPLETED.value)),
        Fields.INCREMENT: convert_if_number(parsed.get(Fields.INCREMENT.value)),
        Fields.TOTAL: convert_if_number(parsed.get(Fields.TOTAL.value)),
        Fields.UNIT: parsed.get(Fields.UNIT.value),
        Fields.RESULT: parsed.get(Fields.RESULT.value),
    }

    return {key: value for key, value in converted.items() if value is not None}


class TaskOutputParser:

    def __init__(self, task_tracker, parsers, conversion=field_conversion):
        self.tracker = task_tracker
        self.parsers = parsers
        self.conversion = conversion

    def __call__(self, output, is_error=False):
        self.new_output(output, is_error)

    def new_output(self, output, is_error=False):
        parsed = {}
        for parser in self.parsers:
            if parsed_kv := parser(output):
                parsed.update(parsed_kv)

        if not parsed:
            return

        kv = self.conversion(parsed)
        if not kv:
            return

        self._update_task(kv)

    def _update_task(self, fields):
        task = fields.get(Fields.TASK)
        prev_task = self.tracker.tasks[-1] if self.tracker.tasks else None
        is_finished = False
        if task:
            current_task = self.tracker.task(task)
            if prev_task == current_task:
                is_finished = True
        else:
            if prev_task and not prev_task.is_finished:
                current_task = prev_task
            else:
                current_task = self.tracker

        is_op = self._update_operation(task, fields)
        if event := fields.get(Fields.EVENT) and not is_op:
            task.add_event(event, fields.get(Fields.TIMESTAMP))

        result = fields.get(Fields.RESULT)
        if result or is_finished:
            current_task.finished(fields.get(Fields.RESULT))

    @staticmethod
    def _update_operation(task, fields):
        op_name = fields.get(Fields.OPERATION) or fields.get(Fields.EVENT)
        ts = fields.get(Fields.TIMESTAMP)
        completed = fields.get(Fields.COMPLETED)
        increment = fields.get(Fields.INCREMENT)
        total = fields.get(Fields.TOTAL)
        unit = fields.get(Fields.UNIT)

        if not completed and not increment and not total and not unit:
            return False

        op = task.operation(op_name)
        op.update(completed or increment, total, unit, increment=increment is not None, timestamp=ts)
        return True
