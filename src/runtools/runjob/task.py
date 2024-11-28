import logging
from enum import Enum
from typing import Set

from runtools.runcore import util
from runtools.runcore.util import convert_if_number


class Fields(Enum):
    EVENT = {'event', 'message', 'msg'}
    OPERATION = {'operation', 'op', 'task'}
    TIMESTAMP = {'timestamp', 'time', 'ts'}
    COMPLETED = {'completed', 'done', 'processed', 'count', 'increment', 'incr', 'added'}
    TOTAL = {'total', 'max', 'target'}
    UNIT = {'unit'}
    RESULT = {'result', 'status'}

    def __init__(self, aliases: Set[str]):
        self.aliases = aliases

    @classmethod
    def find_field(cls, key: str) -> 'Fields | None':
        """Find field enum by any of its aliases"""
        key = key.lower()
        for field in cls:
            if key in field.aliases:
                return field
        return None


DEFAULT_PATTERN = ''


def field_conversion(parsed: dict) -> dict:
    """Convert parsed fields with alias support"""
    converted = {}

    for key, value in parsed.items():
        if field := Fields.find_field(key):
            if field == Fields.TIMESTAMP:
                value = util.parse_datetime(value)
            elif field in {Fields.COMPLETED, Fields.TOTAL}:
                value = convert_if_number(value)
            if value:
                converted[field] = value

    return converted


class OutputToStatus:

    def __init__(self, status_tracker, *, parsers, conversion=field_conversion):
        self.tracker = status_tracker
        self.parsers = list(parsers)
        self.conversion = conversion

    def __call__(self, output, is_error=False):
        self.new_output(output, is_error)

    def create_logging_handler(self):
        """
        Creates and returns a logging.Handler instance that forwards log records to this instance.
        """
        class InternalHandler(logging.Handler):
            def __init__(self, outer_instance):
                super().__init__()
                self.outer_instance = outer_instance

            def emit(self, record):
                output = self.format(record)  # Convert log record to a string
                is_error = record.levelno >= logging.ERROR
                self.outer_instance(output, is_error)

        return InternalHandler(self)

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

        self._update_status(kv)

    def _update_status(self, fields):
        is_op = self._update_operation(fields)
        if not is_op and (event := fields.get(Fields.EVENT)):
            self.tracker.event(event, timestamp=fields.get(Fields.TIMESTAMP))

        if result := fields.get(Fields.RESULT):
            self.tracker.result(result)

    def _update_operation(self, fields):
        op_name = fields.get(Fields.OPERATION) or fields.get(Fields.EVENT)
        ts = fields.get(Fields.TIMESTAMP)
        completed = fields.get(Fields.COMPLETED)
        total = fields.get(Fields.TOTAL)
        unit = fields.get(Fields.UNIT)

        if not any((completed, total, unit)):
            return False

        op = self.tracker.operation(op_name, timestamp=ts)
        op.update(completed, total, unit, ts)
        return True
