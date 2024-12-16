import re
from abc import ABC, abstractmethod
from datetime import datetime, UTC
from enum import Enum
from typing import Set, Optional, List

from runtools.runcore import util
from runtools.runcore.status import Event, Operation, Status
from runtools.runcore.util import convert_if_number
from runtools.runjob.output import OutputEnvironment


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


class OutputToStatusTransformer:

    def __init__(self, status_tracker, *, parsers, conversion=field_conversion):
        self.status_tracker = status_tracker
        self.parsers = list(parsers)
        self.conversion = conversion

    def __call__(self, output_line):
        self.new_output(output_line)

    def new_output(self, output_line):
        parsed = {}
        for parser in self.parsers:
            if parsed_kv := parser(output_line.text):
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
            self.status_tracker.event(event, timestamp=fields.get(Fields.TIMESTAMP))

        if result := fields.get(Fields.RESULT):
            self.status_tracker.result(result)

    def _update_operation(self, fields):
        op_name = fields.get(Fields.OPERATION) or fields.get(Fields.EVENT)
        ts = fields.get(Fields.TIMESTAMP)
        completed = fields.get(Fields.COMPLETED)
        total = fields.get(Fields.TOTAL)
        unit = fields.get(Fields.UNIT)

        if not any((completed, total, unit)):
            return False

        op = self.status_tracker.operation(op_name, timestamp=ts)
        op.update(completed, total, unit, ts)
        return True


class OperationTracker:

    def __init__(self, name: str, created_at: datetime = None):
        self.name = name
        self.completed = None
        self.total = None
        self.unit = ''
        self.created_at = created_at or datetime.now(UTC).replace(tzinfo=None)
        self.updated_at = self.created_at
        self.is_active = True
        self.result: Optional[str] = None

    def update(self,
               completed: Optional[float] = None,
               total: Optional[float] = None,
               unit: Optional[str] = None,
               updated_at: Optional[datetime] = None) -> None:
        if completed is not None:
            if not self.completed or completed > self.completed:
                self.completed = completed  # Assuming is total completed
            else:
                self.completed += completed  # Assuming it is an increment
        if total is not None:
            self.total = total
        if unit is not None:
            self.unit = unit
        self.updated_at = updated_at or datetime.now(UTC).replace(tzinfo=None)

    def finished(self, result, updated_at: Optional[datetime] = None) -> None:
        self.result = result
        self.updated_at = updated_at or datetime.now(UTC).replace(tzinfo=None)

    def parse_value(self, value):
        # Check if value is a string and extract number and unit
        if isinstance(value, str):
            match = re.match(r"(\d+(\.\d+)?)(\s*)(\w+)?", value)
            if match:
                number = float(match.group(1))
                unit = match.group(4) if match.group(4) else ''
                return number, unit
            else:
                raise ValueError("String format is not correct. Expected format: {number}{unit} or {number} {unit}")
        elif isinstance(value, (float, int)):
            return float(value), self.unit
        else:
            raise TypeError("Value must be in the format `{number}{unit}` or `{number} {unit}`, but it was: "
                            + str(value))

    @property
    def finished(self):
        return self.result is not None or (self.total and (self.completed == self.total))

    def to_operation(self) -> Operation:
        return Operation(
            self.name,
            self.completed,
            self.total,
            self.unit,
            self.created_at,
            self.updated_at,
            self.is_active,
            self.result
        )


class StatusTracker:

    def __init__(self):
        self._last_event: Optional[Event] = None
        self._operations: List[OperationTracker] = []
        self._warnings: List[Event] = []
        self._result: Optional[str] = None

    def event(self, text: str, timestamp=None) -> None:
        timestamp = timestamp or datetime.now(UTC).replace(tzinfo=None)
        self._last_event = Event(text, timestamp)
        for op in self._operations:
            if op.finished:
                op.is_active = False

    def warning(self, text: str, timestamp=None) -> None:
        timestamp = timestamp or datetime.now(UTC).replace(tzinfo=None)
        self._warnings.append(Event(text, timestamp))

    def operation(self, name: str, timestamp=None) -> OperationTracker:
        op = self._get_operation(name)
        if not op:
            op = OperationTracker(name, timestamp)
            self._operations.append(op)

        return op

    def _get_operation(self, name: str) -> Optional[OperationTracker]:
        return next((op for op in self._operations if op.name == name), None)

    def result(self, result: str) -> None:
        self._result = result

    def to_status(self) -> Status:
        return Status(self._last_event, [op.to_operation() for op in self._operations],
                      self._warnings, self._result)


class TrackedEnvironment(ABC):

    @property
    @abstractmethod
    def status_tracker(self) -> StatusTracker:
        pass


class MonitoredEnvironment(TrackedEnvironment, OutputEnvironment, ABC):
    pass
