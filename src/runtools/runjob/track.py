import re
from datetime import datetime, UTC
from typing import Optional, List, Callable

from runtools.runcore import util
from runtools.runcore.output import OutputLine, OutputObserver
from runtools.runcore.status import Event, Operation, Status
from runtools.runcore.util import convert_if_number

OutputHandler = Callable[[OutputLine, 'StatusTracker'], None]


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
    def is_finished(self):
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


def ts_or_now(timestamp):
    return timestamp or datetime.now(UTC).replace(tzinfo=None)


def _parse_timestamp(value):
    if value is None or isinstance(value, datetime):
        return value
    return util.parse_datetime(value)


def field_based_handler(output_line: OutputLine, tracker: 'StatusTracker') -> None:
    """Process output lines that have structured fields.

    Expects field names: event, completed, total, unit, result, timestamp, operation.
    """
    if not output_line.fields:
        return

    fields = output_line.fields
    timestamp = _parse_timestamp(fields.get('timestamp'))
    completed = convert_if_number(fields.get('completed'))
    total = convert_if_number(fields.get('total'))

    if any(v is not None for v in (completed, total, fields.get('unit'))):
        op_name = fields.get('operation') or fields.get('event')
        if op_name:
            op = tracker.operation(op_name, timestamp)
            op.update(completed, total, fields.get('unit'), timestamp)
    elif event := fields.get('event'):
        tracker.event(event, timestamp)

    if result := fields.get('result'):
        tracker.result(result, timestamp)


def message_as_event(output_line: OutputLine, tracker: 'StatusTracker') -> None:
    """Treat every output message as an event."""
    if output_line.message:
        tracker.event(output_line.message)


def combined_output_handler(output_line: OutputLine, tracker: 'StatusTracker') -> None:
    """Combined handler: use fields if present, otherwise treat message as event."""
    if output_line.fields:
        field_based_handler(output_line, tracker)
    elif output_line.message:
        tracker.event(output_line.message)


class StatusTracker(OutputObserver):

    def __init__(self, output_handler: OutputHandler = None):
        self._output_handler = output_handler or combined_output_handler
        self._last_event: Optional[Event] = None
        self._operations: List[OperationTracker] = []
        self._warnings: List[Event] = []
        self._result: Optional[Event] = None

    def new_output(self, output_line: OutputLine):
        self._output_handler(output_line, self)

    def event(self, text: str, timestamp=None) -> None:
        timestamp = ts_or_now(timestamp)
        self._last_event = Event(text, timestamp)
        for op in self._operations:
            if op.is_finished:
                op.is_active = False

    def warning(self, text: str, timestamp=None) -> None:
        timestamp = ts_or_now(timestamp)
        self._warnings.append(Event(text, timestamp))

    def operation(self, name: str, timestamp=None) -> OperationTracker:
        op = self._get_operation(name)
        if not op:
            op = OperationTracker(name, timestamp)
            self._operations.append(op)

        return op

    def _get_operation(self, name: str) -> Optional[OperationTracker]:
        return next((op for op in self._operations if op.name == name), None)

    def result(self, result: str, timestamp=None) -> None:
        timestamp = ts_or_now(timestamp)
        self._result = Event(result, timestamp)
        for op in self._operations:
            op.is_active = False

    def to_status(self) -> Status:
        return Status(self._last_event, [op.to_operation() for op in self._operations],
                      self._warnings, self._result)
