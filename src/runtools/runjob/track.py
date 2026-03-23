from datetime import datetime, UTC
from typing import Optional, List, Callable

from runtools.runcore import util
from runtools.runcore.output import OutputLine, OutputObserver
from runtools.runcore.status import Event, Operation, Status
from runtools.runcore.util import convert_if_number

OutputHandler = Callable[[OutputLine, 'StatusTracker'], None]


class OperationTracker:

    def __init__(self, name: str, created_at: datetime = None, on_update: Callable[[], None] = None,
                 source: str | None = None, scope: str | None = None):
        self.name = name
        self.scope = scope
        self.completed = None
        self.total = None
        self.unit = None
        self.created_at = created_at or datetime.now(UTC).replace(tzinfo=None)
        self.updated_at = self.created_at
        self.result: Optional[str] = None
        self.failed: bool = False
        self.source = source
        self._on_update = on_update

    def update(self,
               completed: Optional[float] = None,
               total: Optional[float] = None,
               unit: Optional[str] = None,
               updated_at: Optional[datetime] = None) -> None:
        if completed is not None:
            self.completed = completed
        if total is not None:
            self.total = total
        if unit is not None:
            self.unit = unit
        self.updated_at = updated_at or datetime.now(UTC).replace(tzinfo=None)
        if self._on_update:
            self._on_update()

    def finish(self, result: str, timestamp: Optional[datetime] = None, failed: bool = False) -> None:
        self.result = result
        self.failed = failed
        self.updated_at = timestamp or datetime.now(UTC).replace(tzinfo=None)
        if self._on_update:
            self._on_update()

    @property
    def is_finished(self):
        return self.result is not None or (
                self.total is not None and
                self.completed is not None and
                self.completed >= self.total
        )

    def to_operation(self) -> Operation:
        return Operation(
            name=self.name,
            completed=self.completed,
            total=self.total,
            unit=self.unit,
            created_at=self.created_at,
            updated_at=self.updated_at,
            result=self.result,
            failed=self.failed,
            source=self.source,
            scope=self.scope,
        )


def ts_or_now(timestamp):
    return timestamp or datetime.now(UTC).replace(tzinfo=None)


def _parse_timestamp(value):
    if value is None or isinstance(value, datetime):
        return value
    return util.parse_datetime(value)


def field_based_handler(output_line: OutputLine, tracker: 'StatusTracker') -> None:
    """Process output lines that have structured fields.

    Expects field names: event, operation, completed, total, unit, result, failed, timestamp, scope.
    ``operation`` identifies a tracked operation. ``event`` is a standalone status message.
    ``result`` with ``operation`` finishes that operation; ``result`` alone sets the global result.
    ``failed`` with ``operation`` finishes that operation as failed (result=failed value, failed=True).
    ``scope`` qualifies an operation with a target identifier (e.g., a site key, region, or tenant).
    Scoped operations share a name but are tracked independently; they are excluded from the status summary.
    """
    if not output_line.fields:
        return

    fields = output_line.fields
    timestamp = _parse_timestamp(fields.get('timestamp'))

    completed_raw = str(fields['completed']) if 'completed' in fields else None
    completed_increment = False
    if completed_raw is not None and completed_raw.startswith('+'):
        completed_increment = True
        completed_raw = completed_raw[1:]
    completed = convert_if_number(completed_raw)

    total = convert_if_number(fields.get('total'))
    if total == 0:
        total = None  # Zero total means caller doesn't know the total

    result = fields.get('result')
    scope = fields.get('scope')

    source = output_line.source

    if op_name := fields.get('operation'):
        op = tracker.operation(op_name, timestamp, source=source, scope=scope)
        if completed_increment and completed is not None:
            completed = (op.completed or 0) + completed
        if completed is not None or total is not None or fields.get('unit') is not None:
            op.update(completed, total, fields.get('unit'), timestamp)
        if fail_reason := fields.get('failed'):
            op.finish(fail_reason, timestamp, failed=True)
        elif result:
            op.finish(result, timestamp)
    elif event := fields.get('event'):
        tracker.event(event, timestamp, source=source)

    if result and not fields.get('operation'):
        tracker.result(result, timestamp, source=source)


class StatusTracker(OutputObserver):

    def __init__(self, output_handler: OutputHandler = None, on_change: Callable[[], None] = None):
        self._output_handler = output_handler or field_based_handler
        self._last_event: Optional[Event] = None
        self._operations: List[OperationTracker] = []
        self._warnings: List[Event] = []
        self._result: Optional[Event] = None
        self._on_change = on_change

    def new_output(self, output_line: OutputLine):
        self._output_handler(output_line, self)

    def event(self, text: str, timestamp=None, source: str | None = None) -> None:
        timestamp = ts_or_now(timestamp)
        self._last_event = Event(text, timestamp, source)
        if self._on_change:
            self._on_change()

    def warning(self, text: str, timestamp=None, source: str | None = None) -> None:
        timestamp = ts_or_now(timestamp)
        self._warnings.append(Event(text, timestamp, source))
        if self._on_change:
            self._on_change()

    def operation(self, name: str, timestamp=None, source: str | None = None,
                  scope: str | None = None) -> OperationTracker:
        op = self._get_operation(name, scope)
        if not op:
            op = OperationTracker(name, timestamp, on_update=self._on_change, source=source, scope=scope)
            self._operations.append(op)

        return op

    def _get_operation(self, name: str, scope: str | None = None) -> Optional[OperationTracker]:
        return next((op for op in self._operations if op.name == name and op.scope == scope), None)

    def result(self, result: str, timestamp=None, source: str | None = None) -> None:
        timestamp = ts_or_now(timestamp)
        self._result = Event(result, timestamp, source)
        if self._on_change:
            self._on_change()

    def to_status(self) -> Status:
        return Status(self._last_event, [op.to_operation() for op in self._operations],
                      self._warnings, self._result)
