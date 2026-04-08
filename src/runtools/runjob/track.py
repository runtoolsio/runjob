from datetime import datetime, UTC
from typing import Optional, List, Callable

from runtools.runcore import util
from runtools.runcore.output import OutputLine
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
    """Process output lines with ``rt_``-prefixed tracking fields.

    Recognized fields (all prefixed with ``rt_`` in both ``extra`` dict and KV output):
        ``rt_operation`` — identifies a tracked operation.
        ``rt_event`` — standalone status message.
        ``rt_completed`` — progress count (prefix with ``+`` for increment).
        ``rt_total`` — target count.
        ``rt_unit`` — unit label.
        ``rt_result`` — completion message (finishes operation or sets global result).
        ``rt_failed`` — failure reason (finishes operation as failed).
        ``rt_scope`` — qualifies operation with a target identifier.
        ``rt_timestamp`` — ISO datetime override.
    """
    if not output_line.fields:
        return

    fields = output_line.fields
    timestamp = _parse_timestamp(fields.get('rt_timestamp'))

    completed_raw = str(fields['rt_completed']) if 'rt_completed' in fields else None
    completed_increment = False
    if completed_raw is not None and completed_raw.startswith('+'):
        completed_increment = True
        completed_raw = completed_raw[1:]
    completed = convert_if_number(completed_raw)

    total = convert_if_number(fields.get('rt_total'))

    result = fields.get('rt_result')
    scope = fields.get('rt_scope')

    source = output_line.source

    if op_name := fields.get('rt_operation'):
        op = tracker.operation(op_name, timestamp, source=source, scope=scope)
        if completed_increment and completed is not None:
            completed = (op.completed or 0) + completed
        if completed is not None or total is not None or fields.get('rt_unit') is not None:
            op.update(completed, total, fields.get('rt_unit'), timestamp)
        if fail_reason := fields.get('rt_failed'):
            op.finish(fail_reason, timestamp, failed=True)
        elif result:
            op.finish(result, timestamp)
    elif event := fields.get('rt_event'):
        tracker.event(event, timestamp, source=source)

    if result and not fields.get('rt_operation'):
        tracker.result(result, timestamp, source=source)


class StatusTracker:
    """Output processor that extracts tracking fields and maintains status state.

    As a processor in the output chain, returns None for tracking-only lines (empty message + rt_ fields)
    to prevent them from reaching storage and output observers.
    """

    def __init__(self, output_handler: OutputHandler = None, on_change: Callable[[], None] = None):
        self._output_handler = output_handler or field_based_handler
        self._last_event: Optional[Event] = None
        self._operations: List[OperationTracker] = []
        self._warnings: List[Event] = []
        self._result: Optional[Event] = None
        self._on_change = on_change

    def __call__(self, output_line: OutputLine) -> Optional[OutputLine]:
        self._output_handler(output_line, self)
        return None if output_line.is_tracking_only else output_line

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
