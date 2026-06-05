from datetime import datetime, UTC
from typing import Optional, List, Callable

from runtools.runcore import util
from runtools.runcore.output import OutputLine
from runtools.runcore.status import Event, Operation, Result, Status
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
    """Process output lines with ``runtools.track.``-prefixed tracking fields.

    Recognized fields (all prefixed with ``runtools.track.`` in both ``extra`` dict and KV output):
        ``runtools.track.operation`` — identifies a tracked operation.
        ``runtools.track.event`` — standalone status message.
        ``runtools.track.completed`` — progress count (prefix with ``+`` for increment).
        ``runtools.track.total`` — target count.
        ``runtools.track.unit`` — unit label.
        ``runtools.track.result`` — completion message (finishes operation as ok, or sets the run's
            result as ok when no operation key is present).
        ``runtools.track.failed`` — failure reason (finishes operation as failed, or sets the run's
            result as failed when no operation key is present).
        ``runtools.track.scope`` — qualifies operation with a target identifier.
        ``runtools.track.timestamp`` — ISO datetime override.
    """
    if not output_line.fields:
        return

    fields = output_line.fields
    timestamp = _parse_timestamp(fields.get('runtools.track.timestamp'))

    completed_raw = str(fields['runtools.track.completed']) if 'runtools.track.completed' in fields else None
    completed_increment = False
    if completed_raw is not None and completed_raw.startswith('+'):
        completed_increment = True
        completed_raw = completed_raw[1:]
    completed = convert_if_number(completed_raw)

    total = convert_if_number(fields.get('runtools.track.total'))

    result = fields.get('runtools.track.result')
    scope = fields.get('runtools.track.scope')

    source = output_line.source

    if op_name := fields.get('runtools.track.operation'):
        op = tracker.operation(op_name, timestamp, source=source, scope=scope)
        if completed_increment and completed is not None:
            completed = (op.completed or 0) + completed
        if completed is not None or total is not None or fields.get('runtools.track.unit') is not None:
            op.update(completed, total, fields.get('runtools.track.unit'), timestamp)
        if result_failed := fields.get('runtools.track.failed'):
            op.finish(result_failed, timestamp, failed=True)
        elif result:
            op.finish(result, timestamp)
    elif event := fields.get('runtools.track.event'):
        tracker.event(event, timestamp, source=source)

    # Job-level result: `runtools.track.result` sets the run's outcome (ok);
    # `runtools.track.failed` without an operation sets the run's outcome (failed).
    if not fields.get('runtools.track.operation'):
        if result_failed := fields.get('runtools.track.failed'):
            tracker.result(result_failed, timestamp, source=source, failed=True)
        elif result:
            tracker.result(result, timestamp, source=source)


class StatusTracker:
    """Output processor that extracts tracking fields and maintains status state.

    As a processor in the output chain, returns None for tracking-only lines (empty message + runtools.track.* fields)
    to prevent them from reaching storage and output observers.
    """

    def __init__(self, output_handler: OutputHandler = None, on_change: Callable[[], None] = None):
        self._output_handler = output_handler or field_based_handler
        self._last_event: Optional[Event] = None
        self._operations: List[OperationTracker] = []
        self._warnings: List[Event] = []
        self._result: Optional[Result] = None
        self._on_change = on_change

    def __call__(self, output_line: OutputLine) -> Optional[OutputLine]:
        self._output_handler(output_line, self)
        if output_line.is_tracking_only:
            return None
        return output_line.without_tracking_fields()

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

    def result(self, message: str, timestamp=None, source: str | None = None, failed: bool = False) -> None:
        timestamp = ts_or_now(timestamp)
        self._result = Result(message, timestamp, source, failed=failed)
        if self._on_change:
            self._on_change()

    def to_status(self) -> Status:
        return Status(self._last_event, [op.to_operation() for op in self._operations],
                      self._warnings, self._result)
