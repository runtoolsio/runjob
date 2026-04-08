"""Output capture for Python logging.

Provides ``StdLogOutputLink``, the default output link that bridges Python's stdlib logging
to an OutputSink. It attaches a handler to the root logger for the duration of a job run,
extracting ``rt_``-prefixed fields from LogRecord extras for status tracking.

Custom output links implement ``__call__(self, sink, *, capture_filter) -> ContextManager``.
"""

import logging
from contextlib import contextmanager
from typing import Callable, ContextManager, Optional, Protocol

from runtools.runcore.output import OutputLine
from runtools.runjob.output import OutputSink


class OutputLink(Protocol):
    """Protocol for output capture callables that bridge external sources to an OutputSink."""

    def __call__(self, sink: OutputSink, *, capture_filter: Callable[[], bool]) -> ContextManager: ...


class StdLogOutputLink:
    """Captures Python stdlib logging records and feeds them to an OutputSink.

    Args:
        formatter: Formatter for verbose rendering. Defaults to DEFAULT_FORMATTER.
            Pass None to disable formatting (verbose shows raw message only).
    """

    DEFAULT_FORMATTER = logging.Formatter(
        "%(asctime)s %(levelname)-8s [%(name)s] [%(threadName)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    def __init__(self, formatter: logging.Formatter | None = DEFAULT_FORMATTER):
        self._formatter = formatter

    def __call__(self, sink: OutputSink, *, capture_filter: Callable):
        @contextmanager
        def cm():
            root_logger = logging.getLogger()
            # Suppress tracking-only records from existing handlers (console, file, etc.)
            tracking_filter = _TrackingOnlyFilter()
            existing_handlers = list(root_logger.handlers)
            for h in existing_handlers:
                h.addFilter(tracking_filter)
            # Add capture handler (without the filter — it sees everything)
            handler = _SinkForwardingHandler(sink, capture_filter, self._formatter)
            root_logger.addHandler(handler)
            try:
                yield
            finally:
                root_logger.removeHandler(handler)
                for h in existing_handlers:
                    h.removeFilter(tracking_filter)
        return cm()


class _TrackingOnlyFilter(logging.Filter):
    """Rejects log records that are tracking-only (empty message + rt_ fields)."""

    def filter(self, record):
        if not record.getMessage().strip():
            return not any(k.startswith("rt_") for k in record.__dict__)
        return True


class _SinkForwardingHandler(logging.Handler):
    """Forwards log records to an OutputSink, extracting rt_ tracking fields."""

    def __init__(self, sink: OutputSink, capture_filter: Callable, formatter: logging.Formatter | None):
        super().__init__()
        self._sink = sink
        self._capture_filter = capture_filter
        if formatter:
            self.setFormatter(formatter)
        self._use_formatter = formatter is not None

    def emit(self, record):
        if not self._capture_filter():
            return
        message = record.getMessage()
        is_error = record.levelno >= logging.ERROR
        fields = self._extract_tracking_fields(record)
        formatted = self._build_formatted_template(record, message) if self._use_formatter else None
        self._sink.new_output(message, is_error, fields, formatted=formatted)

    def _build_formatted_template(self, record, message: str) -> str:
        formatted = self.format(record)
        if message and message in formatted:
            return formatted.replace(message, OutputLine.MESSAGE_TOKEN, 1)
        return formatted

    @staticmethod
    def _extract_tracking_fields(record) -> Optional[dict]:
        """Extract rt_-prefixed tracking fields from LogRecord extras."""
        extras = {k: v for k, v in record.__dict__.items() if k.startswith("rt_")}
        return extras if extras else None
