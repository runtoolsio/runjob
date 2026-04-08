"""Output capture for Python logging.

Provides ``StdLogOutputLink``, the default output link that bridges Python's stdlib logging
to an OutputSink. It attaches a handler to the root logger for the duration of a job run,
extracting ``rt_``-prefixed fields from LogRecord extras for status tracking.

Custom output links implement ``__call__(self, sink, *, capture_filter) -> ContextManager``.
"""

import logging
from contextlib import contextmanager
from typing import Callable, ContextManager, Optional, Protocol

from runtools.runjob.output import OutputSink


class OutputLink(Protocol):
    """Protocol for output capture callables that bridge external sources to an OutputSink."""

    def __call__(self, sink: OutputSink, *, capture_filter: Callable[[], bool]) -> ContextManager: ...


class StdLogOutputLink:
    """Captures Python stdlib logging records and feeds them to an OutputSink.

    Args:
        formatter: If provided, format records with this formatter. If None, use record.getMessage().
    """

    def __init__(self, formatter: logging.Formatter | None = None):
        self._formatter = formatter

    def __call__(self, sink: OutputSink, *, capture_filter: Callable):
        @contextmanager
        def cm():
            handler = _SinkForwardingHandler(sink, capture_filter, self._formatter)
            root_logger = logging.getLogger()
            root_logger.addHandler(handler)
            try:
                yield
            finally:
                root_logger.removeHandler(handler)
        return cm()


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
        message = self.format(record) if self._use_formatter else record.getMessage()
        is_error = record.levelno >= logging.ERROR
        fields = self._extract_tracking_fields(record)
        self._sink.new_output(message, is_error, fields)

    @staticmethod
    def _extract_tracking_fields(record) -> Optional[dict]:
        """Extract rt_-prefixed tracking fields from LogRecord extras."""
        extras = {k: v for k, v in record.__dict__.items() if k.startswith("rt_")}
        return extras if extras else None
