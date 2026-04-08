"""Output capture for Python logging.

Provides ``log_capture``, the default output link that bridges Python's stdlib logging
to an OutputSink. It attaches a handler to the root logger for the duration of a job run,
extracting ``rt_``-prefixed fields from LogRecord extras for status tracking.

Custom output links follow the same callable contract: ``(sink, *, capture_filter) -> ContextManager``.
Configurable via ``functools.partial``: ``partial(log_capture, formatter=logging.Formatter(...))``.
"""

import logging
from contextlib import contextmanager
from typing import Callable, Optional

from runtools.runjob.output import OutputSink


def log_capture(sink: OutputSink, *, capture_filter: Callable, formatter: logging.Formatter | None = None):
    """Capture Python logging records and feed them to an OutputSink.

    Args:
        sink: The output sink to feed captured lines into.
        capture_filter: Zero-arg predicate; True if the current record belongs to this instance.
        formatter: If provided, format records with this formatter. If None, use record.getMessage().
    """
    @contextmanager
    def cm():
        handler = _SinkForwardingHandler(sink, capture_filter, formatter)
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
