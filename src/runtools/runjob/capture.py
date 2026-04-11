"""Output capture for Python logging.

Provides ``StdLogOutputLink``, the default output link that bridges Python's stdlib logging
to an OutputSink. It attaches a handler to the root logger for the duration of a job run,
extracting ``rt_``-prefixed fields from LogRecord extras for status tracking.

Custom output links implement ``__call__(self, sink, *, capture_filter) -> ContextManager``.
"""

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, ContextManager, Optional, Protocol

from runtools.runjob.output import OutputSink


class OutputLink(Protocol):
    """Protocol for output capture callables that bridge external sources to an OutputSink."""

    def __call__(self, sink: OutputSink, *, capture_filter: Callable[[], bool]) -> ContextManager: ...


class StdLogOutputLink:
    """Captures Python stdlib logging records and feeds them to an OutputSink.

    Extracts canonical envelope fields (timestamp, level, logger) from LogRecord.
    Extracts ``rt_``-prefixed tracking fields and user extras into fields dict.
    """

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
            handler = _SinkForwardingHandler(sink, capture_filter)
            root_logger.addHandler(handler)
            try:
                yield
            finally:
                root_logger.removeHandler(handler)
                for h in existing_handlers:
                    h.removeFilter(tracking_filter)
        return cm()


def _has_rt_keys(d) -> bool:
    return any(k.startswith("rt_") for k in d)


def _dict_message(msg_dict: dict) -> str:
    msg = msg_dict.get("message")
    if msg is None:
        msg = msg_dict.get("msg")
    return str(msg) if msg is not None else ""


class _TrackingOnlyFilter(logging.Filter):
    """Rejects log records that are tracking-only (empty message + rt_ fields)."""

    def filter(self, record):
        if isinstance(record.msg, dict):
            if not _dict_message(record.msg).strip():
                return not _has_rt_keys(record.msg) and not _has_rt_keys(record.__dict__)
        elif not record.getMessage().strip():
            return not _has_rt_keys(record.__dict__)
        return True


_BUILTIN_ATTRS = frozenset({
    'name', 'msg', 'args', 'created', 'filename', 'funcName', 'levelname',
    'levelno', 'lineno', 'module', 'msecs', 'pathname', 'process',
    'processName', 'relativeCreated', 'stack_info', 'exc_info', 'exc_text',
    'thread', 'threadName', 'taskName', 'message',
})

_DICT_MESSAGE_KEYS = frozenset({"message", "msg"})


class _SinkForwardingHandler(logging.Handler):
    """Forwards log records to an OutputSink, extracting structured fields."""

    def __init__(self, sink: OutputSink, capture_filter: Callable):
        super().__init__()
        self._sink = sink
        self._capture_filter = capture_filter

    def emit(self, record):
        if not self._capture_filter():
            return
        is_error = record.levelno >= logging.ERROR
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec='milliseconds')
        timestamp = ts[:-6] + 'Z' if ts.endswith('+00:00') else ts
        level = record.levelname
        logger_name = record.name
        thread_name = record.threadName

        if isinstance(record.msg, dict):
            message, fields = self._extract_dict_message(record)
        else:
            message = record.getMessage()
            fields = self._extract_extra_fields(record)

        self._sink.new_output(message, is_error,
                              timestamp=timestamp, level=level, logger=logger_name, thread=thread_name, fields=fields)

    @staticmethod
    def _extract_dict_message(record) -> tuple[str, Optional[dict]]:
        """Extract message and fields from a dict-style log record.

        Supports python-json-logger pattern: ``logger.info({"message": "...", "key": "val"})``.
        Recognizes ``message``/``msg`` as the output message; everything else becomes fields.
        """
        msg_dict = record.msg
        message = _dict_message(msg_dict)
        fields = {k: v for k, v in msg_dict.items() if k not in _DICT_MESSAGE_KEYS}
        # Merge extra= kwargs from the logging call
        for k, v in record.__dict__.items():
            if k not in _BUILTIN_ATTRS and not k.startswith('_'):
                fields[k] = v
        return message, fields or None

    @staticmethod
    def _extract_extra_fields(record) -> Optional[dict]:
        """Extract rt_ tracking fields and user extras from LogRecord."""
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in _BUILTIN_ATTRS and not k.startswith('_')
        }
        return extras if extras else None
