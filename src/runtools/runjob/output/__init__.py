"""
Output processing, routing, and storage for job execution.

Key Components:
    OutputSink: Receives output lines, runs processors, dispatches to observers.
    OutputRouter: Routes to tail buffer + disk/batch storages.
    OutputStore: ABC extending OutputBackend with write + retention.
    OutputParser: Smart output parser (JSON → text log → KV → plain).

Factory Functions:
    load_output_store_module: Dynamically loads a write-side backend module.
    create_stores: Creates all enabled stores from storage configs.

Store Module Contract:
    Each store module (e.g., ``runtools.runjob.output.file``) must expose:
        create_store(env_id, config) -> OutputStore
"""

import importlib
import json
import logging
import re
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import replace
from datetime import datetime
from threading import Lock, local
from typing import Optional, Callable, List, Iterable

from runtools.runcore.err import RuntoolsException
from runtools.runcore.job import InstanceID
from runtools.runcore.output import (OutputLine, OutputObserver, TailBuffer, Mode, OutputLineFactory, Output,
                                     TailNotSupportedError, OutputBackend, parse_timestamp)
from runtools.runcore.retention import RetentionPolicy
from runtools.runcore.util.observer import ObservableNotification, DEFAULT_OBSERVER_PRIORITY, ObserverContext
from runtools.runjob.phase import _current_phase

log = logging.getLogger(__name__)

_thread_local = local()


OutputProcessor = Callable[[OutputLine], Optional[OutputLine]]


class ParsingProcessor:
    """Processor that parses output text into structured fields using provided parsers."""

    def __init__(self, parsers):
        self.parsers = parsers

    def __call__(self, output_line: OutputLine) -> Optional[OutputLine]:
        if output_line.fields:
            return output_line

        fields = {}
        for parser in self.parsers:
            if parsed := parser(output_line.message):
                fields.update(parsed)

        if not fields:
            return output_line

        return output_line.with_fields(fields)


# --- Smart output parsing (JSON → text log pattern → KV → plain) ---

# Matches key=value, key=[bracketed], key=(parens), key=<angle> tokens
_KV_TOKEN_PATTERN = re.compile(r'\S+=(?:\[[^\]]*\]|\([^)]*\)|<[^>]*>|\S+)')

_ENVELOPE_KEY_MAP = {
    "timestamp": "timestamp", "ts": "timestamp", "time": "timestamp",
    "level": "level", "lvl": "level", "severity": "level",
    "logger": "logger", "logger_name": "logger", "loggername": "logger",
    "thread": "thread", "thread_name": "thread", "threadname": "thread",
}

_MESSAGE_KEYS = frozenset({"message", "msg"})

# --- Token extractors for text log line parsing ---


class TokenExtractor:
    """Base class for extractors that pull a single field from the front of a string."""

    def extract(self, text: str) -> tuple[str, str, str] | None:
        """Try to extract a token from the start of text.

        Returns:
            (key, value, remainder) if matched, None otherwise.
        """
        return None


class TimestampExtractor(TokenExtractor):
    """Extracts a timestamp from the start: full date+time or time-only."""

    _FULL = re.compile(
        r'^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,6})?(?:Z|[+-]\d{2}:?\d{2})?)\s*')
    _TIME_ONLY = re.compile(r'^(\d{2}:\d{2}:\d{2}(?:[.,]\d{1,3})?)\s*')

    def extract(self, text):
        m = self._FULL.match(text)
        if m:
            return 'timestamp', m.group(1), text[m.end():]
        m = self._TIME_ONLY.match(text)
        if m:
            return 'timestamp', m.group(1), text[m.end():]
        return None


class LevelExtractor(TokenExtractor):
    """Extracts a log level keyword (INFO, ERROR, etc.)."""

    _PATTERN = re.compile(
        r'^(TRACE|DEBUG|INFO|NOTICE|WARN(?:ING)?|ERROR|SEVERE|FATAL|CRITICAL)\b\s*', re.ASCII)

    def extract(self, text):
        m = self._PATTERN.match(text)
        return ('level', m.group(1), text[m.end():]) if m else None


class BracketExtractor(TokenExtractor):
    """Extracts a bracketed token: [main], [c.t.Logger], [pool-3-thread-7]."""

    _PATTERN = re.compile(r'^\[([^\]]+)\]\s*')

    def extract(self, text):
        m = self._PATTERN.match(text)
        return ('bracket', m.group(1), text[m.end():]) if m else None


class DottedIdentifierExtractor(TokenExtractor):
    """Extracts a dotted identifier (logger name): com.example.App, c.t.CLI."""

    _PATTERN = re.compile(r'^([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)+)\s*', re.ASCII)

    def extract(self, text):
        m = self._PATTERN.match(text)
        return ('logger', m.group(1), text[m.end():]) if m else None


class SeparatorExtractor(TokenExtractor):
    """Strips leading separators (-, --, :) before the message."""

    _PATTERN = re.compile(r'^[-:]+\s*')

    def extract(self, text):
        m = self._PATTERN.match(text)
        return ('_separator', '', text[m.end():]) if m else None


class PythonLogExtractor(TokenExtractor):
    """Extracts Python default log format: LEVEL:logger:message."""

    _PATTERN = re.compile(r'^(DEBUG|INFO|WARNING|ERROR|CRITICAL):([^:]+):(.*)', re.ASCII)

    def extract(self, text):
        m = self._PATTERN.match(text)
        if m:
            return '_python', f'{m.group(1)}:{m.group(2)}', m.group(3).strip()
        return None


# Heuristic for bracket resolution
_THREAD_HINT = re.compile(r'(?:thread|pool|main|worker|executor|Timer|ForkJoin)', re.IGNORECASE)


class TextEnvelopeParser:
    """Extracts envelope fields (timestamp, level, logger, thread) from text log lines.

    Runs a chain of token extractors against the front of the string, collecting
    recognized fields. Bracketed tokens are resolved to logger/thread via heuristics.
    """

    def __init__(self):
        self._timestamp = TimestampExtractor()
        self._python = PythonLogExtractor()
        # Extractors tried in a loop after timestamp (order matters: brackets and level can interleave)
        self._mid_extractors = [BracketExtractor(), LevelExtractor()]
        self._dotted = DottedIdentifierExtractor()
        self._separator = SeparatorExtractor()

    def parse(self, text: str) -> tuple[dict, str] | None:
        """Extract envelope from text. Returns (envelope_dict, remaining_message) or None."""
        # Python default format: LEVEL:logger:message
        result = self._python.extract(text)
        if result:
            _, combined, message = result
            level, logger = combined.split(':', 1)
            return {'level': level, 'logger': logger}, message

        # Timestamp is required for structured log detection
        result = self._timestamp.extract(text)
        if not result:
            return None
        envelope = {'timestamp': result[1]}
        remaining = result[2]

        # Collect brackets and level in whatever order they appear
        brackets = []
        for _ in range(3):
            matched = False
            for ext in self._mid_extractors:
                result = ext.extract(remaining)
                if result:
                    key, value, remaining = result
                    if key == 'bracket':
                        brackets.append(value)
                    else:
                        envelope[key] = value
                    matched = True
                    break
            if not matched:
                break

        # Dotted identifier after level → logger
        if 'level' in envelope:
            result = self._dotted.extract(remaining)
            if result:
                envelope['logger'] = result[1]
                remaining = result[2]

        # Resolve brackets to logger/thread
        self._resolve_brackets(envelope, brackets)

        # Strip separator before message
        result = self._separator.extract(remaining)
        if result:
            remaining = result[2]

        return envelope, remaining.strip()

    @staticmethod
    def _resolve_brackets(envelope: dict, brackets: list[str]) -> None:
        """Assign bracketed tokens to logger/thread using heuristics."""
        if not brackets:
            return

        if len(brackets) == 1:
            token = brackets[0]
            if 'level' not in envelope:
                if _THREAD_HINT.search(token):
                    envelope['thread'] = token
                else:
                    envelope['logger'] = token
            else:
                if '.' in token or token[0].isupper():
                    envelope['logger'] = token
                else:
                    envelope['thread'] = token
        elif len(brackets) == 2:
            first, second = brackets
            if _THREAD_HINT.search(first) and not _THREAD_HINT.search(second):
                envelope['thread'] = first
                envelope['logger'] = second
            elif _THREAD_HINT.search(second) and not _THREAD_HINT.search(first):
                envelope['logger'] = first
                envelope['thread'] = second
            else:
                envelope['logger'] = first
                envelope['thread'] = second
        else:
            envelope['logger'] = brackets[0]
            envelope['thread'] = brackets[1]


_text_envelope_parser = TextEnvelopeParser()


class OutputParser:
    """Smart output parser: JSON → text log pattern → KV → plain.

    Extracts envelope fields (timestamp, level, logger, thread) into OutputLine attributes
    and application data into fields. Skips lines already parsed (e.g. from StdLogOutputLink).
    """

    def __init__(self, kv_parser=None):
        self._kv_parser = kv_parser

    def __call__(self, output_line: OutputLine) -> Optional[OutputLine]:
        if output_line.timestamp or output_line.level or output_line.fields:
            return output_line

        text = output_line.message

        # 1. JSON line
        result = self._try_json(text)
        if result is not None:
            return self._apply(output_line, result)

        # 2. Text log pattern → extract envelope, then KV on cleaned message
        result = self._try_text_pattern(text)
        if result is not None:
            output_line = self._apply(output_line, result)
            text = output_line.message

        # 3. KV on (possibly cleaned) message — strip matched pairs to produce clean text
        if self._kv_parser:
            fields = self._kv_parser(text)
            if fields:
                existing = output_line.fields or {}
                cleaned = _KV_TOKEN_PATTERN.sub('', text).strip()
                return replace(output_line, message=cleaned, fields={**existing, **fields})

        return output_line

    @staticmethod
    def _try_json(text: str) -> dict | None:
        stripped = text.strip()
        if not stripped.startswith('{'):
            return None
        try:
            data = json.loads(stripped)
        except (json.JSONDecodeError, ValueError):
            return None
        if not isinstance(data, dict):
            return None

        # Encoder metadata fields that are noise when null/empty
        _ENCODER_NOISE = frozenset({'throwable', 'throwableproxy', 'formattedmessage', 'arguments'})

        envelope, fields, message = {}, {}, None
        for key, value in data.items():
            lower = key.lower()
            if lower in _MESSAGE_KEYS:
                message = "" if value is None or value == "null" else str(value)
            elif lower in _ENVELOPE_KEY_MAP:
                envelope[_ENVELOPE_KEY_MAP[lower]] = value
            elif lower == 'kvplist' and isinstance(value, list):
                for entry in value:
                    if isinstance(entry, dict):
                        fields.update(entry)
            elif lower == 'mdc' and isinstance(value, dict):
                if value:
                    fields.update(value)
            elif lower in _ENCODER_NOISE and not value:
                continue
            else:
                fields[key] = value

        result = {'envelope': envelope, 'fields': fields or None}
        if message is not None:
            result['message'] = message
        return result

    @staticmethod
    def _try_text_pattern(text: str) -> dict | None:
        result = _text_envelope_parser.parse(text)
        if result is None:
            return None
        envelope, message = result
        return {'message': message, 'envelope': envelope}

    @staticmethod
    def _apply(output_line: OutputLine, parsed: dict) -> OutputLine:
        kwargs = {}
        if 'message' in parsed:
            kwargs['message'] = parsed['message']
        for field in ('timestamp', 'level', 'logger', 'thread'):
            if value := parsed.get('envelope', {}).get(field):
                kwargs[field] = parse_timestamp(value) if field == 'timestamp' else value
        if parsed.get('fields'):
            kwargs['fields'] = parsed['fields']
        return replace(output_line, **kwargs) if kwargs else output_line


class OutputSink:
    """Receives output lines, runs them through a processor chain, and dispatches to observers.

    Processors are called in order. If any returns None, the line is dropped and observers are not notified.
    """

    def __init__(self, processors: Iterable[OutputProcessor] = ()):
        self._processors: tuple[OutputProcessor, ...] = tuple(processors)
        self._output_notification = ObservableNotification[OutputObserver]()
        self._line_factory = OutputLineFactory()

    def new_output(self, message: str, is_error: bool = False,
                   source: str | None = None, timestamp: str | datetime | None = None,
                   level: str | None = None, logger: str | None = None,
                   thread: str | None = None, fields: dict | None = None):
        if getattr(_thread_local, 'processing_output', False):
            return
        _thread_local.processing_output = True

        try:
            if source is None:
                phase = _current_phase.get(None)
                source = phase.id if phase else None
            output_line = self._line_factory(message, is_error, source, timestamp, level, logger, thread, fields)

            for processor in self._processors:
                output_line = processor(output_line)
                if output_line is None:
                    return
            self._output_notification.observer_proxy.new_output(output_line)
        finally:
            _thread_local.processing_output = False

    def add_observer(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY) -> None:
        self._output_notification.add_observer(observer, priority)

    def remove_observer(self, observer) -> None:
        self._output_notification.remove_observer(observer)

    def observer_context(self, *observers, priority: int = DEFAULT_OBSERVER_PRIORITY) -> ObserverContext[
        OutputObserver]:
        return self._output_notification.observer_context(*observers, priority=priority)


class OutputContext(ABC):

    @property
    @abstractmethod
    def output_sink(self) -> OutputSink:
        pass


class InMemoryTailBuffer(TailBuffer):

    def __init__(self, max_bytes: int):
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self._max_bytes = max_bytes
        self._lock = Lock()
        self._lines: deque[OutputLine] = deque()
        self._current_bytes = 0

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = Lock()

    @staticmethod
    def _estimate_size(line: OutputLine) -> int:
        """Estimate memory footprint of a single output line. Dominated by the message string."""
        size = len(line.message.encode())
        if line.source:
            size += len(line.source.encode())
        return size

    def add_line(self, output_line: OutputLine):
        line_size = self._estimate_size(output_line)
        with self._lock:
            self._lines.append(output_line)
            self._current_bytes += line_size

            while self._current_bytes > self._max_bytes and len(self._lines) > 1:
                evicted = self._lines.popleft()
                self._current_bytes -= self._estimate_size(evicted)

    def get_lines(self, mode: Mode = Mode.TAIL, max_lines: int = 0) -> List[OutputLine]:
        if max_lines < 0:
            raise ValueError("Count cannot be negative")

        with self._lock:
            output = list(self._lines)
        if not max_lines:
            return output

        match mode:
            case Mode.TAIL:
                return output[-max_lines:]
            case Mode.HEAD:
                return output[:max_lines]
            case _:
                assert False, f"Unhandled mode: {mode}"


class OutputWriter(ABC):

    @property
    @abstractmethod
    def location(self):
        pass

    @abstractmethod
    def store_line(self, line: OutputLine):
        """Store a single output line. Optional for flush-only storages."""
        pass

    def store_lines(self, lines: List[OutputLine]):
        """Optional bulk insert method. Default: loop over store_line."""
        for line in lines:
            self.store_line(line)

    @property
    def batch_size(self) -> Optional[int]:
        """Return preferred batch size, if any."""
        return None

    def close(self):
        pass


class OutputRouter(OutputObserver, Output):
    """Routes OutputLine instances to multiple storages and an optional tail buffer."""

    def __init__(self, *, tail_buffer=None, storages=(), max_batch: int = 100):
        super().__init__()
        self.tail_buffer = tail_buffer
        self.storages = list(storages)
        self.realtime_storages: List[OutputWriter] = [s for s in self.storages if not s.batch_size]
        self.batch_storages: List[OutputWriter] = [s for s in self.storages if s.batch_size]
        self.max_batch = max_batch
        self._batch_lock = Lock()
        self._batch_buffer: List[OutputLine] = []

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_batch_lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._batch_lock = Lock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    @property
    def locations(self):
        return [storage.location for storage in self.storages]

    def new_output(self, output_line: OutputLine):
        if self.tail_buffer:
            self.tail_buffer.add_line(output_line)

        for storage in self.realtime_storages:
            storage.store_line(output_line)

        if self.batch_storages:
            with self._batch_lock:
                self._batch_buffer.append(output_line)
            if (
                    len(self._batch_buffer) >= self.max_batch
                    or any(len(self._batch_buffer) >= s.batch_size for s in self.batch_storages)
            ):
                self._flush_batch_buffer()

    def _flush_batch_buffer(self):
        with self._batch_lock:
            lines_to_flush, self._batch_buffer = self._batch_buffer, []
        if not lines_to_flush:
            return
        for storage in self.batch_storages:
            batch_sz = storage.batch_size or len(lines_to_flush)
            for i in range(0, len(lines_to_flush), batch_sz):
                chunk = lines_to_flush[i: i + batch_sz]
                storage.store_lines(chunk)

    def tail(self, mode: Mode = Mode.TAIL, max_lines: int = 0):
        if not self.tail_buffer:
            raise TailNotSupportedError
        return self.tail_buffer.get_lines(mode, max_lines)

    def close(self):
        if self._batch_buffer:
            self._flush_batch_buffer()
        for storage in self.storages:
            storage.close()


class OutputStore(OutputBackend, ABC):
    """Extends OutputBackend with write and retention capabilities."""

    @abstractmethod
    def create_writer(self, instance_id: InstanceID, *, created_at: datetime) -> OutputWriter:
        """Create a per-instance writer for storing output lines.

        Args:
            instance_id: Fully-qualified InstanceID of the run being written.
            created_at: Canonical creation timestamp of the run (the same value
                threaded into RunStorage.init_run, derived from
                root_phase.created_at). Backends that record retention metadata
                use this so DB-, lifecycle-, and store-side timestamps stay
                consistent.
        """

    @abstractmethod
    def enforce_retention(self, job_id: str, policy: RetentionPolicy):
        """Prune old output for a job according to retention policy."""


# ---------------------------------------------------------------------------
# Store module loader (write-side, mirrors runcore's read-side loader)
# ---------------------------------------------------------------------------

class OutputStoreNotFoundError(RuntoolsException):

    def __init__(self, backend_type):
        super().__init__(f'Cannot find output store module for type {backend_type!r}. '
                         f'Ensure the module is installed.')


_store_modules = {}


def load_output_store_module(backend_type: str):
    """Load a write-side output backend module by type name.

    Imports ``runtools.runjob.output.<backend_type>`` directly. The module must
    expose a ``create_store(env_id, config)`` factory function.
    """
    module = _store_modules.get(backend_type)
    if module:
        return module
    module_name = f"runtools.runjob.output.{backend_type}"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as e:
        if e.name == module_name:
            raise OutputStoreNotFoundError(backend_type)
        raise
    _store_modules[backend_type] = module
    return module


def create_stores(env_id, storage_configs) -> list[OutputStore]:
    """Create all enabled output stores from storage configurations, in config order."""
    stores: list[OutputStore] = []
    for cfg in storage_configs:
        if not cfg.enabled:
            continue
        module = load_output_store_module(cfg.type)
        stores.append(module.create_store(env_id, cfg))
    return stores
