import json
import logging
import os
import re
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import replace
from pathlib import Path
from threading import Lock, local
from typing import Optional, Callable, List, Iterable

from runtools.runcore import paths
from runtools.runcore.job import InstanceID
from runtools.runcore.output import (OutputLine, OutputObserver, TailBuffer, Mode, OutputLineFactory, Output,
                                     TailNotSupportedError, OutputLocation, OutputBackend, FileOutputBackend,
                                     FileOutputStorageConfig, SourceIndex, SourceIndexBuilder)
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

_ENVELOPE_KEY_MAP = {
    "timestamp": "timestamp", "ts": "timestamp", "time": "timestamp",
    "level": "level", "lvl": "level", "severity": "level",
    "logger": "logger", "logger_name": "logger",
    "thread": "thread", "thread_name": "thread",
}

_MESSAGE_KEYS = frozenset({"message", "msg"})

# Pattern A: ISO timestamp + level + optional logger
_LOG_LINE_PATTERN = re.compile(
    r'^(?P<timestamp>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,6})?(?:Z|[+-]\d{2}:?\d{2})?)'
    r'\s+'
    r'(?P<level>TRACE|DEBUG|INFO|NOTICE|WARN(?:ING)?|ERROR|SEVERE|FATAL|CRITICAL)'
    r'(?:'
        r'\s+\[(?P<logger_bracket>[^\]]+)\]'
        r'|'
        r'\s+(?P<logger_dotted>[a-zA-Z_][\w.]*\.[a-zA-Z_][\w.]*)'
    r')?'
    r'\s*[-:]?\s*'
    r'(?P<message>.*)',
    re.ASCII,
)

# Pattern B: Python default format LEVEL:logger:message
_PYTHON_LOG_PATTERN = re.compile(
    r'^(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL):(?P<logger>[^:]+):(?P<message>.*)'
)


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

        # 3. KV on (possibly cleaned) message
        if self._kv_parser:
            fields = self._kv_parser(text)
            if fields:
                existing = output_line.fields or {}
                return output_line.with_fields({**existing, **fields})

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

        envelope, fields, message = {}, {}, None
        for key, value in data.items():
            lower = key.lower()
            if lower in _MESSAGE_KEYS:
                message = str(value)
            elif lower in _ENVELOPE_KEY_MAP:
                envelope[_ENVELOPE_KEY_MAP[lower]] = str(value) if value is not None else None
            else:
                fields[key] = value

        result = {'envelope': envelope, 'fields': fields or None}
        if message is not None:
            result['message'] = message
        return result

    @staticmethod
    def _try_text_pattern(text: str) -> dict | None:
        m = _LOG_LINE_PATTERN.match(text)
        if m:
            logger = m.group('logger_bracket') or m.group('logger_dotted')
            envelope = {'timestamp': m.group('timestamp'), 'level': m.group('level')}
            if logger:
                envelope['logger'] = logger
            return {'message': m.group('message').strip(), 'envelope': envelope}

        m = _PYTHON_LOG_PATTERN.match(text)
        if m:
            return {
                'message': m.group('message').strip(),
                'envelope': {'level': m.group('level'), 'logger': m.group('logger')},
            }
        return None

    @staticmethod
    def _apply(output_line: OutputLine, parsed: dict) -> OutputLine:
        kwargs = {}
        if 'message' in parsed:
            kwargs['message'] = parsed['message']
        for field in ('timestamp', 'level', 'logger', 'thread'):
            if value := parsed.get('envelope', {}).get(field):
                kwargs[field] = value
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
                   source: str | None = None, timestamp: str | None = None,
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
                assert False, f"Unhandled mode: {mode}"  # Should never happen


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


class FileOutputWriter(OutputWriter):
    # TODO Add file size capping (max_bytes) — same semantics as TailBuffer but on disk.
    #  This enables file output as the default (currently opt-in via --log) without risking
    #  disk exhaustion from long-running jobs.

    def __init__(self, file_path: str, append: bool = False, encoding: str = "utf-8"):
        self.file_path = Path(file_path)
        self._encoding = encoding
        self._closed = False
        self._index_builder = SourceIndexBuilder()

        os.makedirs(self.file_path.parent, exist_ok=True)
        self._file = open(self.file_path, "ab" if append else "wb")

    @property
    def location(self):
        return OutputLocation.for_file(self.file_path)

    def store_line(self, line: OutputLine):
        self._write_line(line)
        self._file.flush()

    def store_lines(self, lines: List[OutputLine]):
        for line in lines:
            self._write_line(line)
        self._file.flush()

    def _write_line(self, line: OutputLine):
        raw = (json.dumps(line.serialize(), ensure_ascii=False) + "\n").encode(self._encoding)
        self._index_builder.track(line.source, len(raw))
        self._file.write(raw)

    def close(self):
        if self._closed:
            return
        try:
            if index := self._index_builder.build():
                index.save(self.file_path)
        finally:
            self._closed = True
            self._file.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class OutputRouter(OutputObserver, Output):
    """
    Routes OutputLine instances to multiple storages and an optional tail buffer,
    supporting both immediate and batched writes.
    """

    def __init__(self, *, tail_buffer=None, storages=(), max_batch: int = 100):
        super().__init__()
        self.tail_buffer = tail_buffer
        self.storages = list(storages)
        self.realtime_storages: List[OutputWriter] = [s for s in self.storages if not s.batch_size]
        self.batch_storages: List[OutputWriter] = [s for s in self.storages if s.batch_size]
        self.max_batch = max_batch
        self._batch_lock = Lock()
        self._batch_buffer: List[OutputLine] = []
        self._locations = [storage.location for storage in self.storages]

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
        return False  # Don't suppress exceptions

    @property
    def locations(self):
        return self._locations

    def new_output(self, output_line: OutputLine):
        # 1) tail buffering
        if self.tail_buffer:
            self.tail_buffer.add_line(output_line)

        # 2) immediate stores
        for storage in self.realtime_storages:
            storage.store_line(output_line)

        # 3) buffer for batch stores
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
        # Flush any remaining batched data
        if self._batch_buffer:
            self._flush_batch_buffer()

        # Close all storages
        for storage in self.storages:
            storage.close()


class OutputStore(OutputBackend, ABC):
    """Extends OutputBackend with write and retention capabilities."""

    @abstractmethod
    def create_writer(self, instance_id: InstanceID) -> OutputWriter:
        """Create a per-instance writer for storing output lines."""

    @abstractmethod
    def enforce_retention(self, job_id: str, policy: RetentionPolicy):
        """Prune old output for a job according to retention policy."""


def create_stores(env_id, storage_configs) -> list['FileOutputStore']:
    """Create output stores from storage configuration."""
    stores: list[FileOutputStore] = []
    for cfg in storage_configs:
        if not cfg.enabled:
            continue
        if isinstance(cfg, FileOutputStorageConfig):
            base_dir = Path(cfg.dir).expanduser() if cfg.dir else paths.output_dir(env_id, create=True)
            stores.append(FileOutputStore(base_dir))
        else:
            assert False, f"Unknown output storage config type: {cfg.type}"
    return stores


class FileOutputStore(FileOutputBackend, OutputStore):
    """File-backed output store. Inherits read from FileOutputBackend, adds write + retention."""

    def __init__(self, base_dir: Path):
        super().__init__(base_dir)

    def create_writer(self, instance_id: InstanceID) -> FileOutputWriter:
        path = self._output_path(instance_id)
        os.makedirs(path.parent, exist_ok=True)
        return FileOutputWriter(str(path))

    def enforce_retention(self, job_id: str, policy: RetentionPolicy):
        job_dir = self._base_dir / job_id
        if not job_dir.is_dir():
            return

        files = sorted(job_dir.glob("*.jsonl"), key=lambda f: f.stat().st_mtime, reverse=True)
        if policy.max_runs_per_job >= 0:
            for f in files[policy.max_runs_per_job:]:
                try:
                    f.unlink()
                    SourceIndex.path_for(f).unlink(missing_ok=True)
                except OSError:
                    log.warning("Failed to delete output file: %s", f, exc_info=True)
