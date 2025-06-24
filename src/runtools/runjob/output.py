import logging
import os
from abc import ABC, abstractmethod
from collections import deque
from threading import local
from typing import Optional, Callable, List, Iterable

from runtools.runcore.output import OutputLine, OutputObserver, TailBuffer, Mode, OutputLineFactory, Output, \
    TailNotSupportedError, OutputLocation
from runtools.runcore.util.observer import ObservableNotification, DEFAULT_OBSERVER_PRIORITY, ObserverContext

_thread_local = local()


class LogHandlerContext:

    def __init__(self, loggers: Iterable[logging.Logger], temp_handlers: Iterable[logging.Handler]):
        self.loggers = loggers
        self.temp_handlers = temp_handlers

    def __enter__(self):
        for logger in self.loggers:
            for handler in self.temp_handlers:
                logger.addHandler(handler)

    def __exit__(self, exc_type, exc_val, exc_tb):
        for logger in self.loggers:
            for handler in self.temp_handlers:
                logger.removeHandler(handler)


class OutputSink(ABC):

    def __init__(self):
        self.preprocessing: Optional[Callable[[OutputLine], OutputLine]] = None
        self._output_notification = ObservableNotification[OutputObserver]()

    @abstractmethod
    def _process_output(self, output_line):
        pass

    def new_output(self, output_line):
        if getattr(_thread_local, 'processing_output', False):
            return
        _thread_local.processing_output = True

        try:
            if self.preprocessing:
                output_line = self.preprocessing(output_line)

            self._process_output(output_line)
            self._output_notification.observer_proxy.new_output(output_line)
        finally:
            _thread_local.processing_output = False

    def add_observer(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY) -> None:
        self._output_notification.add_observer(observer, priority)

    def remove_observer(self, observer) -> None:
        self._output_notification.remove_observer(observer)

    def observer_context(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY) -> ObserverContext[OutputObserver]:
        return self._output_notification.observer_context(observer, priority)

    def capturing_log_handler(self, log_filter: Optional[logging.Filter] = None, *, format_record=True):
        """
        Creates and returns a logging.Handler instance that forwards log records to this sink.
        TODO source
        """

        class InternalHandler(logging.Handler):
            def __init__(self, sink):
                super().__init__()
                self.sink = sink
                self._output_line_fact = OutputLineFactory()

            def emit(self, record):
                output = self.format(record) if format_record else record.getMessage()
                is_error = record.levelno >= logging.ERROR
                self.sink.new_output(self._output_line_fact(output, is_error))

        handler = InternalHandler(self)
        if log_filter:
            handler.addFilter(log_filter)
        return handler

    def capture_logs_from(self, *loggers, log_filter=None, format_record=True) -> LogHandlerContext:
        handler = self.capturing_log_handler(log_filter=log_filter, format_record=format_record)
        return LogHandlerContext(loggers, [handler])


class OutputContext(ABC):

    @property
    @abstractmethod
    def output_sink(self) -> OutputSink:
        pass


class InMemoryTailBuffer(TailBuffer):

    def __init__(self, max_capacity: int = 0):
        if max_capacity < 0:
            raise ValueError("max_capacity cannot be negative")
        self._max_capacity = max_capacity or None
        self._lines = deque(maxlen=self._max_capacity)

    def add_line(self, output_line: OutputLine):
        self._lines.append(output_line)

    def get_lines(self, mode: Mode = Mode.TAIL, max_lines: int = 0) -> List[OutputLine]:
        if max_lines < 0:
            raise ValueError("Count cannot be negative")

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


class OutputStorage(ABC):

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


class FileOutputStorage(OutputStorage):
    def __init__(self, file_path: str, append: bool = True, encoding: str = "utf-8"):
        self.file_path = file_path
        self._mode = "a" if append else "w"
        self._encoding = encoding

        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        self._file = open(file_path, self._mode, encoding=encoding)

    @property
    def location(self):
        return OutputLocation(type="file", source=self.file_path)

    def store_line(self, line: OutputLine):
        formatted = self._format_line(line)
        self._file.write(formatted + "\n")
        self._file.flush()  # or buffer and batch flush if needed

    def store_lines(self, lines: List[OutputLine]):
        for line in lines:
            self._file.write(self._format_line(line) + "\n")
        self._file.flush()

    def _format_line(self, line: OutputLine) -> str:
        return line.text

    def close(self):
        self._file.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class OutputRouter(Output, OutputSink):
    """
    Routes OutputLine instances to multiple storages and an optional tail buffer,
    supporting both immediate and batched writes.
    """

    def __init__(self, *, tail_buffer=None, storages=(), max_batch: int = 100):
        super().__init__()
        self.tail_buffer = tail_buffer
        self.storages = list(storages)
        self.realtime_storages: List[OutputStorage] = [s for s in self.storages if not s.batch_size]
        self.batch_storages: List[OutputStorage] = [s for s in self.storages if s.batch_size]
        self.max_batch = max_batch
        self._batch_buffer: List[OutputLine] = []
        self._locations = [storage.location for storage in self.storages]

    @property
    def locations(self):
        return self._locations

    def _process_output(self, output_line: OutputLine):
        # 1) tail buffering
        if self.tail_buffer:
            self.tail_buffer.add_line(output_line)

        # 2) immediate stores
        for storage in self.realtime_storages:
            storage.store_line(output_line)

        # 3) buffer for batch stores
        if self.batch_storages:
            self._batch_buffer.append(output_line)
            if (
                    len(self._batch_buffer) >= self.max_batch
                    or any(len(self._batch_buffer) >= s.batch_size for s in self.batch_storages)
            ):
                self._flush_batch_buffer()

    def _flush_batch_buffer(self):
        lines_to_flush, self._batch_buffer = self._batch_buffer, []
        for storage in self.batch_storages:
            batch_sz = storage.batch_size or len(lines_to_flush)
            for i in range(0, len(lines_to_flush), batch_sz):
                chunk = lines_to_flush[i: i + batch_sz]
                storage.store_lines(chunk)

    def tail(self, mode: Mode = Mode.TAIL, max_lines: int = 0):
        if not self.tail_buffer:
            raise TailNotSupportedError
        return self.tail_buffer.get_lines(mode, max_lines)
