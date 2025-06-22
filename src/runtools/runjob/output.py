import logging
from abc import ABC, abstractmethod
from collections import deque
from threading import local
from typing import Optional, Callable, List, Iterable

from runtools.runcore.output import OutputLine, OutputObserver, TailBuffer, Mode, OutputLineFactory
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

        if mode == Mode.TAIL:
            return output[-max_lines:]

        if mode == Mode.HEAD:
            return output[:max_lines]
