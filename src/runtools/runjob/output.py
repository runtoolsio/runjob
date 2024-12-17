import logging
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional, Callable, List, Iterable

from runtools.runcore.output import OutputLine, OutputObserver, TailBuffer, Mode
from runtools.runcore.util.observer import ObservableNotification, DEFAULT_OBSERVER_PRIORITY, ObserverContext


class LogForwarding:

    def __init__(self, logger, target_handlers: Iterable[logging.Handler]):
        self.logger = logger
        self.target_handlers = target_handlers

    def __enter__(self):
        for handler in self.target_handlers:
            self.logger.addHandler(handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for handler in self.target_handlers:
            self.logger.removeHandler(handler)


class OutputSink(ABC):

    def __init__(self):
        self.preprocessing: Optional[Callable[[OutputLine], OutputLine]] = None
        self._output_notification = ObservableNotification[OutputObserver]()

    @abstractmethod
    def _process_output(self, output_line):
        pass

    def new_output(self, output_line):
        if self.preprocessing:
            output_line = self.preprocessing(output_line)

        self._process_output(output_line)
        self._output_notification.observer_proxy.new_output(output_line)

    def observer(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY) -> ObserverContext[OutputObserver]:
        return self._output_notification.observer(observer, priority)

    def forward_logs_handler(self, format_record=True):
        """
        Creates and returns a logging.Handler instance that forwards log records to this sink.
        """

        class InternalHandler(logging.Handler):
            def __init__(self, sink):
                super().__init__()
                self.sink = sink

            def emit(self, record):
                output = self.format(record) if format_record else record.getMessage()
                is_error = record.levelno >= logging.ERROR
                self.sink.new_output(output, is_error)

        return InternalHandler(self)

    def forward_logs(self, logger, format_record=True):
        return LogForwarding(logger, [self.forward_logs_handler(format_record)])


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


class OutputEnvironment(ABC):

    @property
    @abstractmethod
    def output_sink(self):
        pass
