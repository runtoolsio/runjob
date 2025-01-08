"""
This module contains the `ProcessExecution` class, an implementation of the `Execution` abstract class, used to
execute code in a separate process using the `multiprocessing` package from the standard library.
"""

import logging
import signal
import traceback
from contextlib import contextmanager
from multiprocessing import Queue
from multiprocessing.context import Process
from queue import Full, Empty
from typing import Union, Tuple, Optional

import sys

from runtools.runcore.output import OutputLine
from runtools.runcore.run import TerminateRun, TerminationStatus, FailedRun, Fault
from runtools.runjob.output import OutputContext
from runtools.runjob.phaser import ExecutingPhase

log = logging.getLogger(__name__)

NON_ZERO_RETURN_CODE = "NON_ZERO_RETURN_CODE"

class ProcessPhase(ExecutingPhase[OutputContext]):

    def __init__(self, phase_id: str, target, args=(), *, output_id = None):
        super().__init__(phase_id)
        self.target = target
        self.args = args
        self.output_id = output_id
        self.output_queue: Queue[Tuple[Union[str, _QueueStop], bool]] = Queue(maxsize=2048)
        self._process: Optional[Process] = None
        self._stopped: bool = False
        self._interrupted: bool = False

    def run(self, ctx):
        if not self._stopped and not self._interrupted:
            self._process = Process(target=self._run)

            try:
                self._process.start()
                self._read_output(ctx.output_sink)
                self._process.join(timeout=2)  # Just in case as it should be completed at this point
            finally:
                self.output_queue.close()

            if self._process.exitcode == 0:
                    return

            if self._interrupted or self._process.exitcode == -signal.SIGINT:
                # Exit code is -SIGINT only when SIGINT handler is set back to DFL (KeyboardInterrupt gets exit code 1)
                raise TerminateRun(TerminationStatus.INTERRUPTED)
            if self._stopped or self._process.exitcode < 0:
                raise TerminateRun(TerminationStatus.STOPPED)

            raise FailedRun(Fault(NON_ZERO_RETURN_CODE, f"Process returned non-zero code {self._process.exitcode}"))

    def _run(self):
        with self._capture_stdout():
            try:
                self.target(*self.args)
            except:
                for line in traceback.format_exception(*sys.exc_info()):
                    self.output_queue.put_nowait((line, True))
                raise
            finally:
                self.output_queue.put_nowait((_QueueStop(), False))

    @contextmanager
    def _capture_stdout(self):
        import sys
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        stdout_writer = _CapturingWriter(original_stdout, False, self.output_queue)
        stderr_writer = _CapturingWriter(original_stderr, True, self.output_queue)
        sys.stdout = stdout_writer
        sys.stderr = stderr_writer

        try:
            yield
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr

    @property
    def parameters(self):
        return ('execution', 'process'),

    def stop(self):
        self._stopped = True
        self.output_queue.put_nowait((_QueueStop(), False))
        if self._process:
            self._process.terminate()

    def interrupted(self):
        self._interrupted = True

    def _read_output(self, output_sink):
        while self._process.is_alive():
            try:
                output_text, is_err = self.output_queue.get(timeout=2)
                if isinstance(output_text, _QueueStop):
                    break
                output_sink.new_output(OutputLine(output_text, is_err, self.output_id))
            except Empty:
                pass


class _CapturingWriter:

    def __init__(self, out, is_err, output_queue):
        self.out = out
        self.is_err = is_err
        self.output_queue = output_queue

    def write(self, text):
        text_s = text.rstrip()
        if text_s:
            try:
                self.output_queue.put_nowait((text_s, self.is_err))
            except Full:
                # TODO what to do here?
                log.warning("event=[output_queue_full]")
        self.out.write(text)


class _QueueStop:
    """Poison object signalizing no more objects will be put in the queue"""
    pass
