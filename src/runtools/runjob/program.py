"""
This module contains the `ProgramExecution` class, an implementation of the `Execution` abstract class, used to
run an external program using the `subprocess` module from the standard library.
"""

import io
import logging
import signal
from subprocess import Popen, PIPE
from threading import Thread
from typing import Union, Optional

import sys

from runtools.runcore.run import FailedRun, TerminateRun, TerminationStatus
from runtools.runjob.phaser import ExecutingPhase

USE_SHELL = False  # For testing only

log = logging.getLogger(__name__)


class ProgramPhase(ExecutingPhase):

    def __init__(self, phase_id, *args, read_output: bool = True):
        super().__init__(phase_id)
        self.args = args
        self.read_output: bool = read_output
        self._popen: Union[Popen, None] = None
        self._status = None
        self._stopped: bool = False
        self._interrupted: bool = False

    @property
    def ret_code(self) -> Optional[int]:
        if self._popen is None:
            return None

        return self._popen.returncode

    def run(self, run_ctx):
        if not self._stopped and not self._interrupted:
            stdout = PIPE if self.read_output else None
            stderr = PIPE if self.read_output else None
            try:
                self._popen = Popen(" ".join(self.args) if USE_SHELL else self.args, stdout=stdout, stderr=stderr,
                                    shell=USE_SHELL)
                stdout_reader = None
                stderr_reader = None
                if self.read_output:
                    stdout_reader = self._start_output_reader(run_ctx, self._popen.stdout, False)
                    stderr_reader = self._start_output_reader(run_ctx, self._popen.stderr, True)

                self._popen.wait()
                if self.read_output:
                    stdout_reader.join(timeout=1)
                    stderr_reader.join(timeout=1)
                if self.ret_code == 0:
                    return
            except FileNotFoundError as e:
                sys.stderr.write(str(e) + "\n")
                """TODO Move exception level up"""
                raise FailedRun('CommandNotFound', str(e)) from e

        if self._interrupted or self.ret_code == -signal.SIGINT:
            raise TerminateRun(TerminationStatus.INTERRUPTED)
        if self._stopped or self.ret_code < 0:  # Negative exit code means terminated by a signal
            raise TerminateRun(TerminationStatus.STOPPED)
        raise FailedRun('ProgramError', "Process returned non-zero code " + str(self.ret_code))

    def _start_output_reader(self, run_ctx, infile, is_err):
        name = 'Stderr-Reader' if is_err else 'Stdout-Reader'
        t = Thread(target=self._process_output, args=(run_ctx, infile, is_err), name=name, daemon=True)
        t.start()
        return t

    @property
    def parameters(self):
        return ('execution', 'program'),

    def stop(self):
        self._stopped = True
        if self._popen:
            self._popen.terminate()

    def interrupted(self):
        """
        Call this if the execution was possibly interrupted externally (Ctrl+C) to set the correct final state.
        On windows might be needed to send a signal?
        """
        self._interrupted = True

    def _process_output(self, run_ctx, infile, is_err):
        with infile:
            for line in io.TextIOWrapper(infile, encoding="utf-8"):
                line_stripped = line.rstrip()
                self._status = line_stripped
                print(line_stripped, file=sys.stderr if is_err else sys.stdout)
                run_ctx.new_output(line_stripped, is_err)
