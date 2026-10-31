"""
This module contains the `ProgramPhase` class, a phase implementation that runs an external program using the
`subprocess` module from the standard library.
"""

import io
import logging
import os
import signal
from subprocess import Popen, PIPE
from threading import Lock, Thread
from typing import Optional

import sys
from runtools.runcore.run import TerminationStatus
from runtools.runjob.output import OutputContext
from runtools.runjob.phase import BasePhase, PhaseTerminated

USE_SHELL = False  # For testing only

log = logging.getLogger(__name__)


def _in_terminal_foreground() -> bool:
    """Whether this process runs in the foreground of its controlling terminal — the only place where Ctrl+C and
    terminal input reach its process group."""
    try:
        tty = os.open('/dev/tty', os.O_RDWR)
    except OSError:
        return False  # No controlling terminal: cron, systemd, setsid
    try:
        return os.tcgetpgrp(tty) == os.getpgrp()  # Not equal: started in the background (`&`, `nohup … &`)
    except OSError:
        return False
    finally:
        os.close(tty)


class ProgramPhase(BasePhase[OutputContext]):
    TYPE = 'PROGRAM'

    def __init__(self, phase_id, *args, read_output: bool = True):
        super().__init__(phase_id, ProgramPhase.TYPE)
        self.args = args
        self.read_output: bool = read_output
        self._stop_lock = Lock()
        self._popen: Optional[Popen] = None
        self._own_group = False
        self._status = None

    @property
    def ret_code(self) -> Optional[int]:
        if self._popen is None:
            return None

        return self._popen.returncode

    def _run(self, run_ctx):
        with self._stop_lock:
            self._raise_if_stopped()
            stdout = PIPE if self.read_output else None
            stderr = PIPE if self.read_output else None
            # Unless in a terminal's foreground, the program gets its own process group, so a stop reaches every
            # process it started — a surviving child would keep the output pipes open and the run from ending. In
            # the foreground it stays in the caller's group, where Ctrl+C and terminal input reach it.
            self._own_group = not _in_terminal_foreground()
            try:
                self._popen = Popen( " ".join(self.args) if USE_SHELL else self.args, stdout=stdout, stderr=stderr,
                                     shell=USE_SHELL, process_group=0 if self._own_group else None)
            except OSError as e:
                sys.stderr.write(str(e) + "\n")
                # TODO: Move exception level up
                raise PhaseTerminated(TerminationStatus.FAILED, str(e)) from e

        stdout_reader = None
        stderr_reader = None
        if self.read_output:
            stdout_reader = self._start_output_reader(run_ctx, self._popen.stdout, False)
            stderr_reader = self._start_output_reader(run_ctx, self._popen.stderr, True)

        self._popen.wait()
        if self.read_output:
            stdout_reader.join()
            stderr_reader.join()
        if self.ret_code == 0:
            return

        if self.ret_code == -signal.SIGINT:
            raise PhaseTerminated(TerminationStatus.INTERRUPTED)
        self._raise_if_stopped()
        if self.ret_code < 0:  # Negative exit code means terminated by a signal
            raise PhaseTerminated(TerminationStatus.STOPPED)
        raise PhaseTerminated(
            TerminationStatus.FAILED, f"Program returned non-zero exit code: {self.ret_code}")

    def _start_output_reader(self, run_ctx, infile, is_err):
        name = 'Stderr-Reader' if is_err else 'Stdout-Reader'
        t = Thread(target=self._process_output, args=(run_ctx, infile, is_err), name=name, daemon=True)
        t.start()
        return t

    @property
    def attributes(self):
        return {'args': self.args, 'output_read': self.read_output}

    def _stop_running(self, reason):
        with self._stop_lock:
            if not self._popen:
                return
            if self._own_group:
                try:
                    os.killpg(self._popen.pid, signal.SIGTERM)  # Also after the program itself has exited
                except ProcessLookupError:
                    pass  # Nothing left to stop
            else:
                self._popen.terminate()

    def _process_output(self, run_ctx, infile, is_err):
        observer_error_logged = False
        with infile:
            for line in io.TextIOWrapper(infile, encoding="utf-8", errors="replace"):
                line_stripped = line.rstrip()
                self._status = line_stripped
                print(line_stripped, file=sys.stderr if is_err else sys.stdout)
                try:
                    run_ctx.output_pipeline.new_output(line_stripped, is_err, source=self.id)
                except Exception:
                    if not observer_error_logged:
                        log.exception("Output observer error", extra={"line": line_stripped[:200]})
                        observer_error_logged = True
