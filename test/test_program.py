"""Stopping a ProgramPhase: outside a terminal's foreground the whole process group is stopped, not just the
program."""
import os
import pty
import subprocess
import sys
import time

import pytest

from runtools.runcore.run import TerminationStatus
from runtools.runjob import node, program
from runtools.runjob.program import ProgramPhase


def _run(env, phase):
    inst = env.create_instance('prog_job', root_phase=phase)
    inst.run(in_background=True)
    _wait_until(lambda: phase._popen is not None)
    return inst


def _wait_until(condition, timeout=5):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise TimeoutError("Condition not met")
        time.sleep(0.05)


def _group_alive(pgid):
    return subprocess.run(['pgrep', '-g', str(pgid)], capture_output=True).returncode == 0


@pytest.fixture
def not_in_terminal_foreground(monkeypatch):
    monkeypatch.setattr(program, '_in_terminal_foreground', lambda: False)


def test_stop_terminates_the_programs_children(not_in_terminal_foreground):
    phase = ProgramPhase('work', 'sh', '-c', 'sleep 300 & wait')
    with node.in_process(transient=True) as env:
        inst = _run(env, phase)
        time.sleep(0.3)  # let sh start its child

        inst.stop()

        _wait_until(lambda: inst.snap().lifecycle.is_ended)
        assert inst.snap().lifecycle.termination.status == TerminationStatus.STOPPED
        assert not _group_alive(phase._popen.pid)


def test_stop_works_after_the_program_exited_while_a_child_holds_its_output(not_in_terminal_foreground):
    phase = ProgramPhase('work', 'sh', '-c', 'sleep 300 &')  # sh exits at once; sleep keeps the pipes open
    with node.in_process(transient=True) as env:
        inst = _run(env, phase)
        _wait_until(lambda: phase._popen.poll() is not None)
        assert not inst.snap().lifecycle.is_ended  # still waiting for the child's output to close

        inst.stop()

        _wait_until(lambda: inst.snap().lifecycle.is_ended)
        assert not _group_alive(phase._popen.pid)


def test_in_a_terminals_foreground_the_program_stays_in_the_callers_process_group(monkeypatch):
    monkeypatch.setattr(program, '_in_terminal_foreground', lambda: True)
    phase = ProgramPhase('work', 'sleep', '300')
    with node.in_process(transient=True) as env:
        inst = _run(env, phase)

        assert os.getpgid(phase._popen.pid) == os.getpgrp()  # Ctrl+C from the terminal still reaches it
        inst.stop()

        _wait_until(lambda: inst.snap().lifecycle.is_ended)
        assert inst.snap().lifecycle.termination.status == TerminationStatus.STOPPED


_DETECT = 'from runtools.runjob import program; print(program._in_terminal_foreground(), flush=True)'
_ATTACH_AND_DETECT = """
import fcntl, os, subprocess, sys, termios
tty = os.open(sys.argv[1], os.O_RDWR)
fcntl.ioctl(tty, termios.TIOCSCTTY, 0)  # This process leads a new session: make the pty its controlling terminal
if sys.argv[2] == 'background':  # A process in another group of the same session — like `run job … &`
    subprocess.run([sys.executable, '-c', sys.argv[3]], process_group=0)
else:
    subprocess.run([sys.executable, '-c', sys.argv[3]])
"""


def _detect_in_terminal(*, background):
    """Run the detection in fresh processes attached to a real (pseudo) terminal, in its foreground or background."""
    master, slave = pty.openpty()
    try:
        out = subprocess.run(
            [sys.executable, '-c', _ATTACH_AND_DETECT, os.ttyname(slave), 'background' if background else 'foreground',
             _DETECT],
            capture_output=True, text=True, start_new_session=True, timeout=30)
        return out.stdout.strip()
    finally:
        os.close(slave)
        os.close(master)


def test_detects_the_terminal_foreground():
    assert _detect_in_terminal(background=False) == 'True'


def test_detects_a_background_job_in_a_terminal():
    assert _detect_in_terminal(background=True) == 'False'


def test_detects_no_terminal():
    out = subprocess.run([sys.executable, '-c', _DETECT], capture_output=True, text=True, start_new_session=True)
    assert out.stdout.strip() == 'False'  # A new session has no controlling terminal, as under cron or systemd
