"""Stopping a ProgramPhase: without a terminal the whole process group is stopped, not just the program."""
import os
import subprocess
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
def no_terminal(monkeypatch):
    monkeypatch.setattr(program, '_has_controlling_terminal', lambda: False)


def test_stop_terminates_the_programs_children(no_terminal):
    phase = ProgramPhase('work', 'sh', '-c', 'sleep 300 & wait')
    with node.in_process(transient=True) as env:
        inst = _run(env, phase)
        time.sleep(0.3)  # let sh start its child

        inst.stop()

        _wait_until(lambda: inst.snap().lifecycle.is_ended)
        assert inst.snap().lifecycle.termination.status == TerminationStatus.STOPPED
        assert not _group_alive(phase._popen.pid)


def test_stop_works_after_the_program_exited_while_a_child_holds_its_output(no_terminal):
    phase = ProgramPhase('work', 'sh', '-c', 'sleep 300 &')  # sh exits at once; sleep keeps the pipes open
    with node.in_process(transient=True) as env:
        inst = _run(env, phase)
        _wait_until(lambda: phase._popen.poll() is not None)
        assert not inst.snap().lifecycle.is_ended  # still waiting for the child's output to close

        inst.stop()

        _wait_until(lambda: inst.snap().lifecycle.is_ended)
        assert not _group_alive(phase._popen.pid)


def test_with_a_terminal_the_program_stays_in_the_callers_process_group(monkeypatch):
    monkeypatch.setattr(program, '_has_controlling_terminal', lambda: True)
    phase = ProgramPhase('work', 'sleep', '300')
    with node.in_process(transient=True) as env:
        inst = _run(env, phase)

        assert os.getpgid(phase._popen.pid) == os.getpgrp()  # Ctrl+C from the terminal still reaches it
        inst.stop()

        _wait_until(lambda: inst.snap().lifecycle.is_ended)
        assert inst.snap().lifecycle.termination.status == TerminationStatus.STOPPED
