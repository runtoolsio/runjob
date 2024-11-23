"""
Tests :mod:`process` module
"""
from multiprocessing import Pipe
from threading import Thread

import pytest
from time import sleep

from runtools.runcore.run import FailedRun, TerminateRun, TerminationStatus
from runtools.runjob.process import ProcessPhase
from runtools.runjob.test.phaser import FakeRunContext


def test_exec():
    parent, child = Pipe()
    e = ProcessPhase('hello phase', exec_hello, (child,))
    e.run(FakeRunContext())
    assert parent.recv() == ['hello']


def exec_hello(pipe):
    pipe.send(['hello'])
    pipe.close()


def test_failure_error():
    e = ProcessPhase('failed phase', raise_error, ())
    with pytest.raises(FailedRun):
        e.run(FakeRunContext())


def raise_error():
    raise AssertionError


def test_failure_exit():
    e = ProcessPhase('error code phase', exec_failure_exit, ())
    with pytest.raises(FailedRun):
        e.run(FakeRunContext())


def exec_failure_exit():
    exit(1)


def test_stop():
    e = ProcessPhase('never ending story', exec_infinity_loop, ())
    t = Thread(target=stop_after, args=(0.5, e))
    t.start()
    with pytest.raises(TerminateRun) as exc_info:
        e.run(FakeRunContext())

    assert exc_info.value.term_status == TerminationStatus.STOPPED


def exec_infinity_loop():
    while True:
        sleep(0.1)


def stop_after(sec, execution):
    sleep(sec)
    execution.stop()
