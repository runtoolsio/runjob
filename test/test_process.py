"""
Tests :mod:`process` module
"""
from multiprocessing import Pipe
from threading import Thread

import pytest
from time import sleep

from runtools.runjob.execution import ExecutionResult, ExecutionException
from runtools.runjob.process import ProcessExecution


def test_exec():
    parent, child = Pipe()
    e = ProcessExecution(exec_hello, (child,))
    exec_res = e.execute()
    assert parent.recv() == ['hello']
    assert exec_res == ExecutionResult.DONE


def exec_hello(pipe):
    pipe.send(['hello'])
    pipe.close()


def test_failure_error():
    e = ProcessExecution(exec_failure_error, ())
    with pytest.raises(ExecutionException):
        e.execute()


def exec_failure_error():
    raise AssertionError


def test_failure_exit():
    e = ProcessExecution(exec_failure_exit, ())
    with pytest.raises(ExecutionException):
        e.execute()


def exec_failure_exit():
    exit(1)


# @pytest.mark.skip(reason="Hangs tests executed for all project")  # TODO
def test_stop():
    e = ProcessExecution(exec_never_ending_story, ())
    t = Thread(target=stop_after, args=(0.5, e))
    t.start()
    term_state = e.execute()
    assert term_state == ExecutionResult.STOPPED


def exec_never_ending_story():
    while True:
        sleep(0.1)


def stop_after(sec, execution):
    sleep(sec)
    execution.stop()
