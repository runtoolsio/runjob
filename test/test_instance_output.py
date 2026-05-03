"""
Tests :mod:`runjob` module
"""
import pytest

from runtools.runcore.job import iid
from runtools.runcore.run import TerminationStatus
from runtools.runcore.test.observer import TestOutputObserver

from runtools.runjob import instance
from runtools.runjob.instance import OUTPUT_PERSISTENCE_ERROR
from runtools.runjob.output import OutputRouter, OutputWriter
from runtools.runjob.process import ProcessPhase
from runtools.runjob.test.phase import TestPhase


def print_hello():
    print("Hello, lucky boy. Where are you today?")


def print_countdown():
    text = "3\n2\n1\neveryone\nin\nthe\nworld\nis\ndoing\nsomething\nwithout\nme"
    lines = text.split('\n')

    for line in lines:
        print(line)


def test_output_observer():
    exec_phase = ProcessPhase('Printing', print_hello)
    i = instance.create(iid('j1', 'i1'), None, exec_phase)
    observer = TestOutputObserver()
    i.notifications.add_observer_output(observer)

    i.run()

    assert observer.last_message == "Hello, lucky boy. Where are you today?"


def test_last_output():
    exec_phase = ProcessPhase('Printing', print_countdown)
    i = instance.create(iid('j1', 'i1'), None, exec_phase)
    i.run()
    assert ([line.message for line in i.output.tail(max_lines=10)] ==
            "1 everyone in the world is doing something without me".split())


class _FailingOnCloseWriter(OutputWriter):
    """OutputWriter that accepts lines but fails its final close."""

    def __init__(self):
        self._location = None

    @property
    def location(self):
        return self._location

    def store_line(self, line):
        pass

    def close(self):
        raise RuntimeError("simulated S3 PUT failure")


def test_output_persistence_failure_records_fault_and_propagates():
    """When OutputRouter.close() fails, the run records OUTPUT_PERSISTENCE_ERROR
    as a fault, surfaces the failure to the caller, and leaves termination_status
    on root_phase unchanged (it reflects what the program actually did).
    """
    phase = TestPhase()  # succeeds, terminates COMPLETED
    router = OutputRouter(storages=[_FailingOnCloseWriter()])
    inst = instance.create(iid('j1', 'i1'), None, phase, output_router=router)

    with pytest.raises(RuntimeError, match="simulated S3 PUT failure"):
        inst.run()

    snap = inst.snap()
    assert phase.termination.status == TerminationStatus.COMPLETED  # unchanged
    fault_categories = [f.category for f in snap.faults]
    assert OUTPUT_PERSISTENCE_ERROR in fault_categories
    persistence_fault = next(f for f in snap.faults if f.category == OUTPUT_PERSISTENCE_ERROR)
    assert "simulated S3 PUT failure" in persistence_fault.reason