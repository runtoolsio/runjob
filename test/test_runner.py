"""
Tests :mod:`runjob` module
"""
from runtools.runcore.output import Mode
from runtools.runcore.test.observer import TestOutputObserver

from runtools.runjob import RunnerJobInstance
from runtools.runjob.execution import ExecutingPhase
from runtools.runjob.phaser import Phaser
from runtools.runjob.process import ProcessExecution


def test_output_observer():
    def print_it():
        print("Hello, lucky boy. Where are you today?")

    execution = ProcessExecution(print_it)
    instance = RunnerJobInstance('j1', 'i1', Phaser([ExecutingPhase('Printing', execution)]))
    observer = TestOutputObserver()
    instance.add_observer_output(observer)

    instance.run()

    assert observer.last_line == "Hello, lucky boy. Where are you today?"


def test_last_output():
    def print_it():
        text = "3\n2\n1\neveryone\nin\nthe\nworld\nis\ndoing\nsomething\nwithout\nme"
        lines = text.split('\n')

        for line in lines:
            print(line)

    execution = ProcessExecution(print_it)
    instance = RunnerJobInstance('j1', 'i1', Phaser([ExecutingPhase('Printing', execution)]))
    instance.run()
    assert ([out for out, _ in instance.get_output(Mode.TAIL, lines=10)] ==
            "1 everyone in the world is doing something without me".split())
