"""
Tests :mod:`runjob` module
"""
from runtools.runcore.test.observer import TestOutputObserver

from runtools.runjob import RunnerJobInstance
from runtools.runjob.phaser import Phaser
from runtools.runjob.process import ProcessPhase


def print_hello():
    print("Hello, lucky boy. Where are you today?")


def print_countdown():
    text = "3\n2\n1\neveryone\nin\nthe\nworld\nis\ndoing\nsomething\nwithout\nme"
    lines = text.split('\n')

    for line in lines:
        print(line)


def test_output_observer():
    exec_phase = ProcessPhase('Printing', print_hello)
    instance = RunnerJobInstance('j1', 'i1', Phaser([exec_phase]))
    observer = TestOutputObserver()
    instance.add_observer_output(observer)

    instance.run()

    assert observer.last_text == "Hello, lucky boy. Where are you today?"


def test_last_output():
    exec_phase = ProcessPhase('Printing', print_countdown)
    instance = RunnerJobInstance('j1', 'i1', Phaser([exec_phase]))
    instance.run()
    assert ([line.text for line in instance.output.tail(max_lines=10)] ==
            "1 everyone in the world is doing something without me".split())