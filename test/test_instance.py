"""
Tests :mod:`runjob` module
"""
from runtools.runcore.test.observer import TestOutputObserver

from runtools.runjob import instance
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
    i = instance.create('j1', [exec_phase], instance_id='i1')
    observer = TestOutputObserver()
    i.add_observer_output(observer)

    i.run()

    assert observer.last_text == "Hello, lucky boy. Where are you today?"


def test_last_output():
    exec_phase = ProcessPhase('Printing', print_countdown)
    i = instance.create('j1', [exec_phase], instance_id='i1')
    i.run()
    assert ([line.text for line in i.output.tail(max_lines=10)] ==
            "1 everyone in the world is doing something without me".split())