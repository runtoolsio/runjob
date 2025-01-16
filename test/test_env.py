from dataclasses import dataclass
from typing import List

import pytest

from runtools.runcore.job import JobInstance
from runtools.runcore.run import RunState
from runtools.runjob import instance, environment
from runtools.runjob.environment import Feature
from runtools.runjob.test.phaser import TestPhase


@dataclass
class TestFeature(Feature):
    opened = False
    closed = False
    added_instances: List[JobInstance] = None
    removed_instances: List[JobInstance] = None

    def __post_init__(self):
        self.added_instances = []
        self.removed_instances = []

    def on_open(self):
        self.opened = True

    def on_close(self):
        self.closed = True

    def on_instance_added(self, inst):
        self.added_instances.append(inst)

    def on_instance_removed(self, inst):
        self.removed_instances.append(inst)


@pytest.fixture
def feature():
    return TestFeature()

@pytest.fixture
def env(feature):
    with environment.isolated(features=feature, transient=True) as env:
        yield env


def test_environment_lifecycle(feature):
    """Test basic environment lifecycle - open, add instance, close"""
    with environment.isolated(features=feature, transient=True) as e:
        assert feature.opened

        inst = e.create_instance("test_job", [(TestPhase())])
        inst2 = e.add_instance(instance.create('test_job_2', [TestPhase()]))

        assert feature.added_instances[0] == inst
        assert feature.added_instances[1] == inst2
        assert inst in e.instances
        assert inst2 in e.instances

        inst.run()
        inst2.run()

        assert not e.instances
        assert feature.removed_instances[0] == inst
        assert feature.removed_instances[1] == inst2
        assert not feature.closed

    assert feature.closed


def test_instance_explicit_removal(feature, env):
    """Test explicitly removing an instance"""
    inst = instance.create("test_job", [TestPhase()])

    env.add_instance(inst)
    assert inst in env.instances
    assert feature.added_instances[0] == inst

    assert env.remove_instance(inst.instance_id) == inst
    assert not env.instances
    assert feature.removed_instances[0] == inst


def test_instance_transition_observer(env):
    """Test instance transition notifications"""
    transitions = []

    def observer(job_run, _, new_phase, __):
        transitions.append((job_run.metadata.job_id, new_phase.run_state))

    env.add_observer_transition(observer)

    i = env.create_instance("test_job", [(TestPhase())])
    # TODO uncomment when fixed
    # assert transitions[0] == ("test_job", RunState.CREATED)

    i.run()
    assert transitions[-1] == ("test_job", RunState.ENDED)


def test_output_observer(env):
    """Test instance output notifications"""
    outputs = []

    def observer(metadata, output_line):
        outputs.append((metadata.job_id, output_line.text))

    env.add_observer_output(observer)
    env.create_instance("test_job", [(TestPhase(output_text='hey more'))]).run()
    assert outputs[0] == ("test_job", "hey more")
