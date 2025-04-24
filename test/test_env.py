from dataclasses import dataclass
from typing import List

import pytest

from runtools.runcore.job import JobInstance, iid
from runtools.runcore.run import Stage
from runtools.runjob import environment
from runtools.runjob.environment import Feature
from runtools.runjob.test.phase import TestPhase


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

        inst = e.create_instance(iid("test_job"), [TestPhase()])
        inst2 = e.create_instance(iid('test_job_2'), [TestPhase()])

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


def test_instance_stage_observer(env):
    """Test instance transition notifications"""
    stages = []
    transitions = []

    def observer_s(event):
        stages.append(event)

    def observer_t(event):
        transitions.append(event)

    env.add_observer_lifecycle(observer_s)
    env.add_observer_transition(observer_t)

    i = env.create_instance(iid("test_job"), [(TestPhase())])
    # TODO assert transitions[-1].new_stage == Stage.CREATED + transition phases

    i.run()
    assert stages[-2].new_stage == Stage.RUNNING
    assert stages[-1].new_stage == Stage.ENDED


def test_output_observer(env):
    """Test instance output notifications"""
    outputs = []

    def observer(e):
        outputs.append(e)

    env.add_observer_output(observer)
    env.create_instance(iid("test_job"), [(TestPhase(output_text='hey more'))]).run()
    assert outputs[0].output_line.text == 'hey more'
