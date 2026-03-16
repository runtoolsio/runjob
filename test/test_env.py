from dataclasses import dataclass
from typing import List

import pytest

from runtools.runcore.job import DuplicateInstanceError, DuplicateStrategy, JobInstance
from runtools.runcore.run import Stage
from runtools.runjob import node
from runtools.runjob.node import Feature
from runtools.runjob.test.phase import TestPhase


@dataclass
class TestFeature(Feature):
    __test__ = False  # Prevent pytest collection
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
    with node.in_process(features=feature, transient=True) as env:
        yield env


def test_environment_lifecycle(feature):
    """Test basic environment lifecycle - open, add instance, close"""
    with node.in_process(features=feature, transient=True) as e:
        assert feature.opened

        inst = e.create_instance("test_job", root_phase=TestPhase())
        inst2 = e.create_instance("test_job_2", root_phase=TestPhase())

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

    env.notifications.add_observer_lifecycle(observer_s)
    env.notifications.add_observer_phase(observer_t)

    i = env.create_instance("test_job", root_phase=TestPhase())
    # TODO assert transitions[-1].new_stage == Stage.CREATED + transition phases

    i.run()
    assert stages[-2].new_stage == Stage.RUNNING
    assert stages[-1].new_stage == Stage.ENDED


def test_output_observer(env):
    """Test instance output notifications"""
    outputs = []

    def observer(e):
        outputs.append(e)

    env.notifications.add_observer_output(observer)
    env.create_instance("test_job", root_phase=TestPhase(output_text='hey more')).run()
    assert outputs[0].output_line.message == 'hey more'


# --- Admission / duplicate handling ---


def test_duplicate_instance_raises(env):
    """Creating the same instance_id twice with default strategy raises DuplicateInstanceError."""
    env.create_instance("dup_job", "run1", TestPhase()).run()

    with pytest.raises(DuplicateInstanceError):
        env.create_instance("dup_job", "run1", TestPhase())


def test_duplicate_continue_increments_ordinal(env):
    """With CONTINUE strategy, a duplicate admission succeeds with incremented ordinal."""
    first = env.create_instance("rerun_job", "run1", TestPhase(), duplicate_strategy=DuplicateStrategy.ALLOW)
    first.run()

    second = env.create_instance("rerun_job", "run1", TestPhase(), duplicate_strategy=DuplicateStrategy.ALLOW)
    assert second.id.ordinal == 2
    second.run()


def test_continue_multiple_ordinals(env):
    """Creating the same logical run 3 times with CONTINUE yields ordinals 1, 2, 3."""
    ordinals = []

    for _ in range(3):
        inst = env.create_instance("multi_job", "run1", TestPhase(), duplicate_strategy=DuplicateStrategy.ALLOW)
        ordinals.append(inst.id.ordinal)
        inst.run()

    assert ordinals == [1, 2, 3]


def test_duplicate_different_run_ids_no_conflict(env):
    """Two instances with the same job_id but different run_ids do not conflict."""
    first = env.create_instance("job", "run_a", TestPhase())
    second = env.create_instance("job", "run_b", TestPhase())

    assert first.id.run_id == "run_a"
    assert second.id.run_id == "run_b"
    first.run()
    second.run()


def test_duplicate_different_ordinals_no_conflict(env):
    """Instances with same (job_id, run_id) but different ordinals do not conflict."""
    first = env.create_instance("job", "run1", TestPhase())
    second = env.create_instance("job", "run1", TestPhase(),
                                 duplicate_strategy=DuplicateStrategy.ALLOW)

    assert first.id.ordinal == 1
    assert second.id.ordinal == 2
    first.run()
    second.run()
