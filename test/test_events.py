from runtools.runcore.listening import InstanceTransitionReceiver, InstanceOutputReceiver
from runtools.runcore.run import PhaseRun, RunState, PhaseInfo, JobInstanceMetadata, PhaseKey
from runtools.runcore.test.job import ended_run
from runtools.runcore.test.observer import GenericObserver
from runtools.runcore.util import utc_now

from runtools.runner.events import TransitionDispatcher, OutputDispatcher


def test_state_dispatching():
    dispatcher = TransitionDispatcher()
    receiver = InstanceTransitionReceiver()
    observer = GenericObserver()
    receiver.add_observer_transition(observer)
    receiver.start()

    test_run = ended_run('j1')
    prev = PhaseRun(PhaseKey('approval', 'id'), RunState.PENDING, utc_now(), utc_now())
    new = PhaseRun(PhaseKey('exec', 'id'), RunState.EXECUTING, utc_now(), None)
    try:
        dispatcher.new_instance_phase(test_run, prev, new, 2)
    finally:
        dispatcher.close()
        receiver.close()

    instance_meta, prev_run, new_run, ordinal = observer.updates.get(timeout=2)[1]
    assert instance_meta.metadata.entity_id == 'j1'
    assert prev_run == prev
    assert new_run == new
    assert ordinal == 2


def test_output_dispatching():
    dispatcher = OutputDispatcher()
    receiver = InstanceOutputReceiver()
    observer = GenericObserver()
    receiver.add_observer_output(observer)
    receiver.start()
    instance_meta = JobInstanceMetadata('j1', 'r1', 'i1', {}, {})
    phase = PhaseInfo("Magic", "Bar in Pai", RunState.EXECUTING)
    try:
        dispatcher.new_instance_output(instance_meta, phase, "Happy Mushrooms", True)
    finally:
        dispatcher.close()
        receiver.close()

    o_instance_meta, o_phase, new_output, is_error = observer.updates.get(timeout=2)[1]
    assert o_instance_meta == instance_meta
    assert o_phase == phase
    assert new_output == "Happy Mushrooms"
    assert is_error
