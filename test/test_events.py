from runtools.runcore.listening import InstanceTransitionReceiver, InstanceOutputReceiver
from runtools.runcore.output import OutputLine
from runtools.runcore.run import PhaseRun, RunState, PhaseInfo
from runtools.runcore.job import JobInstanceMetadata
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.test.observer import GenericObserver
from runtools.runcore.util import utc_now

from runtools.runjob.events import TransitionDispatcher, OutputDispatcher


def test_state_dispatching():
    dispatcher = TransitionDispatcher()
    receiver = InstanceTransitionReceiver()
    observer = GenericObserver()
    receiver.add_observer_transition(observer)
    receiver.start()

    test_run = fake_job_run('j1')
    prev = PhaseRun('approval', RunState.PENDING, utc_now(), utc_now())
    new = PhaseRun('exec', RunState.EXECUTING, utc_now(), None)
    try:
        dispatcher.new_instance_phase(test_run, prev, new, 2)
    finally:
        dispatcher.close()
        receiver.close()

    instance_meta, prev_run, new_run, ordinal = observer.updates.get(timeout=2)[1]
    assert instance_meta.metadata.job_id == 'j1'
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
    try:
        dispatcher.new_instance_output(instance_meta, OutputLine("Happy Mushrooms", True, "Magic"))
    finally:
        dispatcher.close()
        receiver.close()

    o_instance_meta, output_line = observer.updates.get(timeout=2)[1]
    assert o_instance_meta == instance_meta
    assert output_line.text == "Happy Mushrooms"
    assert output_line.is_error
    assert output_line.source == "Magic"
