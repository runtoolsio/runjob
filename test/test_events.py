from runtools.runcore.job import JobInstanceMetadata, InstanceTransitionEvent, InstanceOutputEvent
from runtools.runcore.listening import InstanceTransitionReceiver, InstanceOutputReceiver
from runtools.runcore.output import OutputLine
from runtools.runcore.run import Stage
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.test.observer import GenericObserver
from runtools.runcore.util import utc_now
from runtools.runjob.events import TransitionDispatcher, OutputDispatcher


def test_transition_dispatching():
    dispatcher = TransitionDispatcher()
    receiver = InstanceTransitionReceiver()
    observer = GenericObserver()
    receiver.add_observer_transition(observer)
    receiver.start()

    job_run = fake_job_run('j1', 'r1')
    event = InstanceTransitionEvent(
        instance=job_run.metadata,
        job_run=job_run,
        is_root_phase=True,
        phase_id=job_run.phases[0].phase_id,
        new_stage=Stage.RUNNING,
        timestamp=(utc_now())
    )

    try:
        dispatcher.new_instance_transition(event)
    finally:
        dispatcher.close()
        receiver.close()

    received_event = observer.updates.get(timeout=2)[1][0]
    assert received_event == event


def test_output_dispatching():
    dispatcher = OutputDispatcher()
    receiver = InstanceOutputReceiver()
    observer = GenericObserver()
    receiver.add_observer_output(observer)
    receiver.start()

    event = InstanceOutputEvent(
        instance=(JobInstanceMetadata('j1', 'r1', 'i1', {})),
        output_line=(OutputLine("Fucking voodoo magic, man!", True, "5555")),
        timestamp=utc_now()
    )

    try:
        dispatcher.new_instance_output(event)
    finally:
        dispatcher.close()
        receiver.close()

    received_event = observer.updates.get(timeout=2)[1][0]
    assert received_event == event
