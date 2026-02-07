from runtools.runcore.job import JobInstanceMetadata, InstancePhaseEvent, InstanceOutputEvent, InstanceID
from runtools.runcore.listening import EventReceiver, InstanceEventReceiver
from runtools.runcore.output import OutputLine
from runtools.runcore.run import Stage
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.test.observer import GenericObserver
from runtools.runcore.util import utc_now
from runtools.runcore.util.socket import DatagramSocketClient
from runtools.runjob.events import EventDispatcher
from runtools.runjob.test import testutil


def test_transition_dispatching():
    test_path = testutil.random_test_socket()
    dispatcher = EventDispatcher(DatagramSocketClient(lambda: [test_path]))
    observer = GenericObserver()
    instance_event_receiver = InstanceEventReceiver()
    instance_event_receiver.add_observer_phase(observer)
    receiver = EventReceiver(test_path).register_handler(instance_event_receiver, InstancePhaseEvent.EVENT_TYPE)
    receiver.start()

    job_run = fake_job_run('j1', 'r1')
    event = InstancePhaseEvent(
        instance=job_run.metadata,
        job_run=job_run,
        is_root_phase=True,
        phase_id=job_run.root_phase.children[0].phase_id,
        new_stage=Stage.RUNNING,
        timestamp=(utc_now())
    )

    try:
        dispatcher(event)
        received_event = observer.updates.get(timeout=2)[1][0]
    finally:
        dispatcher.close()
        receiver.close()

    assert received_event == event


def test_output_dispatching():
    test_path = testutil.random_test_socket()
    dispatcher = EventDispatcher(DatagramSocketClient(lambda: [test_path]))
    observer = GenericObserver()
    instance_event_receiver = InstanceEventReceiver()
    instance_event_receiver.add_observer_output(observer)
    receiver = EventReceiver(test_path).register_handler(instance_event_receiver, InstanceOutputEvent.EVENT_TYPE)
    receiver.start()

    event = InstanceOutputEvent(
        instance=(JobInstanceMetadata(InstanceID('j1', 'r1'), {})),
        output_line=(OutputLine("Fucking voodoo magic, man!", 1, is_error=True, source="5555")),
        timestamp=utc_now()
    )

    try:
        dispatcher(event)
        received_event = observer.updates.get(timeout=2)[1][0]
    finally:
        dispatcher.close()
        receiver.close()

    assert received_event == event
