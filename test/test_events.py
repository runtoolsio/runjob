from runtools.runcore.job import JobInstanceMetadata, InstanceTransitionEvent, InstanceOutputEvent
from runtools.runcore.listening import EventReceiver, InstanceEventReceiver
from runtools.runcore.output import OutputLine
from runtools.runcore.run import Stage
from runtools.runjob.test import testutil
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.test.observer import GenericObserver
from runtools.runcore.util import utc_now
from runtools.runcore.util.socket import SocketClient
from runtools.runjob.events import EventDispatcher


def test_transition_dispatching():
    test_path = testutil.random_test_socket()
    dispatcher = EventDispatcher(SocketClient(lambda: [test_path]))
    observer = GenericObserver()
    instance_event_receiver = InstanceEventReceiver()
    instance_event_receiver.add_observer_transition(observer)
    receiver = EventReceiver(test_path).register_handler(instance_event_receiver, InstanceTransitionEvent.EVENT_TYPE)
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
        dispatcher(event)
    finally:
        dispatcher.close()
        receiver.close()

    received_event = observer.updates.get(timeout=2)[1][0]
    assert received_event == event


def test_output_dispatching():
    test_path = testutil.random_test_socket()
    dispatcher = EventDispatcher(SocketClient(lambda: [test_path]))
    observer = GenericObserver()
    instance_event_receiver = InstanceEventReceiver()
    instance_event_receiver.add_observer_output(observer)
    receiver = EventReceiver(test_path).register_handler(instance_event_receiver, InstanceOutputEvent.EVENT_TYPE)
    receiver.start()

    event = InstanceOutputEvent(
        instance=(JobInstanceMetadata('j1', 'r1', 'i1', {})),
        output_line=(OutputLine("Fucking voodoo magic, man!", True, "5555")),
        timestamp=utc_now()
    )

    try:
        dispatcher(event)
    finally:
        dispatcher.close()
        receiver.close()

    received_event = observer.updates.get(timeout=2)[1][0]
    assert received_event == event
