"""
This module contains event dispatchers as part of the Job Instance Event framework.
The dispatchers deliver events of registered job instances to the listeners using domain socket communication.
The event listeners are expected to create a domain socket with a corresponding file suffix located
in the user's own subdirectory, which is in the `/tmp` directory by default. The sockets are used by the dispatchers
to send the events. In this design, the communication is unidirectional, with servers acting as consumers
and clients as producers.
"""

import json
import logging

from runtools.runcore.job import (
    InstanceTransitionObserver,
    InstanceOutputObserver,
    InstanceTransitionEvent,
    InstanceOutputEvent,
    InstanceStageObserver,
    InstanceStageEvent
)
from runtools.runcore.util.socket import PayloadTooLarge

log = logging.getLogger(__name__)


class EventDispatcher:
    """
    Handles the sending of events over socket connections.
    """

    def __init__(self, client):
        """
        Initialize with a socket client for communication.

        Args:
            client: Socket client used to communicate with listeners
        """
        self._client = client

    def send_event(self, event_type, instance_meta, event_serialized):
        """
        Send an event to all registered listeners.

        Args:
            event_type: Type identifier for the event
            instance_meta: Metadata about the instance generating the event
            event_serialized: Serialized event data
        """
        event_body = {
            "event_metadata": {
                "event_type": event_type
            },
            "instance_metadata": instance_meta.serialize(),
            "event": event_serialized
        }
        try:
            self._client.communicate(json.dumps(event_body))
        except PayloadTooLarge:
            log.warning("event=[event_dispatch_failed] reason=[payload_too_large] note=[Please report this issue!]")

    def close(self):
        """Release resources used by the dispatcher."""
        self._client.close()


class StageDispatcher(InstanceStageObserver):
    """
    Dispatches job instance stage change events.
    This dispatcher should be registered to the job instance as an `InstanceStageObserver`.
    """

    def __init__(self, event_dispatcher):
        """
        Initialize with an event dispatcher.

        Args:
            event_dispatcher: EventDispatcher instance for sending events
        """
        self._dispatcher = event_dispatcher

    def new_instance_stage(self, event: InstanceStageEvent):
        """
        Handle a stage change event by dispatching it.

        Args:
            event: The stage change event
        """
        self._dispatcher.send_event("new_instance_stage", event.instance, event.serialize())

    def close(self):
        """Release resources used by the dispatcher."""
        self._dispatcher.close()


class TransitionDispatcher(InstanceTransitionObserver):
    """
    Dispatches job instance transition events.
    This dispatcher should be registered to the job instance as an `InstanceTransitionObserver`.
    """

    def __init__(self, event_dispatcher):
        """
        Initialize with an event dispatcher.

        Args:
            event_dispatcher: EventDispatcher instance for sending events
        """
        self._dispatcher = event_dispatcher

    def new_instance_transition(self, event: InstanceTransitionEvent):
        """
        Handle a transition event by dispatching it.

        Args:
            event: The transition event
        """
        self._dispatcher.send_event("new_instance_transition", event.instance, event.serialize())

    def close(self):
        """Release resources used by the dispatcher."""
        self._dispatcher.close()


class OutputDispatcher(InstanceOutputObserver):
    """
    Dispatches job instance output events.
    This dispatcher should be registered to the job instance as an `InstanceOutputObserver`.
    """

    def __init__(self, event_dispatcher):
        """
        Initialize with an event dispatcher.

        Args:
            event_dispatcher: EventDispatcher instance for sending events
        """
        self._dispatcher = event_dispatcher

    def new_instance_output(self, event: InstanceOutputEvent):
        """
        Handle an output event by dispatching it.

        Args:
            event: The output event
        """
        self._dispatcher.send_event("new_instance_output", event.instance, event.serialize(10000))

    def close(self):
        """Release resources used by the dispatcher."""
        self._dispatcher.close()
