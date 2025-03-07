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
from abc import ABC, abstractmethod

from runtools.runcore.util.socket import PayloadTooLarge

log = logging.getLogger(__name__)


class Event(ABC):

    @property
    @abstractmethod
    def event_type(self) -> str:
        pass


class EventDispatcher:
    """
    Handles the sending of events over socket connections.
    """

    def __init__(self, client, event_type_to_sockets_provider=None):
        """
        Initialize with a socket client for communication.

        Args:
            client: Socket client used to communicate with listeners
        """
        self._client = client
        self._event_type_to_sockets_provider = event_type_to_sockets_provider or {}

    def __call__(self, event):
        self.send_event(event.event_type, event)

    def send_event(self, event_type, event):
        """
        Send an event to all registered listeners.

        Args:
            event_type(str): Type identifier for the event
            event(Serializable): Event to send
        """
        socket_listeners_provider = self._event_type_to_sockets_provider.get(event_type)
        if socket_listeners_provider:
            addresses = socket_listeners_provider()
        else:
            addresses = None
        try:
            self._client.communicate(json.dumps(event.serialize()), addresses)
        except PayloadTooLarge:
            log.warning("event=[event_dispatch_failed] reason=[payload_too_large] note=[Please report this issue!]")

    def close(self):
        """Release resources used by the dispatcher."""
        self._client.close()
