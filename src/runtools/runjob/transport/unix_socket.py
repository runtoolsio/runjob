"""Unix socket transport — node-side runtime.

Layout, RPC server, and factory wiring for a node participating in a ``unix_socket``
transport environment. Connector-side pieces (client, layout base, liveness helpers)
live under ``runtools.runcore.transport.unix_socket`` and are imported here.
"""

import json
import logging
from abc import ABC, abstractmethod
from json import JSONDecodeError
from pathlib import Path
from typing import Callable, Dict, List, Optional

from runtools.runcore import paths
from runtools.runcore.err import run_isolated_collect_exceptions
from runtools.runcore.job import (
    InstanceControlEvent,
    InstanceID,
    InstanceLifecycleEvent,
    InstanceOutputEvent,
    InstancePhaseEvent,
    InstanceStatusEvent,
    JobInstance,
)
from runtools.runcore.matching import JobRunCriteria
from runtools.runcore.transport.unix_socket import (
    StandardUnixSocketConnectorLayout,
    UnixSocketConnectorLayout,
    UnixSocketEventReceiver,
    UnixSocketInstanceDirectory,
    UnixSocketInstanceDiscovery,
    UnixSocketRpcClient,
    clean_stale_component_dirs,
    ensure_component_dir,
    resolve_env_dir,
)
from runtools.runcore.util.json import ErrorCode, JsonRpcError
from runtools.runcore.util.socket import DatagramSocketClient, StreamSocketServer
from runtools.runjob.server import (
    DEFAULT_METHODS,
    JsonRpcMethodType,
    _error_response,
    _is_valid_params,
    _is_valid_request_id,
    _success_response,
    validate_params,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Layout (extends connector-side layout with node-specific socket paths)
# ---------------------------------------------------------------------------

class UnixSocketNodeLayout(UnixSocketConnectorLayout, ABC):
    """Base class for unix_socket node layouts"""

    @property
    @abstractmethod
    def server_socket_path(self) -> Path:
        """Return the server RPC socket path"""
        pass

    @property
    @abstractmethod
    def listener_lifecycle_sockets_provider(self) -> Callable:
        """Return the provider for lifecycle listener sockets"""
        pass

    @property
    @abstractmethod
    def listener_phase_sockets_provider(self) -> Callable:
        """Return the provider for phase listener sockets"""
        pass

    @property
    @abstractmethod
    def listener_output_sockets_provider(self) -> Callable:
        """Return the provider for output listener sockets"""
        pass


class StandardUnixSocketNodeLayout(StandardUnixSocketConnectorLayout, UnixSocketNodeLayout):
    """
    Standard implementation of a unix_socket node layout.

    Extends the connector layout with additional functionality specific to nodes,
    such as server sockets and additional listener providers.

    Example structure:
    /tmp/runtools/env/{env_id}/                      # Directory for the specific environment (env_dir)
    │
    └── node_abc123/                                 # Node directory (component_dir)
        ├── server-rpc.sock                          # Node's RPC server socket
        ├── listener-events.sock                     # Node's events listener socket
        └── ...                                      # Other node-specific sockets
    """

    def __init__(self, env_dir: Path, component_name: str):
        """
        Initializes the node layout with environment directory and component name.

        Args:
            env_dir: Directory containing the environment structure.
            component_name: Name of the node subdirectory.
        """
        super().__init__(env_dir=env_dir, component_name=component_name)
        self._listener_lifecycle_socket_name = "listener-lifecycle.sock"
        self._listener_phase_socket_name = "listener-phase.sock"
        self._listener_output_socket_name = "listener-output.sock"
        self._listener_control_socket_name = "listener-control.sock"
        self._listener_status_socket_name = "listener-status.sock"

    @classmethod
    def create(cls, env_id: str, root_dir: Optional[Path] = None, component_prefix: str = "node_"):
        """
        Creates a layout for a new node with a unique component directory.

        Args:
            env_id: Identifier for the environment.
            root_dir: Root directory containing environments or uses the default one.
            component_prefix: Prefix for component directories.

        Returns (StandardUnixSocketNodeLayout): Layout instance for a node.
        """
        return cls(*ensure_component_dir(env_id, component_prefix, root_dir))

    @property
    def server_socket_path(self) -> Path:
        """
        Returns:
            Path: Full path of server domain socket used for sending requests to RPC servers
        """
        return self._component_dir / self._server_socket_name

    def _provider_sockets_listener(self, socket_listener_name) -> Callable:
        """
        Helper method for creating socket providers for different listener types.

        Args:
            socket_listener_name: Socket filename to look for

        Returns:
            Callable: Provider function for the specified socket(s)
        """
        file_names = [self._listener_events_socket_name, socket_listener_name]
        return paths.files_in_subdir_provider(self._env_dir, file_names)

    @property
    def listener_lifecycle_sockets_provider(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to lifecycle listener socket files
        """
        return self._provider_sockets_listener(self._listener_lifecycle_socket_name)

    @property
    def listener_phase_sockets_provider(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to phase listener socket files
        """
        return self._provider_sockets_listener(self._listener_phase_socket_name)

    @property
    def listener_output_sockets_provider(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to output listener socket files
        """
        return self._provider_sockets_listener(self._listener_output_socket_name)

    @property
    def listener_control_sockets_provider(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to control listener socket files
        """
        return self._provider_sockets_listener(self._listener_control_socket_name)

    @property
    def listener_status_sockets_provider(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to status listener socket files
        """
        return self._provider_sockets_listener(self._listener_status_socket_name)


# ---------------------------------------------------------------------------
# RPC server
# ---------------------------------------------------------------------------

class UnixSocketNodeServer(StreamSocketServer):
    """
    Server for handling requests to job instances over the unix_socket transport (JSON-RPC 2.0).

    Each instance method requires an instance_id parameter, while collection methods use
    run_match parameter to identify target job instances. The run_match follows
    JobRunCriteria serialization format.
    """
    def __init__(self, socket_path, methods=DEFAULT_METHODS):
        super().__init__(socket_path, allow_ping=True)
        self._methods = {method.method_name: method for method in methods}
        self._job_instances = {}

    def register_instance(self, job_instance):
        self._job_instances[job_instance.id] = job_instance

    def unregister_instance(self, job_instance):
        del self._job_instances[job_instance.id]

    def handle(self, req: str) -> str:
        try:
            req_data = json.loads(req)
        except JSONDecodeError:
            return _error_response(None, ErrorCode.PARSE_ERROR, "Invalid JSON")

        # Validate JSON-RPC request
        if not isinstance(req_data, dict) or req_data.get('jsonrpc') != '2.0' or 'method' not in req_data:
            return _error_response(req_data.get('id'), ErrorCode.INVALID_REQUEST, "Invalid JSON-RPC 2.0 request")

        request_id = req_data.get('id')
        if not _is_valid_request_id(request_id):
            return _error_response(request_id, ErrorCode.INVALID_REQUEST, "Invalid request ID")

        params = req_data.get('params', {})
        if not _is_valid_params(params):
            return _error_response(request_id, ErrorCode.INVALID_REQUEST, "Invalid parameters")

        method_name = req_data['method']
        method = self._methods.get(method_name)
        if not method:
            return _error_response(request_id, ErrorCode.METHOD_NOT_FOUND, f"Method not found: {method_name}")

        try:
            validated_args = validate_params(method.parameters, params)
            if method.type == JsonRpcMethodType.INSTANCE:
                try:
                    job_instance = self._job_instances[InstanceID.deserialize(validated_args[0])]
                except KeyError:
                    return _error_response(request_id, ErrorCode.TARGET_NOT_FOUND,
                                           f"Instance not found: {validated_args[0]}")
                exec_retval = method.execute(job_instance, *validated_args[1:])
            elif method.type == JsonRpcMethodType.COLLECTION:
                job_instances = self._matching_instances(validated_args[0])
                exec_retval = method.execute(job_instances, *validated_args[1:])
            else:
                raise AssertionError("Missing implementation for method type: " + str(method.type))
        except JsonRpcError as e:
            return _error_response(request_id, e.code, e.message, e.data)
        except Exception as e:
            log.error("JSON-RPC handler error", exc_info=True)
            return _error_response(request_id, ErrorCode.METHOD_EXECUTION_ERROR, f"Internal error: {str(e)}")

        return _success_response(request_id, {"retval": exec_retval})

    def _matching_instances(self, run_match: Dict) -> List:
        try:
            matching_criteria = JobRunCriteria.deserialize(run_match)
        except ValueError as e:
            raise JsonRpcError(ErrorCode.INVALID_PARAMS, f"Invalid run match criteria: {e}")

        return [job_instance for job_instance in self._job_instances.values() if
                matching_criteria.matches(job_instance.snap())]


# ---------------------------------------------------------------------------
# Event dispatcher
# ---------------------------------------------------------------------------

class UnixSocketEventDispatcher:
    """Datagram-socket dispatcher that broadcasts a node's events to listeners.

    Used as a callable observer (``__call__(event)`` serializes and sends the event over a
    ``DatagramSocketClient``). Private to :class:`UnixSocketInstanceAccessPoint`, which
    attaches it when an instance is registered.
    """

    def __init__(self, client, event_type_to_sockets_provider=None):
        """Initialize with a socket client for communication.

        Args:
            client: Socket client used to communicate with listeners
        """
        self._client = client
        self._event_type_to_sockets_provider = event_type_to_sockets_provider or {}

    def __call__(self, event):
        self.send_event(event.event_type, event)

    def send_event(self, event_type, event):
        socket_listeners_provider = self._event_type_to_sockets_provider.get(event_type)
        if socket_listeners_provider:
            addresses = socket_listeners_provider()
        else:
            addresses = None

        payload = json.dumps(event.serialize())
        self._client.communicate(payload, addresses)

    def close(self):
        """Release resources used by the dispatcher."""
        self._client.close()


# ---------------------------------------------------------------------------
# Node-side transport bundle
# ---------------------------------------------------------------------------

class UnixSocketInstanceAccessPoint:
    """Node-side runtime bundle for the unix_socket transport.

    Exposes the node's instances to the environment by registering them with the RPC
    server and attaching the event dispatcher as an observer. The split between the
    two is a unix_socket detail; the node only asks to register/unregister an instance.
    The node's sibling-facing connector is built separately by ``create_node`` and is
    not part of this bundle.

    Conforms to :class:`runtools.runjob.transport.InstanceAccessPoint`.
    """

    def __init__(self, rpc_server: UnixSocketNodeServer, event_dispatcher: UnixSocketEventDispatcher):
        self._rpc_server = rpc_server
        self._event_dispatcher = event_dispatcher

    def start(self) -> None:
        self._rpc_server.start()

    def register_instance(self, job_instance: JobInstance) -> None:
        self._rpc_server.register_instance(job_instance)
        job_instance.notifications.add_observer_all_events(self._event_dispatcher)

    def unregister_instance(self, job_instance: JobInstance) -> None:
        job_instance.notifications.remove_observer_all_events(self._event_dispatcher)
        self._rpc_server.unregister_instance(job_instance)

    def close(self) -> None:
        run_isolated_collect_exceptions(
            "Errors during closing unix_socket instance access point",
            self._rpc_server.close,
            self._event_dispatcher.close,
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_node_transports(
        env_id: str,
        root_dir: Optional[Path] = None,
) -> tuple[UnixSocketInstanceDirectory, UnixSocketInstanceAccessPoint]:
    """Build a unix_socket node's transport pair, sharing a single layout.

    Returns ``(sibling_directory, access_point)``. The directory is for the node's
    sibling-monitoring use; the access point carries the node-only server, event
    dispatcher, and lock factory. Both reference the same layout so the node lives in
    one component dir.
    """
    # Sweep stale components before allocating our own — no need to scan past our live lock.
    clean_stale_component_dirs(resolve_env_dir(env_id, root_dir))
    layout = StandardUnixSocketNodeLayout.create(env_id, root_dir)

    rpc_client = UnixSocketRpcClient(layout.server_sockets_provider)
    sibling_directory = UnixSocketInstanceDirectory(layout, rpc_client, UnixSocketInstanceDiscovery(rpc_client),
                                                    UnixSocketEventReceiver(layout.listener_events_socket_path))
    access_point = UnixSocketInstanceAccessPoint(
        UnixSocketNodeServer(layout.server_socket_path),
        UnixSocketEventDispatcher(DatagramSocketClient(), {
            InstanceLifecycleEvent.EVENT_TYPE: layout.listener_lifecycle_sockets_provider,
            InstancePhaseEvent.EVENT_TYPE: layout.listener_phase_sockets_provider,
            InstanceOutputEvent.EVENT_TYPE: layout.listener_output_sockets_provider,
            InstanceControlEvent.EVENT_TYPE: layout.listener_control_sockets_provider,
            InstanceStatusEvent.EVENT_TYPE: layout.listener_status_sockets_provider,
        }),
    )
    return sibling_directory, access_point
