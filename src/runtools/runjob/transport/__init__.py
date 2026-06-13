"""Transport runtimes for runtools job nodes.

Each transport ships its own module here with the concrete node-side bundle, layout,
and RPC server. Connector-side transport pieces live under ``runtools.runcore.transport``.

The :class:`InstanceAccessPoint` protocol declares what the concrete node
(``runtools.runjob.node._Node``) holds to expose its instances to the environment.
Concrete bundles live in ``runtools.runjob.transport.<transport>``.
"""

from typing import Callable, Protocol, runtime_checkable

from runtools.runcore.job import JobInstance


@runtime_checkable
class NodeRpcServer(Protocol):
    """Minimal RPC server surface used by the concrete environment node.

    The server exposes this node's job instances to remote connectors and is started /
    stopped alongside the node's lifecycle.
    """
    def start(self) -> None: ...

    def close(self) -> None: ...

    def register_instance(self, job_instance: JobInstance) -> None: ...

    def unregister_instance(self, job_instance: JobInstance) -> None: ...


@runtime_checkable
class NodeEventDispatcher(Protocol):
    """Callable that broadcasts a single event from this node to its listeners.

    The observer machinery treats callable observers as event sinks — the dispatcher is
    invoked as ``dispatcher(event)`` for each event of any type the node emits.
    """
    def __call__(self, event) -> None: ...

    def close(self) -> None: ...


@runtime_checkable
class InstanceAccessPoint(Protocol):
    """Node-side bundle through which the node's instances are exposed to the env.

    Implementations expose:
      - ``rpc_server``: the RPC server exposing this node's instances.
      - ``event_dispatcher``: broadcasts this node's events (see :class:`NodeEventDispatcher`).
      - ``lock_factory``: ``(lock_id) -> context-manager lock`` for job coordination.
      - ``close()``: releases node-only resources.

    The node's sibling-facing connector is **not** part of this bundle — it is constructed
    separately by the transport factory (sharing whatever layout / shared state the
    transport needs) and passed into the node.
    """
    rpc_server: NodeRpcServer
    event_dispatcher: NodeEventDispatcher
    lock_factory: Callable[[str], object]

    def close(self) -> None:
        ...
