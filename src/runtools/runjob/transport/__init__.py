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
class InstanceAccessPoint(Protocol):
    """Node-side seam through which the node's instances are exposed to the env.

    ``register_instance`` / ``unregister_instance`` make an instance reachable and
    observable; how that maps onto wire resources (RPC server, event dispatch, ...) is
    the transport's concern, not the node's. ``lock_factory`` builds ``(lock_id) ->
    context-manager lock`` for job coordination. ``close()`` releases node-only resources.

    The node's sibling-facing connector is **not** part of this seam — it is constructed
    separately by the transport factory (sharing whatever layout / shared state the
    transport needs) and passed into the node.
    """
    lock_factory: Callable[[str], object]

    def start(self) -> None: ...

    def register_instance(self, job_instance: JobInstance) -> None: ...

    def unregister_instance(self, job_instance: JobInstance) -> None: ...

    def close(self) -> None: ...
