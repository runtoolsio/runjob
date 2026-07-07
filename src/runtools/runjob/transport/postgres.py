"""Postgres transport — node-side runtime.

The postgres kind has no wire to serve: instances are exposed by the run-state persister's
snapshots (``runcore.db.persister``), which remote polling directories read. This module
holds only the node's receiving end.
"""


class PostgresInstanceAccessPoint:
    """Node-side receiving end for postgres environments — currently empty.

    Remote commands reach the node with signals-as-state (design point 5), which implements
    this as the signal doorbell/reconciler (plausibly LISTEN/NOTIFY). Until then there is
    nothing to receive, and remote control of this node's instances is unsupported
    (consumer proxies raise).

    Conforms to :class:`runtools.runjob.transport.InstanceAccessPoint`.
    """

    def start(self) -> None:
        pass

    def register_instance(self, job_instance) -> None:
        pass

    def unregister_instance(self, job_instance) -> None:
        pass

    def close(self) -> None:
        pass
