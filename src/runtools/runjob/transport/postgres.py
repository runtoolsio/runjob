"""Postgres transport — node-side runtime.

The postgres kind has no wire to serve: instances are exposed by the run-state persister's
snapshots (``runcore.db.persister``), which remote polling directories read. Inbound commands
arrive through the signals mailbox (design point 5) — this module holds the node's receiving
end, the signal reconciler.
"""

import logging
from threading import Event, Lock, Thread, current_thread

from runtools.runcore.db import EnvironmentDatabase, HEARTBEAT_STALE_AFTER
from runtools.runcore.job import STOP_OP
from runtools.runcore.run import StopReason

log = logging.getLogger(__name__)

SIGNAL_POLL_INTERVAL = 1.0        # Seconds between mailbox polls; the durability floor (doorbell is an add-on)
ORPHAN_SWEEP_INTERVAL = 60.0      # Seconds between sweeps of signals whose instance is gone
ORPHAN_SIGNAL_MAX_AGE = 60.0      # Seconds a pending signal may age before the sweep may take it


def _apply_signal(instance, signal):
    """Decode the command envelope onto the instance's public control surface.

    Transport plumbing only — this is the same call path a local caller uses, so the
    ControlRequest recording happens at the instance boundary, not here.
    """
    if signal.phase_id is None:
        if signal.op != STOP_OP:
            raise ValueError(f"Unknown instance operation: {signal.op}")
        instance.stop(StopReason[signal.args[0]] if signal.args else StopReason.STOPPED)
    else:
        control = instance.find_phase_control_by_id(signal.phase_id)
        if control is None:
            raise ValueError(f"Phase not found: {signal.phase_id}")
        getattr(control, signal.op)(*signal.args)


class PostgresInstanceAccessPoint:
    """Node-side receiving end for postgres environments — the signal reconciler.

    Coarse-polls the signals mailbox for commands targeting this node's registered instances
    and applies each at the instance's control apply point (which records it in the run and
    notifies observers); the applied row is then deleted — the durable record lives on the run
    (``JobRun.control_requests``). Un-appliable rows are logged loudly and deleted; rows whose
    target can no longer apply them (run gone, or its owner no longer heartbeating) are removed
    by a slow-cadence orphan sweep.

    Conforms to :class:`runtools.runjob.transport.InstanceAccessPoint`.
    """

    def __init__(self, db: EnvironmentDatabase):
        self._db = db
        self._instances = {}
        self._lock = Lock()
        self._stop = Event()
        self._poll_thread = None

    def start(self) -> None:
        self._poll_thread = Thread(target=self._poll_loop, name="signal-reconciler", daemon=True)
        self._poll_thread.start()

    def register_instance(self, job_instance) -> None:
        with self._lock:
            self._instances[job_instance.id] = job_instance

    def unregister_instance(self, job_instance) -> None:
        with self._lock:
            self._instances.pop(job_instance.id, None)

    def _poll_loop(self) -> None:
        next_sweep = 0.0  # First sweep on the first tick clears leftovers from prior runs
        while not self._stop.wait(SIGNAL_POLL_INTERVAL):
            try:
                self.reconcile_signals()
            except Exception:
                log.warning("Signal reconcile failed; retrying next interval", exc_info=True)
            next_sweep -= SIGNAL_POLL_INTERVAL
            if next_sweep <= 0:
                try:
                    self._sweep_orphans()
                    next_sweep = ORPHAN_SWEEP_INTERVAL
                except Exception:
                    log.warning("Orphan signal sweep failed; retrying next interval", exc_info=True)

    def reconcile_signals(self) -> None:
        """Apply and delete the pending commands for this node's instances.

        On-demand poll body — driven solely by the poll loop (single-threaded); a future
        doorbell should wake the loop, not call this concurrently.
        """
        with self._lock:
            instances = dict(self._instances)
        if not instances:
            return
        for signal in self._db.read_signals(instances.keys()):
            instance = instances.get(signal.instance_id)
            if instance is None:
                continue  # Detached since the read — leave the row to the orphan sweep
            try:
                _apply_signal(instance, signal)
            except Exception:
                log.warning("Signal rejected signal=%s", signal, exc_info=True)
            self._db.delete_signals([signal.signal_id])

    def _sweep_orphans(self) -> None:
        """Remove aged signals whose target can no longer apply them (run gone or owner dead).

        Orphan-hood is reconciler policy composed from two storage primitives: old rows come
        from the signal domain, target liveness from the run domain's version scan. A target
        counts as live only while its owner attests it with a fresh heartbeat — a crashed node's
        runs stay non-ended forever, so lifecycle state alone would keep their signals pending
        indefinitely. The age bound is only a race guard; an owner wedged past staleness that
        recovers right after a sweep loses the command, which the liveness contract accepts
        (consumers saw the run as lost the whole time, and ops are idempotent and re-sendable).
        """
        old_signals = self._db.read_signals(older_than=ORPHAN_SIGNAL_MAX_AGE)
        if not old_signals:
            return
        attested = {v.instance_id for v in self._db.active_run_versions()
                    if v.heartbeat_age is not None and v.heartbeat_age <= HEARTBEAT_STALE_AFTER}
        orphans = [s for s in old_signals if s.instance_id not in attested]
        for signal in orphans:
            log.warning("Orphan signal swept: never applied signal=%s", signal)
        self._db.delete_signals([s.signal_id for s in orphans])

    def close(self) -> None:
        self._stop.set()  # the loop exits after its current pass even without a join
        # close() may run on the poll thread itself (an observer closing the node while handling
        # a control event) and a thread cannot join itself, so skip the join in that case.
        if self._poll_thread is not None and self._poll_thread is not current_thread():
            self._poll_thread.join()
