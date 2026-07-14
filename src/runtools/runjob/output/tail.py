"""Node-side publisher of the live output tail (transport doc point 7).

Owned by the db-transport access point: at instance registration the access point subscribes
the publisher to the instance's output notifications — the unix kind already publishes
instance events through registration-time observer subscription; the postgres kind does the
same for the output tail. Staged lines are drained by the access point's poll loop — one
batched write per node per cadence, so transaction rate scales with nodes, not instances.
"""

import logging
from collections import Counter
from threading import Lock
from typing import List, Tuple

from runtools.runcore.db import OutputTailStorage
from runtools.runcore.job import InstanceID, InstanceOutputEvent, InstanceOutputObserver
from runtools.runcore.output import OutputLine

log = logging.getLogger(__name__)


class OutputTailPublisher(InstanceOutputObserver):
    """Stages the registered instances' output lines and flushes them coalesced."""

    def __init__(self, db: OutputTailStorage, cap: int):
        self._db = db
        self._cap = cap
        self._lock = Lock()  # Guards _staged and _unpruned — never held across DB I/O
        self._staged: List[Tuple[InstanceID, OutputLine]] = []
        self._unpruned = Counter()  # Lines appended per instance since its last prune

    def instance_output_update(self, event: InstanceOutputEvent):
        """Observer intake — the event carries the instance identity with the line."""
        with self._lock:
            self._staged.append((event.instance.instance_id, event.output_line))

    def finalize(self, instance_id: InstanceID):
        """Force a final prune on the instance's next (and last) flush.

        Called at instance unregistration: an ended instance produces no further output to
        trip the amortized threshold, so without this its tail could stay over cap until the
        sweep removes it entirely. Skipped when the instance has no pending work — its tail
        is already within cap.
        """
        with self._lock:
            if instance_id in self._unpruned or any(iid == instance_id for iid, _ in self._staged):
                self._unpruned[instance_id] = self._cap

    def flush(self):
        """Write the staged lines — one batched statement for the whole node.

        Single-threaded by contract (the access point's poll loop). A failed write retains
        the batch for the next tick — appends are idempotent per (instance, line ordinal), so
        re-writes are harmless. Pruning is amortized: an instance is pruned after ~``cap``
        appended lines, not on every flush; a failed prune keeps its counter and is retried
        on the next tick even when nothing new is staged (a quiet instance must not hold
        its over-cap rows until it happens to produce output again).
        """
        with self._lock:
            staged, self._staged = self._staged, []
        if staged:
            batch = self._newest_per_instance(staged)
            try:
                self._db.append_output(batch)
            except Exception:
                with self._lock:
                    self._staged[:0] = batch
                raise
        with self._lock:
            if staged:
                self._unpruned.update(iid for iid, _ in batch)
            due = [i for i, count in self._unpruned.items() if count >= self._cap]
        for instance_id in due:
            self._db.prune_output_tail(instance_id, self._cap)
            with self._lock:
                self._unpruned.pop(instance_id, None)

    def _newest_per_instance(self, staged):
        """Keep only the newest ``cap`` staged lines per instance — anything older would be
        pruned right away, so per-instance write volume is bounded at cap × flush cadence
        regardless of output rate (readers tolerate the resulting ordinal gap)."""
        counts = Counter(iid for iid, _ in staged)
        if all(count <= self._cap for count in counts.values()):
            return staged
        kept, seen = [], Counter()
        for entry in reversed(staged):
            seen[entry[0]] += 1
            if seen[entry[0]] <= self._cap:
                kept.append(entry)
        kept.reverse()
        return kept

    def close(self):
        """Best-effort final flush — the poll loop has stopped, so a failure here cannot be
        retried; it is logged rather than propagated (shutdown must not fail on it)."""
        try:
            self.flush()
        except Exception:
            log.warning("Final output tail flush failed; staged lines dropped on shutdown", exc_info=True)
