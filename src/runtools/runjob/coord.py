import logging
from dataclasses import dataclass
from enum import Enum, auto
from threading import Condition, Event, Lock
from typing import Any

from runtools.runcore.matching import JobRunCriteria, PhaseCriterion
from runtools.runcore.job import InstancePhaseEvent
from runtools.runcore.run import TerminationStatus, control_api, Stage
from runtools.runjob.instance import JobInstanceContext
from runtools.runjob.phase import BasePhase, PhaseTerminated, SequentialPhase

log = logging.getLogger(__name__)


class CoordTypes(Enum):
    CHECKPOINT = 'CHECKPOINT'
    APPROVAL = 'APPROVAL'
    MUTEX = 'MUTEX'
    DEPENDENCY = 'DEPENDENCY'
    AWAIT = 'AWAIT'
    QUEUE = 'QUEUE'


# Lock IDs are a cross-process protocol: nodes exclude each other only when they compute
# identical IDs, so each format is defined exactly once here. Prefixes are framework-reserved.

def _mutex_lock_id(exclusion_group):
    return f"mutex-{exclusion_group}"


def _queue_slot_lock_id(group_id, slot):
    return f"eq-{group_id}#{slot}"


class CheckpointPhase(BasePhase[Any]):
    """
    A strict barrier that must be resumed for the pipeline to continue.

    - Leaf node (no children)
    - Timeout stops the parent sequence
    - Use when: "If this checkpoint isn't passed, abort the whole operation."
    """

    def __init__(self, phase_id, *, timeout=0):
        super().__init__(phase_id, CoordTypes.CHECKPOINT.value)
        self._timeout = timeout
        self._event = Event()

    def _run(self, _):
        log.debug("Checkpoint waiting", extra={"phase": self.id})
        resumed = self._event.wait(self._timeout or None)
        self._raise_if_stopped()
        if not resumed:
            log.debug("Checkpoint timeout", extra={"phase": self.id})
            raise PhaseTerminated(TerminationStatus.TIMEOUT)
        log.debug("Checkpoint resumed", extra={"phase": self.id})

    @property
    def is_idle(self):
        return self.stage == Stage.RUNNING and not self._event.is_set()

    @control_api
    def resume(self):
        self._event.set()

    @property
    def is_resumed(self):
        return self._event.is_set() and not self._stop_reason

    def _stop_running(self, reason):
        self._event.set()


class ApprovalPhase(BasePhase[Any]):
    """
    A conditional scope that runs its child only if approved.

    - Container with single child
    - Rejection or timeout skips the child (SKIPPED status) but pipeline continues
    - Use when: "If not approved, skip this part but keep going."
    """

    def __init__(self, phase_id, protected_phase, *, timeout=0):
        super().__init__(phase_id, CoordTypes.APPROVAL.value, [protected_phase])
        self._timeout = timeout
        self._event = Event()

    def _run(self, ctx):
        log.debug("Approval waiting", extra={"phase": self.id})
        approved = self._event.wait(self._timeout or None)
        self._raise_if_stopped()
        if not approved:
            log.debug("Approval skipped", extra={"phase": self.id})
            raise PhaseTerminated(TerminationStatus.SKIPPED, "TODO - can be passed via approve method")

        log.debug("Approval granted", extra={"phase": self.id})
        self.run_child(self._children[0])

    @property
    def is_idle(self):
        return self.stage == Stage.RUNNING and not self._event.is_set()

    @control_api
    def approve(self):
        self._event.set()

    @property
    def approved(self):
        return self._event.is_set() and not self._stop_reason

    def _stop_running(self, reason):
        self._event.set()


class MutualExclusionPhase(BasePhase[JobInstanceContext]):
    """
    A phase that prevents concurrent execution of protected phases within the same exclusion group.

    Claim-then-act: the group lock is claimed without waiting when the phase runs and held for
    the protected child's whole run — the claim's success is the overlap decision, so no state
    is read and nothing can be stale, and a crashed holder's claim is released by the lock
    medium. If the group lock is already held elsewhere, this phase terminates with OVERLAP.
    """

    def __init__(self, phase_id, protected_phase, *, exclusion_group=None):
        super().__init__(phase_id, CoordTypes.MUTEX.value, [protected_phase])
        self._exclusion_group = exclusion_group
        self._attrs = {'exclusion_group': self._exclusion_group}

    @property
    def exclusion_group(self):
        return self._exclusion_group

    @property
    def attributes(self):
        return self._attrs

    def _run(self, ctx: JobInstanceContext):
        excl_group = self.exclusion_group or ctx.metadata.job_id
        mutex_lock = ctx.environment.lock(_mutex_lock_id(excl_group))
        if not mutex_lock.try_acquire():
            log.warning("Mutex overlap found", extra={"group": excl_group})
            raise PhaseTerminated(TerminationStatus.OVERLAP)
        try:
            self.run_child(self._children[0])
        finally:
            mutex_lock.release()


class DependencyPhase(BasePhase[JobInstanceContext]):
    """
    A phase that checks if a required dependency is currently active.

    If no active runs match the dependency criteria (excluding self), this phase
    terminates with UNSATISFIED status. Otherwise, it completes successfully.
    """

    def __init__(self, dependency_match, phase_id=None):
        super().__init__(phase_id or str(dependency_match), CoordTypes.DEPENDENCY.value)
        self._dependency_match = dependency_match

    @property
    def dependency_match(self):
        return self._dependency_match

    def _run(self, ctx):
        log.debug("Searching active dependency", extra={"match": str(self._dependency_match)})

        matching_runs = [r for r in ctx.environment.get_active_runs(self._dependency_match) if
                         r.instance_id != ctx.metadata.instance_id]
        if not matching_runs:
            log.debug("Active dependency not found", extra={"match": str(self._dependency_match)})
            raise PhaseTerminated(TerminationStatus.UNSATISFIED)
        log.debug("Active dependency found", extra={"instances": str([r.instance_id for r in matching_runs])})


class AwaitPhase(BasePhase[JobInstanceContext]):
    """A phase that waits until each given criterion is satisfied by a matching run in the environment."""

    def __init__(self, *criteria: JobRunCriteria, timeout=0, phase_id=None):
        super().__init__(phase_id or 'await', CoordTypes.AWAIT.value)
        self._criteria = criteria
        self._timeout = timeout
        self._watcher = None
        self._watcher_lock = Lock()

    def _run(self, ctx):
        with self._watcher_lock:
            self._raise_if_stopped()
            self._watcher = ctx.environment.watcher(*self._criteria, search_past=True)

        if not self._watcher.wait(timeout=self._timeout or None):
            self._raise_if_stopped()
            if self._watcher.is_timed_out:
                raise PhaseTerminated(TerminationStatus.TIMEOUT)
            raise PhaseTerminated(TerminationStatus.UNSATISFIED)

    def _stop_running(self, reason):
        with self._watcher_lock:
            if self._watcher:
                self._watcher.cancel()


class QueuedState(Enum):
    NONE = auto(), False
    IN_QUEUE = auto(), False
    DISPATCHED = auto(), True
    CANCELLED = auto(), True

    def __new__(cls, value, dequeued):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.dequeued = dequeued
        return obj


@dataclass(frozen=True)
class ConcurrencyGroup:
    group_id: str
    max_executions: int

    def __post_init__(self):
        if self.max_executions < 1:
            raise ValueError("max_executions must be greater than 0")


class ExecutionQueue(BasePhase[JobInstanceContext]):
    """
    A phase that limits concurrent execution of its child phase across instances in the same concurrency group.

    Claim-then-act: capacity is enforced by ``max_executions`` slot locks per group. An instance
    entering this phase repeatedly tries to claim any free slot, holds the claimed slot for the
    child's whole run, and releases it after — the claim's success is the dispatch decision, so
    no shared state is read and nothing can be stale, and a crashed holder's slot is released by
    the lock medium.

    Dispatch order is claim order softened by seniority-staggered claims: before claiming, an
    instance counts the visibly older queued instances of its group and delays its attempt
    proportionally, so older waiters get first pick after each wake-up. This is politeness, not
    protocol — the view may be stale (order inversions possible, capacity violations not), and
    a stale entry (e.g. from a dead node) only delays claims by one stagger step, never blocks
    them.

    Queue state machine (guarded by ``_queue_change_condition``)::

        NONE ──→ IN_QUEUE ──→ DISPATCHED
                    │
                    └──→ CANCELLED

    - NONE → IN_QUEUE: On entry to ``_run``, before the claim loop starts.
    - IN_QUEUE → DISPATCHED: When this instance claims a free slot.
    - IN_QUEUE → CANCELLED: When ``stop()`` is called before a slot is claimed.

    Wake-ups:
        An observer on environment phase transitions detects when a QUEUE phase in the same group
        ends (its slot is freed), setting ``_queue_changed`` and waking the claim loop for a retry.
        A periodic rescan timeout (default 30s) provides a fallback in case a notification is
        missed — a spurious or late wake-up is just a failed claim attempt.
    """

    GROUP_ID = "group_id"
    MAX_EXEC = "max_exec"
    STATE = "state"
    CLAIM_STAGGER_INTERVAL = 0.1  # Seconds of claim delay per visibly older queued instance

    def __init__(self, phase_id, concurrency_group, limited_phase, queue_rescan_timeout=30):
        super().__init__(phase_id or concurrency_group.group_id, CoordTypes.QUEUE.value, [limited_phase])
        if not concurrency_group:
            raise ValueError('Concurrency group must be specified')

        self._group = concurrency_group
        self._attrs = {
            ExecutionQueue.GROUP_ID: concurrency_group.group_id,
            ExecutionQueue.MAX_EXEC: concurrency_group.max_executions,
        }

        self._phase_filter = PhaseCriterion(
            phase_type=CoordTypes.QUEUE.value,
            attributes=self._attrs,
        )

        self._queue_change_condition = Condition()
        self._rescan_timeout = queue_rescan_timeout
        # vv Guarding these fields vv
        self._state = QueuedState.NONE
        self._queue_changed = True


    @property
    def attributes(self):
        return self._attrs

    @property
    def variables(self):
        return {ExecutionQueue.STATE: self._state.name}

    @property
    def state(self):
        return self._state

    @property
    def execution_group(self):
        return self._group

    def _run(self, ctx):
        slot_lock = None
        try:
            ctx.environment.notifications.add_observer_phase(self._instance_phase_update)

            with self._queue_change_condition:
                # Transition under lock to prevent NONE -> CANCELLED -> IN_QUEUE race condition
                if self._state == QueuedState.NONE:
                    self._state = QueuedState.IN_QUEUE

            while True:
                with self._queue_change_condition:
                    if self._state == QueuedState.CANCELLED:
                        self._raise_if_stopped()

                    if not self._queue_changed:
                        if self._queue_change_condition.wait(timeout=self._rescan_timeout):
                            continue

                    self._queue_changed = False

                rank = self._claim_rank(ctx)
                if rank:
                    # Stagger by seniority so older waiters get first pick after a wake-up;
                    # an early notify (stop, slot freed) just advances the attempt
                    with self._queue_change_condition:
                        self._queue_change_condition.wait(timeout=rank * ExecutionQueue.CLAIM_STAGGER_INTERVAL)

                slot_lock = self._try_claim_slot(ctx)
                if slot_lock:
                    with self._queue_change_condition:
                        if self._state == QueuedState.CANCELLED:
                            # Stopped between the claim and the transition -> give the slot back
                            slot_lock.release()
                            slot_lock = None
                            self._raise_if_stopped()
                        self._state = QueuedState.DISPATCHED
                    break
        finally:
            ctx.environment.notifications.remove_observer_phase(self._instance_phase_update)

        try:
            self.run_child(self._children[0])
        finally:
            slot_lock.release()

    def _try_claim_slot(self, ctx):
        """Try to claim any free slot of the concurrency group. Returns the held lock or None."""
        for slot in range(self._group.max_executions):
            slot_lock = ctx.environment.lock(_queue_slot_lock_id(self._group.group_id, slot))
            if slot_lock.try_acquire():
                log.debug("Queue slot claimed", extra={"group": self._group.group_id, "slot": slot})
                return slot_lock
        return None

    def _claim_rank(self, ctx):
        """Soft ordering: the number of visibly older queued instances of the group.

        The rank staggers claim attempts (oldest first). Politeness only — the observation view
        may be stale, so this can invert order but never break capacity, and a stale entry only
        delays a claim by one stagger step, never blocks it.
        """
        runs = ctx.environment.get_active_runs(JobRunCriteria(phase_criteria=(self._phase_filter,)))
        rank = 0
        for run in runs:
            if run.instance_id == ctx.metadata.instance_id:
                continue
            queue_phase = run.find_first_phase(self._phase_filter)
            if (queue_phase.variables.get(ExecutionQueue.STATE) == QueuedState.IN_QUEUE.name
                    and queue_phase.lifecycle.created_at < self.created_at):
                rank += 1
        if rank:
            log.debug("Deferring claim to older queued instances",
                      extra={"group": self._group.group_id, "older_count": rank})
        return rank

    def _stop_running(self, reason):
        with self._queue_change_condition:
            if self._state.dequeued:
                return

            self._state = QueuedState.CANCELLED
            self._queue_change_condition.notify_all()

    def _instance_phase_update(self, event: InstancePhaseEvent):
        with self._queue_change_condition:
            if self._queue_changed or event.new_stage != Stage.ENDED:
                return

            ended_phase = event.job_run.find_phase_by_id(event.phase_id)
            if not ended_phase or not self._phase_filter(ended_phase):
                return

            log.debug("Queue slot freed", extra={"instance": str(event.instance.instance_id), "phase": event.phase_id})
            self._queue_changed = True
            self._queue_change_condition.notify()


def checkpoint(decor=None, *, timeout=0):
    """Decorator that adds a checkpoint gate before a @phase function.

    Usage::

        @checkpoint
        @phase
        def deploy(env):
            ...

        @checkpoint(timeout=30)
        @phase
        def deploy(env):
            ...
    """
    def apply(d):
        original = d.create_phase

        def wrapped_create(*args, **kwargs):
            fn_phase = original(*args, **kwargs)
            cp = CheckpointPhase(f'{fn_phase.id}_checkpoint', timeout=timeout)
            return SequentialPhase(f'{fn_phase.id}_seq', [cp, fn_phase])

        d.create_phase = wrapped_create
        return d

    if decor is not None:
        return apply(decor)
    return apply


def approval(decor=None, *, timeout=0):
    """Decorator that wraps a @phase function with ApprovalPhase.

    Usage::

        @approval
        @phase
        def deploy(env):
            ...

        @approval(timeout=60)
        @phase
        def deploy(env):
            ...
    """
    def apply(d):
        original = d.create_phase

        def wrapped_create(*args, **kwargs):
            fn_phase = original(*args, **kwargs)
            return ApprovalPhase(f'{fn_phase.id}_approval', fn_phase, timeout=timeout)

        d.create_phase = wrapped_create
        return d

    if decor is not None:
        return apply(decor)
    return apply


def mutex(decor=None, *, group=None):
    """Decorator that wraps a @phase function with MutualExclusionPhase.

    Usage::

        @mutex
        @phase
        def deploy():
            ...

        @mutex(group="deploys")
        @phase
        def deploy():
            ...
    """
    def apply(d):
        original = d.create_phase

        def wrapped_create(*args, **kwargs):
            fn_phase = original(*args, **kwargs)
            return MutualExclusionPhase(f'{fn_phase.id}_mutex', fn_phase, exclusion_group=group)

        d.create_phase = wrapped_create
        return d

    if decor is not None:
        return apply(decor)
    return apply


def queue(*, max_concurrent, group=None):
    """Decorator that wraps a @phase function with ExecutionQueue.

    Usage::

        @queue(max_concurrent=2)
        @phase
        def deploy():
            ...

        @queue(group="deploys", max_concurrent=3)
        @phase
        def deploy():
            ...
    """
    def apply(d):
        original = d.create_phase

        def wrapped_create(*args, **kwargs):
            fn_phase = original(*args, **kwargs)
            grp = ConcurrencyGroup(group or fn_phase.id, max_concurrent)
            return ExecutionQueue(f'{fn_phase.id}_queue', grp, fn_phase)

        d.create_phase = wrapped_create
        return d

    return apply
