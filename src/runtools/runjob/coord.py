import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from threading import Condition, Event, Lock
from typing import Optional, Any

from runtools import runcore
from runtools.runcore import paths
from runtools.runcore.criteria import JobRunCriteria, PhaseCriterion, MetadataCriterion, negate_id
from runtools.runcore.job import JobRun, JobRuns, InstanceTransitionObserver
from runtools.runcore.listening import InstanceTransitionReceiver
from runtools.runcore.run import RunState, TerminationStatus, PhaseRun, TerminateRun, control_api, Phase
from runtools.runcore.util import lock
from runtools.runjob.instance import JobInstanceContext
from runtools.runjob.output import OutputContext

log = logging.getLogger(__name__)


class CoordTypes(Enum):
    APPROVAL = 'APPROVAL'
    NO_OVERLAP = 'NO_OVERLAP'
    DEPENDENCY = 'DEPENDENCY'
    WAITING = 'WAITING'
    QUEUE = 'QUEUE'


class ApprovalPhase(Phase[Any]):
    """
    Approval parameters (incl. timeout) + approval eval as separate objects
    TODO: parameters
    """

    def __init__(self, phase_id='approval', phase_name='Approval', *, timeout=0):
        self._id = phase_id
        self._name = phase_name
        self._timeout = timeout
        self._event = Event()
        self._stopped = False

    @property
    def id(self):
        return self._id

    @property
    def type(self) -> str:
        return CoordTypes.APPROVAL.value

    @property
    def run_state(self) -> RunState:
        return RunState.PENDING

    @property
    def name(self) -> Optional[str]:
        return self._name

    def run(self, ctx: OutputContext):
        # TODO Add support for denial request (rejection)
        log.debug("[waiting_for_approval]")

        approved = self._event.wait(self._timeout or None)
        if self._stopped:
            log.debug("[approval_cancelled]")
            return
        if not approved:
            log.debug('[approval_timeout]')
            raise TerminateRun(TerminationStatus.TIMEOUT)

        log.debug("[approved]")

    @control_api
    def approve(self):
        self._event.set()

    @control_api
    @property
    def approved(self):
        return self._event.is_set() and not self._stopped

    def stop(self):
        self._stopped = True
        self._event.set()

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED


class MutualExclusionPhase(Phase[JobInstanceContext]):
    """
    TODO Docs
    1. Set continue flag to be checked
    """
    EXCLUSION_ID = 'exclusion_id'
    UNTIL_PHASE = 'until_phase'

    def __init__(self, exclusion_id, phase_id=None, phase_name='Mutual Exclusion Check', *, until_phase=None):
        if not exclusion_id:
            raise ValueError("Parameter `no_overlap_id` cannot be empty")
        self._id = phase_id or exclusion_id
        self._name = phase_name
        self._exclusion_id = exclusion_id
        self._until_phase = until_phase
        self._attrs = {
            MutualExclusionPhase.EXCLUSION_ID: self._exclusion_id,
            MutualExclusionPhase.UNTIL_PHASE: self._until_phase,
        }
        attr_to_match = {MutualExclusionPhase.EXCLUSION_ID: self._exclusion_id}
        self._excl_phase_filter = PhaseCriterion(phase_type=CoordTypes.NO_OVERLAP.value, attributes=attr_to_match)

    @property
    def id(self):
        return self._id

    @property
    def type(self) -> str:
        return CoordTypes.NO_OVERLAP.value

    @property
    def run_state(self) -> RunState:
        return RunState.EVALUATING

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def exclusion_id(self):
        return self._exclusion_id

    @property
    def attributes(self):
        return self._attrs

    def _is_in_exclusion_phase(self, job_run: JobRun) -> bool:
        """
        Checks if the given job run is currently between its exclusion phase and until phase.

        Args:
            job_run: The job run to check

        Returns:
            True if the current phase is between the exclusion phase and its target phase (inclusive),
            False otherwise
        """
        exclusion_phase = job_run.find_phase(self._excl_phase_filter)
        if not exclusion_phase:
            return False

        until_phase_id = exclusion_phase.attributes.get(MutualExclusionPhase.UNTIL_PHASE)
        if not until_phase_id:
            next_phase = job_run.phase_after(exclusion_phase)
            if not next_phase:
                # TODO event to tracker
                return False
            until_phase_id = next_phase.phase_id

        current_phase = job_run.current_phase
        if not current_phase:
            return False

        until_phase = job_run.find_phase(PhaseCriterion(phase_id=until_phase_id))
        if not until_phase:
            # TODO event to tracker
            return False

        try:
            phases = job_run.phases
            excl_idx = phases.index(exclusion_phase)
            until_idx = phases.index(until_phase)
            current_idx = phases.index(current_phase)

            return excl_idx <= current_idx <= until_idx
        except ValueError:
            return False

    def run(self, ctx: JobInstanceContext):
        op = ctx.status_tracker.operation("No overlap check")

        with ctx.environment.locker(f"mutex-{self.exclusion_id}")():
            c = JobRunCriteria()
            c += MetadataCriterion(instance_id=negate_id(ctx.metadata.instance_id))  # Excl self
            c += self._excl_phase_filter
            runs, _ = runcore.get_active_runs(c)

            for run in runs:
                if self._is_in_exclusion_phase(run):
                    op.finished(f"Overlap found: {run.metadata}")
                    raise TerminateRun(TerminationStatus.OVERLAP)

        op.finished("No overlap found")

    def stop(self):
        pass

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED


class DependencyPhase(Phase[JobInstanceContext]):

    def __init__(self, dependency_match, phase_id=None, phase_name='Active dependency check'):
        self._id = phase_id or str(dependency_match)
        self._name = phase_name
        self._dependency_match = dependency_match

    @property
    def id(self):
        return self._id

    @property
    def type(self) -> str:
        return CoordTypes.DEPENDENCY.value

    @property
    def run_state(self) -> RunState:
        return RunState.EVALUATING

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def dependency_match(self):
        return self._dependency_match

    def run(self, ctx):
        op = ctx.status_tracker.operation("Dependency check")

        matching_runs, _ = runcore.get_active_runs(self._dependency_match)
        if not matching_runs:
            op.finished(f"Required dependency `{self._dependency_match}` not found")
            raise TerminateRun(TerminationStatus.UNSATISFIED)
        op.finished(f"Required dependency `{self._dependency_match}` found")

    def stop(self):
        pass

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED


class WaitingPhase(Phase[OutputContext]):
    """
    """

    def __init__(self, phase_id, observable_conditions, timeout=0):
        self._id = phase_id
        self._observable_conditions = observable_conditions
        self._timeout = timeout
        self._conditions_lock = Lock()
        self._event = Event()
        self._term_status = TerminationStatus.NONE

    @property
    def id(self):
        return self._id

    @property
    def type(self) -> str:
        return CoordTypes.WAITING.value

    @property
    def run_state(self) -> RunState:
        return RunState.WAITING

    def run(self, ctx):
        for condition in self._observable_conditions:
            condition.add_result_listener(self._result_observer)
            condition.start_evaluating()

        resolved = self._event.wait(self._timeout or None)
        if not resolved:
            self._term_status = TerminationStatus.TIMEOUT

        self._stop_all()
        if self._term_status:
            raise TerminateRun(self._term_status)

    def _result_observer(self, *_):
        wait = False
        with self._conditions_lock:
            for condition in self._observable_conditions:
                if not condition.result:
                    wait = True
                elif not condition.result.success:
                    self._term_status = TerminationStatus.UNSATISFIED
                    wait = False
                    break

        if not wait:
            self._event.set()

    def stop(self):
        self._stop_all()
        self._event.set()

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED

    def _stop_all(self):
        for condition in self._observable_conditions:
            condition.stop()


class ConditionResult(Enum):
    """
    Enum representing the result of a condition evaluation.

    Attributes:
        NONE: The condition has not been evaluated yet.
        SATISFIED: The condition is satisfied.
        UNSATISFIED: The condition is not satisfied.
        EVALUATION_ERROR: The condition could not be evaluated due to an error in the evaluation logic.
    """
    NONE = (auto(), False)
    SATISFIED = (auto(), True)
    UNSATISFIED = (auto(), False)
    EVALUATION_ERROR = (auto(), False)

    def __new__(cls, value, success):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.success = success
        return obj

    def __bool__(self):
        return self != ConditionResult.NONE


class ObservableCondition(ABC):
    """
    Abstract base class representing a (child) waiter associated with a specific (parent) pending object.

    A waiter is designed to be held by a job instance, enabling the job to enter its waiting phase
    before actual execution. This allows for synchronization between different parts of the system.
    Depending on the parent waiting, the waiter can either be manually released, or all associated
    waiters can be released simultaneously when the main condition of the waiting is met.

    TODO:
    1. Add notifications to this class
    """

    @abstractmethod
    def start_evaluation(self) -> None:
        """
        Instructs the waiter to begin waiting on its associated condition.

        When invoked by a job instance, the job enters its pending phase, potentially waiting for
        the overarching pending condition to be met or for a manual release.
        """
        pass

    @property
    @abstractmethod
    def result(self):
        """
        Returns:
            ConditionResult: The result of the evaluation or NONE if not yet evaluated.
        """
        pass

    @abstractmethod
    def add_result_listener(self, listener):
        pass

    @abstractmethod
    def remove_result_listener(self, listener):
        pass

    def stop(self):
        pass


class Queue:

    @abstractmethod
    def create_waiter(self, job_instance):
        pass


class QueuedState(Enum):
    NONE = auto(), False
    IN_QUEUE = auto(), False
    DISPATCHED = auto(), True
    CANCELLED = auto(), True
    UNKNOWN = auto(), False

    def __new__(cls, value, dequeued):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.dequeued = dequeued
        return obj

    @classmethod
    def from_str(cls, value: str):
        try:
            return cls[value.upper()]
        except KeyError:
            return cls.UNKNOWN


@dataclass
class ExecutionGroupLimit:
    group: str
    max_executions: int


class ExecutionQueue(Phase[OutputContext], InstanceTransitionObserver):

    def __init__(self, queue_id, max_executions, phase_id=None, phase_name=None, *,
                 until_phase=None,
                 locker_factory=lock.default_locker_factory(),
                 state_receiver_factory=InstanceTransitionReceiver):
        if not queue_id:
            raise ValueError('Queue ID must be specified')
        if max_executions < 1:
            raise ValueError('Max executions must be greater than zero')

        self._id = phase_id or queue_id
        self._name = phase_name
        self._state = QueuedState.NONE
        self._queue_id = queue_id
        self._until_phase = until_phase
        self._max_executions = max_executions
        self._locker = locker_factory(paths.lock_path(f"eq-{queue_id}.lock", True))
        self._state_receiver_factory = state_receiver_factory
        self._wait_guard = Condition()
        # vv Guarding these fields vv
        self._current_wait = False
        self._state_receiver = None

    @property
    def id(self):
        return self._id

    @property
    def type(self) -> str:
        return CoordTypes.QUEUE.value

    @property
    def run_state(self) -> RunState:
        return RunState.IN_QUEUE

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED

    @property
    def state(self):
        return self._state

    @property
    def queue_id(self):
        return self._queue_id

    def run(self, ctx):
        with self._wait_guard:
            if self._state == QueuedState.NONE:
                self._state = QueuedState.IN_QUEUE

        while True:
            with self._wait_guard:
                if self._state.dequeued:
                    return

                if self._current_wait:
                    self._wait_guard.wait()
                    continue

                self._current_wait = True
                self._start_listening()

            with self._locker():
                self._dispatch_next()

    def stop(self):
        with self._wait_guard:
            if self._state.dequeued:
                return

            self._state = QueuedState.CANCELLED
            self._stop_listening()
            self._wait_guard.notify_all()

    def signal_dispatch(self):
        with self._wait_guard:
            if self._state == QueuedState.CANCELLED:
                return False

            if self._state.dequeued:
                return True  # TODO Safe to keep?

            self._state = QueuedState.DISPATCHED
            self._wait_guard.notify_all()
            return True

    def _start_listening(self):
        self._state_receiver = self._state_receiver_factory()
        self._state_receiver.add_observer_transition(self)
        self._state_receiver.start()

    def _dispatch_next(self):
        phase_filter = PhaseCriterion(phase_type=CoordTypes.QUEUE, protection_id=self._queue_id)
        criteria = JobRunCriteria(phase_criteria=phase_filter)
        runs, _ = runcore.get_active_runs(criteria)

        # TODO Sort by phase start
        sorted_group_runs = JobRuns(sorted(runs, key=lambda job_run: job_run.lifecycle.created_at))
        occupied = len(
            [r for r in sorted_group_runs
             if r.in_protected_phase(CoordTypes.QUEUE, self._queue_id)
             or (r.current_phase_id.phase_type == CoordTypes.QUEUE and r.current_phase_id.queued_state.dequeued)])
        free_slots = self._max_executions - occupied
        if free_slots <= 0:
            # self._log.debug("event[no_dispatch] slots=[%d] occupied=[%d]", self._max_executions, occupied)
            return False

        # self._log.debug("event[dispatching] free_slots=[%d]", free_slots)
        for next_proceed in sorted_group_runs.queued:
            signal_resp = runcore.signal_dispatch(JobRunCriteria.exact_match(next_proceed), self._queue_id)
            for r in signal_resp.successful:
                if r.dispatched:
                    # self._log.debug("event[dispatched] run=[%s]", next_proceed.metadata)
                    free_slots -= 1
                    if free_slots <= 0:
                        return

    def new_instance_phase(self, job_run: JobRun, previous_phase: PhaseRun, new_phase: PhaseRun, ordinal: int):
        with self._wait_guard:
            if not self._current_wait:
                return
            if (protected_phases := job_run.protected_phases(CoordTypes.QUEUE, self._queue_id)) \
                    and previous_phase.phase_id in protected_phases \
                    and new_phase not in protected_phases:
                # Run slot freed
                self._current_wait = False
                self._stop_listening()
                self._wait_guard.notify()

    def _stop_listening(self):
        self._state_receiver.close()
        self._state_receiver.remove_observer_transition(self)
        self._state_receiver = None
