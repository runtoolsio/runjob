import copy
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from threading import Condition, Event, Lock
from typing import Any, List

from runtools.runcore import JobRun
from runtools.runcore.criteria import JobRunCriteria, PhaseCriterion, MetadataCriterion, LifecycleCriterion
from runtools.runcore.job import InstanceTransitionEvent
from runtools.runcore.run import RunState, TerminationStatus, TerminateRun, control_api, Stage
from runtools.runjob.instance import JobInstanceContext
from runtools.runjob.output import OutputContext
from runtools.runjob.phase import BasePhase, ExecutionTerminated, Phase

log = logging.getLogger(__name__)


class CoordTypes(Enum):
    APPROVAL = 'APPROVAL'
    NO_OVERLAP = 'NO_OVERLAP'
    DEPENDENCY = 'DEPENDENCY'
    WAITING = 'WAITING'
    QUEUE = 'QUEUE'


class ApprovalPhase(BasePhase[Any]):
    """
    Approval parameters (incl. timeout) + approval eval as separate objects
    TODO: parameters
    """

    def __init__(self, phase_id='approval', phase_name='Approval', *, timeout=0):
        super().__init__(phase_id, CoordTypes.APPROVAL.value, RunState.PENDING, phase_name)
        self._timeout = timeout
        self._event = Event()
        self._stopped = False

    def _run(self, _: OutputContext):
        # TODO Add support for denial request (rejection)
        log.debug("[waiting_for_approval]")

        approved = self._event.wait(self._timeout or None)
        if self._stopped:
            log.debug("[approval_cancelled]")
            raise ExecutionTerminated(TerminationStatus.STOPPED)
        if not approved:
            log.debug('[approval_timeout]')
            raise ExecutionTerminated(TerminationStatus.TIMEOUT)

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


class MutualExclusionPhase(BasePhase[JobInstanceContext]):
    """
    TODO Docs
    1. Set continue flag to be checked
    """
    EXCLUSION_ID = 'exclusion_id'

    def __init__(self, exclusion_id, protected_phase, *, phase_id=None, phase_name='Mutex Parent'):
        super().__init__(phase_id or exclusion_id, CoordTypes.NO_OVERLAP.value, RunState.EVALUATING, phase_name)
        if not exclusion_id:
            raise ValueError("Parameter `no_overlap_id` cannot be empty")
        self._exclusion_id = exclusion_id
        self._protected_phase = protected_phase
        self._attrs = {MutualExclusionPhase.EXCLUSION_ID: self._exclusion_id}
        self._excl_running_phase_filter = PhaseCriterion(
            phase_type=CoordTypes.NO_OVERLAP.value,
            attributes={MutualExclusionPhase.EXCLUSION_ID: self._exclusion_id},
            lifecycle=LifecycleCriterion(stage=Stage.RUNNING)
        )

    @property
    def children(self) -> List[Phase]:
        return [self._protected_phase]

    @property
    def exclusion_id(self):
        return self._exclusion_id

    @property
    def attributes(self):
        return self._attrs

    def _run(self, ctx: JobInstanceContext):
        log.debug("[mutex_check_started]")
        with ctx.environment.lock(f"mutex-{self.exclusion_id}"):  # TODO Manage lock names better
            c = JobRunCriteria()
            c += MetadataCriterion.all_except(ctx.metadata.instance_id)  # Excl self
            c += self._excl_running_phase_filter
            excl_runs = ctx.environment.get_active_runs(c)

            for exc_run in excl_runs:
                log.debug(f"[overlap_found]: {exc_run.metadata}")
                raise TerminateRun(TerminationStatus.OVERLAP)

            self._protected_phase.run(ctx)

    def stop(self):
        pass

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED


class DependencyPhase(BasePhase[JobInstanceContext]):

    def __init__(self, dependency_match, phase_id=None, phase_name='Active dependency check'):
        super().__init__(phase_id or str(dependency_match), CoordTypes.DEPENDENCY.value, RunState.EVALUATING,
                         phase_name)
        self._dependency_match = dependency_match

    @property
    def dependency_match(self):
        return self._dependency_match

    def _run(self, ctx):
        log.debug(f"[active_dependency_search] dependency=[{self._dependency_match}]")

        matching_runs = [r for r in ctx.environment.get_active_runs(self._dependency_match) if
                         r.instance_id != ctx.metadata.instance_id]
        if not matching_runs:
            log.debug(f"[active_dependency_not_found] dependency=[{self._dependency_match}]")
            raise TerminateRun(TerminationStatus.UNSATISFIED)
        log.debug(f"[active_dependency_found] instances={[r.instance_id for r in matching_runs]}")

    def stop(self):
        pass

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED


class WaitingPhase(BasePhase[OutputContext]):
    """
    """

    def __init__(self, phase_id, observable_conditions, timeout=0):
        super().__init__(phase_id, CoordTypes.WAITING.value, RunState.WAITING)
        self._observable_conditions = observable_conditions
        self._timeout = timeout
        self._conditions_lock = Lock()
        self._event = Event()
        self._term_status = TerminationStatus.NONE

    def _run(self, ctx):
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


@dataclass(frozen=True)
class ExecutionGroup:
    group_id: str
    max_executions: int

    def __post_init__(self):
        if self.max_executions < 1:
            raise ValueError("max_executions must be greater than 0")


class ExecutionQueue(BasePhase[JobInstanceContext]):
    GROUP_ID = "group_id"
    MAX_EXEC = "max_exec"
    STATE = "state"

    def __init__(self, execution_group, limited_phase, phase_id=None, phase_name=None):
        super().__init__(phase_id or execution_group.group_id, CoordTypes.QUEUE.value, RunState.IN_QUEUE, phase_name)
        if not execution_group:
            raise ValueError('Execution group must be specified')

        self._execution_group = execution_group
        self._limited_phase = limited_phase
        self._attrs = {
            ExecutionQueue.GROUP_ID: execution_group.group_id,
            ExecutionQueue.MAX_EXEC: execution_group.max_executions,
        }

        self._phase_filter = PhaseCriterion(
            phase_type=CoordTypes.QUEUE.value,
            attributes=self._attrs,
        )
        self._phase_filter_running = copy.copy(self._phase_filter)
        self._phase_filter_running.lifecycle = LifecycleCriterion(stage=Stage.RUNNING)

        self._queue_change_condition = Condition()
        # vv Guarding these fields vv
        self._state = QueuedState.NONE
        self._queue_changed = False

    def _lock_name(self):
        return f"eq-{self.execution_group}.lock"

    @property
    def children(self) -> List[Phase]:
        return [self._limited_phase]

    @property
    def attributes(self):
        return self._attrs

    @property
    def variables(self):
        return {ExecutionQueue.STATE: self._state.name}

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED

    @property
    @control_api
    def state(self):
        return self._state

    @property
    @control_api
    def execution_group(self):
        return self._execution_group

    def _run(self, ctx):
        try:
            ctx.environment.add_observer_transition(self._new_instance_transition)

            with self._queue_change_condition:
                # Transition under lock to prevent NONE -> CANCELLED -> IN_QUEUE race condition
                if self._state == QueuedState.NONE:
                    self._state = QueuedState.IN_QUEUE

            while True:
                with self._queue_change_condition:
                    if self._state == QueuedState.CANCELLED:
                        return
                    if self._state == QueuedState.DISPATCHED:
                        break

                    if not self._queue_changed:
                        self._queue_change_condition.wait()
                        continue

                    self._queue_changed = False

                with ctx.environment.lock(self._lock_name()):
                    self._dispatch_next(ctx)
        finally:
            ctx.environment.remove_observer_transition(self._new_instance_transition)

        self._limited_phase.run(ctx)

    def stop(self):
        with self._queue_change_condition:
            if self._state.dequeued:
                self._limited_phase.stop()
                return

            self._state = QueuedState.CANCELLED
            self._queue_change_condition.notify_all()

    @control_api
    def signal_dispatch(self):
        with self._queue_change_condition:
            if self._state.dequeued:
                return False

            self._state = QueuedState.DISPATCHED
            self._queue_change_condition.notify_all()
            return True

    def _dispatch_next(self, ctx):
        criteria = JobRunCriteria(
            metadata_criteria=MetadataCriterion.all_except(ctx.metadata.instance_id),
            phase_criteria=self._phase_filter_running
        )
        runs: List[JobRun] = ctx.get_active_runs(criteria)

        runs_sorted = sorted(runs, key=lambda run: run.find_phase(self._phase_filter).lifecycle.created_at)
        dispatched = {r for r in runs_sorted if
                      r.find_phase(self._phase_filter).variables[ExecutionQueue.STATE] == QueuedState.DISPATCHED.name}
        free_slots = self._execution_group.max_executions - len(dispatched)
        if free_slots <= 0:
            log.debug("event[exec_limit_reached] slots=[%d] dispatched=[%d]",
                      self._execution_group.max_executions, len(dispatched))
            return False

        log.debug("event[dispatching_from_queue] count=[%d]", free_slots)
        for next_dispatch in runs_sorted:
            if next_dispatch in dispatched:
                continue
            dispatched = (
                ctx.get_instance(next_dispatch.instance_id).find_phase_control(self._phase_filter).signal_dispatch())
            if dispatched:
                log.debug("event[dispatched] run=[%s]", next_dispatch.metadata)
                free_slots -= 1
                if free_slots <= 0:
                    return

    def _new_instance_transition(self, event: InstanceTransitionEvent):
        with self._queue_change_condition:
            if not self._queue_changed or event.new_stage != Stage.ENDED or not self._phase_filter(
                    event.job_run.find_phase_by_id(event.phase_id)):
                return

            self._queue_changed = True  # Run slot freed
            self._queue_change_condition.notify()
