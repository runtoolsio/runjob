import copy
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from threading import Condition, Event, Lock
from typing import Any, List, Optional

from runtools.runcore.criteria import JobRunCriteria, PhaseCriterion, MetadataCriterion, LifecycleCriterion
from runtools.runcore.job import JobRun, InstanceTransitionEvent
from runtools.runcore.run import TerminationStatus, control_api, Stage
from runtools.runjob.instance import JobInstanceContext
from runtools.runjob.output import OutputContext
from runtools.runjob.phase import BasePhase, PhaseTerminated

log = logging.getLogger(__name__)


class CoordTypes(Enum):
    APPROVAL = 'APPROVAL'
    MUTEX = 'MUTEX'
    DEPENDENCY = 'DEPENDENCY'
    WAITING = 'WAITING'
    QUEUE = 'QUEUE'


class ApprovalPhase(BasePhase[Any]):
    """
    Approval parameters (incl. timeout) + approval eval as separate objects
    TODO: parameters
    """

    def __init__(self, phase_id, phase_name=None, *, children=(), timeout=0):
        super().__init__(phase_id, CoordTypes.APPROVAL.value, phase_name, children)
        self._timeout = timeout
        self._event = Event()

    def _run(self, _):
        # TODO Add support for denial request (rejection)
        log.info("waiting_for_approval phase=[%s]", self.id)

        approved = self._event.wait(self._timeout or None)
        if self._stop_reason:
            raise PhaseTerminated(self._stop_reason.termination_status)
        if not approved:
            raise PhaseTerminated(TerminationStatus.TIMEOUT)

        log.info("approved phase=[%s]", self.id)
        self.run_children()

    @control_api
    @property
    def is_idle(self):
        return self.stage == Stage.RUNNING and not self._event.is_set()

    @control_api
    def approve(self):
        self._event.set()

    @control_api
    @property
    def approved(self):
        return self._event.is_set() and not self._stop_reason

    def _stop_started_run(self, reason):
        self._event.set()


class MutualExclusionPhase(BasePhase[JobInstanceContext]):
    """
    A phase that prevents concurrent execution of protected phases within the same exclusion group.

    If another instance is already running a MutualExclusionPhase with the same exclusion group,
    this phase terminates with OVERLAP status. Otherwise, it proceeds to run the protected child phase.

    Note: Current implementation doesn't implement a fair strategy for phases executed at nearly the same time.
    """
    running_mutex_filter = PhaseCriterion(
        phase_type=CoordTypes.MUTEX.value,
        lifecycle=LifecycleCriterion(stage=Stage.RUNNING)
    )

    def __init__(self, phase_id, protected_phase, *, exclusion_group=None, phase_name=None):
        super().__init__(phase_id, CoordTypes.MUTEX.value, phase_name, [protected_phase])
        self._exclusion_group = exclusion_group
        self._attrs = {'exclusion_group': self._exclusion_group}

    @property
    @control_api
    def exclusion_group(self):
        return self._exclusion_group

    @property
    def attributes(self):
        return self._attrs

    @staticmethod
    def _excl_running_job_filter(ctx):
        return JobRunCriteria(
            metadata_criteria=MetadataCriterion.all_except(ctx.metadata.instance_id),  # Excl self
            phase_criteria=MutualExclusionPhase.running_mutex_filter
        )

    def _run(self, ctx: JobInstanceContext):
        excl_group = self.exclusion_group or ctx.metadata.job_id
        log.debug("[mutex_check_started] exclusion_group=[%s]", excl_group)
        with ctx.environment.lock(f"mutex-{excl_group}"):  # TODO Manage lock names better
            excl_runs: List[JobRun] = ctx.environment.get_active_runs(self._excl_running_job_filter(ctx))
            for excl_run in excl_runs:
                for mutex_phase in excl_run.search_phases(MutualExclusionPhase.running_mutex_filter):
                    phase_excl_group = mutex_phase.attributes.get('exclusion_group') or excl_run.job_id
                    if phase_excl_group == excl_group:
                        log.warning(f"mutex_overlap_found instance=[{excl_run.instance_id}] "
                                    f"phase=[{mutex_phase.phase_id}] exclusion_group=[{excl_group}]")
                        raise PhaseTerminated(TerminationStatus.OVERLAP)  # TODO Race-condition - set flag before raise

        self.run_child(self._children[0])

    def _stop_started_run(self, reason):
        pass


class DependencyPhase(BasePhase[JobInstanceContext]):

    def __init__(self, dependency_match, phase_id=None, phase_name='Active dependency check'):
        super().__init__(phase_id or str(dependency_match), CoordTypes.DEPENDENCY.value, phase_name)
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
            raise PhaseTerminated(TerminationStatus.UNSATISFIED)
        log.debug(f"[active_dependency_found] instances={[r.instance_id for r in matching_runs]}")

    def _stop_started_run(self, reason):
        pass


class WaitingPhase(BasePhase[OutputContext]):
    """
    """

    def __init__(self, phase_id, observable_conditions, timeout=0):
        super().__init__(phase_id, CoordTypes.WAITING.value)
        self._observable_conditions = observable_conditions
        self._timeout = timeout
        self._conditions_lock = Lock()
        self._event = Event()
        self._term_status: Optional[TerminationStatus] = None

    def _run(self, ctx):
        for condition in self._observable_conditions:
            condition.add_result_listener(self._result_observer)
            condition.start_evaluating()

        resolved = self._event.wait(self._timeout or None)
        if not resolved:
            self._term_status = TerminationStatus.TIMEOUT

        self._stop_all()
        if self._term_status:
            raise PhaseTerminated(self._term_status)

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

    def _stop_started_run(self, reason):
        self._stop_all()
        self._event.set()

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
class ConcurrencyGroup:
    group_id: str
    max_executions: int

    def __post_init__(self):
        if self.max_executions < 1:
            raise ValueError("max_executions must be greater than 0")


class ExecutionQueue(BasePhase[JobInstanceContext]):
    GROUP_ID = "group_id"
    MAX_EXEC = "max_exec"
    STATE = "state"

    def __init__(self, phase_id, concurrency_group, limited_phase, phase_name=None):
        super().__init__(phase_id or concurrency_group.group_id, CoordTypes.QUEUE.value, phase_name, [limited_phase])
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
        self._phase_filter_running = copy.copy(self._phase_filter)
        self._phase_filter_running.lifecycle = LifecycleCriterion(stage=Stage.RUNNING)

        self._queue_change_condition = Condition()
        # vv Guarding these fields vv
        self._state = QueuedState.NONE
        self._queue_changed = True

    def _lock_name(self):
        # TODO Modifiable (inheritance?)
        return f"eq-{self.execution_group}.lock"

    @property
    def attributes(self):
        return self._attrs

    @property
    def variables(self):
        return {ExecutionQueue.STATE: self._state.name}

    @property
    @control_api
    def state(self):
        return self._state

    @property
    @control_api
    def execution_group(self):
        return self._group

    def _run(self, ctx):
        try:
            ctx.environment.notifications.add_observer_transition(self._instance_transition_update)

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
                        self._queue_change_condition.wait()  # Set timeout
                        continue

                    self._queue_changed = False

                with ctx.environment.lock(self._lock_name()):
                    self._dispatch_next(ctx)
        finally:
            ctx.environment.notifications.remove_observer_transition(self._instance_transition_update)

        self._children[0].run(ctx)

    def _stop_started_run(self, reason):
        with self._queue_change_condition:
            if self._state.dequeued:
                self._children[0].stop(reason)
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
        runs: List[JobRun] = ctx.environment.get_active_runs(JobRunCriteria(phase_criteria=self._phase_filter_running))
        runs_sorted = sorted(runs, key=lambda run: run.find_first_phase(self._phase_filter).lifecycle.created_at)
        ids_dispatched = {r.instance_id for r in runs_sorted if
                          r.find_first_phase(self._phase_filter).variables[
                              ExecutionQueue.STATE] == QueuedState.DISPATCHED.name}
        free_slots = self._group.max_executions - len(ids_dispatched)

        if free_slots <= 0:
            log.debug("event[exec_limit_reached] slots=[%d] dispatched=[%d]",
                      self._group.max_executions, len(ids_dispatched))
            return False

        log.debug("event[dispatching_from_queue] free_slots=[%d]", free_slots)
        for next_dispatch in runs_sorted:
            if next_dispatch.instance_id in ids_dispatched:
                continue
            dispatched = (
                ctx.environment.get_instance(next_dispatch.instance_id).find_phase_control(
                    self._phase_filter).signal_dispatch())
            if dispatched:
                log.debug("event[dispatched] run=[%s]", next_dispatch.metadata)
                free_slots -= 1
                if free_slots <= 0:
                    return
            else:
                log.debug("event[not_dispatched] run=[%s]", next_dispatch.metadata)

    def _instance_transition_update(self, event: InstanceTransitionEvent):
        with self._queue_change_condition:
            if self._queue_changed or event.new_stage != Stage.ENDED or event.is_root_phase or not self._phase_filter(
                    # TODO Root phase
                    event.job_run.find_phase_by_id(event.phase_id)):
                return

            log.debug("event[queue_slot_freed] instance=[%s] phase=[%s]", event.instance.instance_id, event.phase_id)
            self._queue_changed = True
            self._queue_change_condition.notify()
