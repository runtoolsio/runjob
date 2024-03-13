import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from logging import DEBUG
from threading import Condition, Event, Lock
from typing import Dict

import sys

import runtools.runcore
from runtools.runcore import paths
from runtools.runcore.criteria import InstanceMetadataCriterion, EntityRunCriteria, PhaseCriterion
from runtools.runcore.job import JobRun, JobRuns, InstanceTransitionObserver
from runtools.runcore.listening import InstanceTransitionReceiver
from runtools.runcore.run import RunState, Phase, TerminationStatus, PhaseRun, TerminateRun, RunContext, PhaseInfo, \
    register_phase_info
from runtools.runcore.util import lock, KVParser
from runtools.runcore.util.log import ForwardLogs
from runtools.runner.task import OutputToTask

log = logging.getLogger(__name__)


def output_to_task_handler(run_ctx):
    return OutputToTask(run_ctx.task_tracker, parsers=[KVParser()]).create_logging_handler()


def forward_logs(logger, run_ctx):
    return ForwardLogs(logger, [run_ctx.create_logging_handler(), output_to_task_handler(run_ctx)])


class CoordTypes(Enum):
    APPROVAL = 'APPROVAL'
    NO_OVERLAP = 'NO_OVERLAP'
    DEPENDENCY = 'DEPENDENCY'
    WAITING = 'WAITING'
    QUEUE = 'QUEUE'


class ApprovalPhase(Phase):
    """
    Approval parameters (incl. timeout) + approval eval as separate objects
    TODO: parameters
    """

    def __init__(self, phase_id='approval', phase_name='Approval', *, timeout=0):
        super().__init__(CoordTypes.APPROVAL, phase_id, RunState.PENDING, phase_name)
        self._log = logging.getLogger(self.__class__.__name__)
        self._log.setLevel(DEBUG)
        self._timeout = timeout
        self._event = Event()
        self._stopped = False

    def run(self, run_ctx: RunContext):
        with forward_logs(self._log, run_ctx):
            self._log.debug("task=[Approval] operation=[Waiting]")

            approved = self._event.wait(self._timeout or None)
            if self._stopped:
                self._log.debug("task=[Approval] result=[Cancelled]")
                return
            if not approved:
                self._log.debug("task=[Approval] result=[Not Approved]")
                raise TerminateRun(TerminationStatus.TIMEOUT)

            self._log.debug("task=[Approval] result=[Approved]")

    def approve(self):
        self._event.set()

    def is_approved(self):
        self._event.is_set() and not self._stopped

    def stop(self):
        self._stopped = True
        self._event.set()

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED


class NoOverlapPhase(Phase):
    """
    TODO Docs
    1. Set continue flag to be checked
    """

    def __init__(self, no_overlap_id, phase_id=None, phase_name='No Overlap Check',
                 *, until_phase=None, locker_factory=lock.default_locker_factory()):
        if not no_overlap_id:
            raise ValueError("no_overlap_id cannot be empty")

        super().__init__(CoordTypes.NO_OVERLAP, phase_id or no_overlap_id, RunState.EVALUATING, phase_name,
                         protection_id=no_overlap_id, last_protected_phase=until_phase)
        self._log = logging.getLogger(self.__class__.__name__)
        self._log.setLevel(DEBUG)
        self._locker = locker_factory(paths.lock_path(f"noo-{no_overlap_id}.lock", True))

    def run(self, run_ctx):
        with forward_logs(self._log, run_ctx):
            self._log.debug("task=[No Overlap Check]")
            with self._locker():
                no_overlap_filter = PhaseCriterion(phase_type=CoordTypes.NO_OVERLAP, protection_id=self._protection_id)
                c = EntityRunCriteria(phase_criteria=no_overlap_filter)
                runs, _ = runtools.runcore.get_active_runs(c)
                if any(r for r in runs if r.run.in_protected_phase(CoordTypes.NO_OVERLAP, self._protection_id)):
                    self._log.debug("task=[No Overlap Check] result=[Overlap found]")
                    raise TerminateRun(TerminationStatus.OVERLAP)

        self._log.debug("task=[No Overlap Check] result=[No overlap found]")

    def stop(self):
        pass

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED


class DependencyPhase(Phase):

    def __init__(self, dependency_match, phase_id=None, phase_name='Active dependency check'):
        phase_id = phase_id or str(dependency_match)
        super().__init__(CoordTypes.DEPENDENCY, phase_id, RunState.EVALUATING, phase_name)
        self._log = logging.getLogger(self.__class__.__name__)
        self._log.setLevel(DEBUG)
        self._dependency_match = dependency_match

    @property
    def dependency_match(self):
        return self._dependency_match

    def run(self, run_ctx):
        with forward_logs(self._log, run_ctx):
            self._log.debug("task=[Dependency pre-check] dependency=[%s]", self._dependency_match)
            runs, _ = runtools.runcore.get_active_runs()
            matches = [r.metadata for r in runs if self._dependency_match(r.metadata)]
            if not matches:
                self._log.debug("result=[No active dependency found] dependency=[%s]]", self._dependency_match)
                raise TerminateRun(TerminationStatus.UNSATISFIED)
            self._log.debug("result=[Active dependency found] matches=%s", matches)

    def stop(self):
        pass

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED


class WaitingPhase(Phase):
    """
    """

    def __init__(self, phase_id, observable_conditions, timeout=0):
        super().__init__(CoordTypes.WAITING, phase_id, RunState.WAITING)
        self._observable_conditions = observable_conditions
        self._timeout = timeout
        self._conditions_lock = Lock()
        self._event = Event()
        self._term_status = TerminationStatus.NONE

    def run(self, run_ctx):
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


@register_phase_info(CoordTypes.QUEUE)
@dataclass(frozen=True)
class ExecutionQueueInfo(PhaseInfo):
    queued_state: QueuedState

    def serialize(self) -> Dict:
        d = super().serialize()
        d["queued_state"] = self.queued_state.name
        return d


class ExecutionQueue(Phase, InstanceTransitionObserver):

    def __init__(self, queue_id, max_executions, phase_id=None, phase_name=None, *,
                 until_phase=None,
                 locker_factory=lock.default_locker_factory(),
                 state_receiver_factory=InstanceTransitionReceiver):
        if not queue_id:
            raise ValueError('Queue ID must be specified')
        if max_executions < 1:
            raise ValueError('Max executions must be greater than zero')

        super().__init__(CoordTypes.QUEUE, phase_id or queue_id, RunState.IN_QUEUE, phase_name,
                         protection_id=queue_id, last_protected_phase=until_phase)
        self._log = logging.getLogger(self.__class__.__name__)
        self._log.setLevel(DEBUG)
        self._state = QueuedState.NONE
        self._queue_id = queue_id
        self._max_executions = max_executions
        self._locker = locker_factory(paths.lock_path(f"eq-{queue_id}.lock", True))
        self._state_receiver_factory = state_receiver_factory
        self._wait_guard = Condition()
        # vv Guarding these fields vv
        self._current_wait = False
        self._state_receiver = None

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)  # Set the minimum logging level for this handler

        # Optionally, set a formatter for the handler
        formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        stdout_handler.setFormatter(formatter)

        # Add the handler to the logger
        self._log.addHandler(stdout_handler)

    def info(self) -> ExecutionQueueInfo:
        return ExecutionQueueInfo(
            self._phase_type,
            self._phase_id,
            self._run_state,
            self._phase_name,
            self._protection_id,
            self._last_protected_phase,
            self._state
        )

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED

    @property
    def state(self):
        return self._state

    @property
    def queue_id(self):
        return self._queue_id

    def run(self, run_ctx):
        with forward_logs(self._log, run_ctx):
            with self._wait_guard:
                if self._state == QueuedState.NONE:
                    self._state = QueuedState.IN_QUEUE

            while True:
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
        self._log.debug("event[dispatch_signalled]")
        with self._wait_guard:
            if self._state.dequeued:
                return False  # Cancelled

            self._state = QueuedState.DISPATCHED
            self._wait_guard.notify_all()
            return True

    def _start_listening(self):
        self._state_receiver = self._state_receiver_factory()
        self._state_receiver.add_observer_transition(self)
        self._state_receiver.start()

    def _dispatch_next(self):
        phase_filter = PhaseCriterion(phase_type=CoordTypes.QUEUE, protection_id=self._queue_id)
        criteria = EntityRunCriteria(phase_criteria=phase_filter)
        runs, _ = runtools.runcore.get_active_runs(criteria)

        # TODO Sort by phase start
        sorted_group_runs = JobRuns(sorted(runs, key=lambda job_run: job_run.run.lifecycle.created_at))
        occupied = len(
            [r for r in sorted_group_runs
             if r.run.in_protected_phase(CoordTypes.QUEUE, self._queue_id)
             or (r.run.current_phase().phase_type == CoordTypes.QUEUE and r.run.current_phase().queued_state.dequeued)])
        free_slots = self._max_executions - occupied
        if free_slots <= 0:
            self._log.debug("event[no_dispatch] slots=[%d] occupied=[%d]", self._max_executions, occupied)
            return False

        self._log.debug("event[dispatch] free_slots=[%d]", free_slots)
        for next_proceed in sorted_group_runs.queued:
            c = EntityRunCriteria(metadata_criteria=InstanceMetadataCriterion.for_run(next_proceed))
            signal_resp = runtools.runcore.signal_dispatch(c, self._queue_id)
            for r in signal_resp.responses:
                if r.dispatched:
                    free_slots -= 1
                    if free_slots <= 0:
                        return

    def new_instance_phase(self, job_run: JobRun, previous_phase: PhaseRun, new_phase: PhaseRun, ordinal: int):
        with self._wait_guard:
            if not self._current_wait:
                return
            if (protected_phases := job_run.run.protected_phases(CoordTypes.QUEUE, self._queue_id)) \
                    and previous_phase.phase_key in protected_phases \
                    and new_phase not in protected_phases:
                # Run slot freed
                self._current_wait = False
                self._stop_listening()
                self._wait_guard.notify()

    def _stop_listening(self):
        self._state_receiver.close()
        self._state_receiver.remove_observer_transition(self)
        self._state_receiver = None
