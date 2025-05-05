import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from multiprocessing import Lock  # TODO
from typing import Optional, Generic, List

from runtools.runcore import err
from runtools.runcore.job import Stage
from runtools.runcore.run import TerminationStatus, TerminationInfo, RunState, C, PhaseControl, \
    PhaseDetail, PhaseTransitionObserver, PhaseTransitionEvent, Outcome, RunCompletionError
from runtools.runcore.util import utc_now
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY, ObservableNotification

log = logging.getLogger(__name__)

UNCAUGHT_PHASE_EXEC_EXCEPTION = "UNCAUGHT_PHASE_RUN_EXCEPTION"
CHILD_RUN_ERROR = "ERROR"


class PhaseTerminated(Exception):
    """
    Exception raised to signal that a Phase has been externally terminated.

    This exception is raised within phases to indicate intentional termination.
    The parent phase's policy determines whether only the current phase stops
    or if the termination propagates to stop all phases.

    Attributes:
        termination_status (TerminationStatus): The status code indicating how the phase terminated.
        message (Optional[str]): Optional termination message.
    """

    def __init__(self, termination_status: TerminationStatus, message=None):
        super().__init__(message)
        self.termination_status = termination_status
        self.message = message


class Phase(ABC, Generic[C]):

    @property
    @abstractmethod
    def id(self):
        pass

    @property
    @abstractmethod
    def type(self) -> str:
        """
        The type of this phase. Should be defined as a constant value in each implementing class.
        """
        pass

    @property
    @abstractmethod
    def run_state(self) -> RunState:
        """
        The run state of this phase. Should be defined as a constant value in each implementing class.
        """
        pass

    @property
    def name(self) -> Optional[str]:
        return None

    @property
    def attributes(self):
        return {}

    @property
    def variables(self):
        return {}

    @abstractmethod
    def detail(self):
        """
        Creates a view of the current phase state.

        Returns:
            PhaseView: An immutable view of the phase's current state
        """
        pass

    @property
    def control(self):
        return PhaseControl(self)

    @property
    @abstractmethod
    def children(self):
        pass

    @abstractmethod
    def find_phase_control(self, phase_filter) -> Optional[PhaseControl]:
        pass

    def find_phase_control_by_id(self, phase_id: str) -> Optional[PhaseControl]:
        self.find_phase_control(lambda phase: phase.id == phase_id)

    @abstractmethod
    def run(self, ctx: Optional[C]):
        pass

    @abstractmethod
    def stop(self):
        pass

    @property
    @abstractmethod
    def created_at(self):
        pass

    @property
    @abstractmethod
    def started_at(self):
        pass

    @property
    @abstractmethod
    def termination(self):
        pass

    @abstractmethod
    def add_phase_observer(self, observer, *, priority=DEFAULT_OBSERVER_PRIORITY):
        pass

    @abstractmethod
    def remove_phase_observer(self, observer):
        pass


class BasePhase(Phase[C], ABC):
    """Base implementation providing common functionality for V2 phases"""

    def __init__(self, phase_id: str, phase_type: str, run_state: RunState, children=(), name: Optional[str] = None):
        self._id = phase_id
        self._type = phase_type
        self._run_state = run_state
        self._children = list(children)
        self._name = name
        self._created_at: datetime = utc_now()
        self._ctx = None
        self._started_at: Optional[datetime] = None
        self._termination: Optional[TerminationInfo] = None
        self._notification = ObservableNotification[PhaseTransitionObserver]()
        for c in children:
            c.add_phase_observer(self._notification.observer_proxy)

    @abstractmethod
    def _run(self, ctx: Optional[C]):
        pass

    @property
    def id(self) -> str:
        return self._id

    @property
    def type(self) -> str:
        return self._type

    @property
    def run_state(self) -> RunState:
        return self._run_state

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def created_at(self) -> Optional[datetime]:
        return self._created_at

    @property
    def started_at(self) -> Optional[datetime]:
        return self._started_at

    @property
    def termination(self) -> Optional[TerminationInfo]:
        return self._termination

    @property
    def total_run_time(self) -> Optional[timedelta]:
        """
        Calculates the total runtime of the phase.

        Returns:
            Optional[timedelta]: Time between phase start and termination if both exist, otherwise None
        """
        if not self._started_at or not self._termination:
            return None

        return self._termination.terminated_at - self._started_at

    @property
    def children(self) -> List[Phase]:
        return self._children.copy()

    def find_phase_control(self, phase_filter) -> Optional[PhaseControl]:
        """
        Find phase control, searching recursively through all children.

        Args:
            phase_filter: The filter to find the phase

        Returns:
            PhaseControl for the matching phase, or None if not found
        """
        phase = None
        if phase_filter(self.detail()):
            phase = self
        else:
            for child in self.children:
                if phase_filter(child.detail()):
                    phase = child
                    break
                if phase_control := child.find_phase_control(phase_filter):
                    return phase_control

        if not phase:
            return None

        return phase.control

    def detail(self) -> PhaseDetail:
        """
        Creates a view of the current phase state.

        Returns:
            PhaseView: An immutable view of the phase's current state
        """
        return PhaseDetail.from_phase(self)

    def run(self, ctx: Optional[C]):
        self._started_at = utc_now()
        self._notification.observer_proxy.new_phase_transition(
            PhaseTransitionEvent(self.detail(), Stage.RUNNING, self._started_at))

        self._ctx = ctx
        term = None
        try:
            self._run(ctx)
        except PhaseTerminated as e:
            if e.termination_status.is_outcome(Outcome.NON_SUCCESS):
                msg = e.message
                stack_trace = None
                if e.__cause__:
                    if not msg:
                        msg = str(e.__cause__)
                    stack_trace = err.stacktrace_str(e.__cause__)
                term = TerminationInfo(e.termination_status, utc_now(), msg, stack_trace)
        except RunCompletionError as e:
            term = TerminationInfo(TerminationStatus.ERROR, utc_now(), e.original_message())
            raise RunCompletionError(self.id, f"{e.phase_id} -> {e}") from e
        except Exception as e:
            term = TerminationInfo(TerminationStatus.ERROR, utc_now(), str(e), err.stacktrace_str(e))
            raise RunCompletionError(self.id, str(e)) from e
        except (KeyboardInterrupt, SystemExit) as e:
            self.stop()
            term = TerminationInfo(TerminationStatus.INTERRUPTED, utc_now())
            raise e
        finally:
            self._ctx = None
            if term:
                self._termination = term
            else:
                for child in self._children:
                    if child.termination and child.termination.status != TerminationStatus.COMPLETED:
                        self._termination = TerminationInfo(child.termination.status, utc_now(), child.termination.message)
                        break
                if not self._termination:
                    self._termination = TerminationInfo(TerminationStatus.COMPLETED, utc_now())
            self._notification.observer_proxy.new_phase_transition(
                PhaseTransitionEvent(self.detail(), Stage.ENDED, self._termination.terminated_at))

    def run_child(self, child):
        if self._termination:
            raise RuntimeError("Parent already terminated")
        if child not in self._children:
            self._children.append(child)
            child.add_phase_observer(self._notification.observer_proxy)
        child.run(self._ctx)

    def add_phase_observer(self, observer, *, priority=DEFAULT_OBSERVER_PRIORITY):
        self._notification.add_observer(observer, priority)

    def remove_phase_observer(self, observer):
        self._notification.remove_observer(observer)


class SequentialPhase(BasePhase):
    """
    A phase that executes its children sequentially.
    If any child phase fails, the execution stops and the error is propagated.
    """

    TYPE = 'SEQUENTIAL'

    def __init__(self, phase_id: str, children: List[Phase[C]], name: Optional[str] = None):
        super().__init__(phase_id, SequentialPhase.TYPE, RunState.EXECUTING, children, name)
        self._current_child: Optional[Phase[C]] = None
        self._stop_lock = Lock()
        self._stopped = False

    def _run(self, ctx: Optional[C]):
        """
        Execute child phases in sequence.
        If any phase fails, execution is terminated.
        """
        try:
            for child in self._children:
                with self._stop_lock:
                    if self._stopped:
                        if self.termination:
                            break
                        else:
                            raise PhaseTerminated(TerminationStatus.STOPPED)
                    self._current_child = child
                self.run_child(child)
                if child.termination.status != TerminationStatus.COMPLETED:
                    raise PhaseTerminated(child.termination.status, child.termination.message)
        finally:
            self._current_child = None

    def stop(self):
        """
        Stop the current child phase if one is executing
        """
        with self._stop_lock:
            self._stopped = True
            if self._current_child:
                self._current_child.stop()
            else:
                self._termination = TerminationInfo(TerminationStatus.STOPPED, utc_now())
