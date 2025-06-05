import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from multiprocessing import Lock  # TODO
from threading import Timer
from typing import Optional, Generic, List

from runtools.runcore import err
from runtools.runcore.job import Stage
from runtools.runcore.run import TerminationStatus, TerminationInfo, C, PhaseControl, \
    PhaseDetail, PhaseTransitionObserver, PhaseTransitionEvent, Outcome, RunCompletionError, StopReason
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
    def is_idle(self) -> bool:
        """
        TODO
        """
        return False

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
        return self.find_phase_control(lambda phase: phase.id == phase_id)

    @abstractmethod
    def run(self, ctx: Optional[C]):
        pass

    @abstractmethod
    def stop(self, reason=StopReason.STOPPED):
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

    @property
    def stage(self) -> Stage:
        """Determines the current stage of the phase in its lifecycle."""
        if self.termination:
            return Stage.ENDED
        if self.started_at:
            return Stage.RUNNING
        return Stage.CREATED

    @abstractmethod
    def add_phase_observer(self, observer, *, priority=DEFAULT_OBSERVER_PRIORITY):
        pass

    @abstractmethod
    def remove_phase_observer(self, observer):
        pass


class PhaseDecorator(Phase[C], Generic[C]):
    """
    A generic base class for Phase decorators.
    This class implements all Phase methods by forwarding calls to the wrapped instance,
    allowing subclasses (concrete decorators) to selectively override behaviors to add new functionality.
    """

    def __init__(self, wrapped: Phase[C]):
        """
        Initialize with a delegate Phase.

        Args:
            wrapped: The Phase implementation to wrap
        """
        self._wrapped = wrapped

    @property
    def id(self) -> str:
        """Delegates to the wrapped phase's id property"""
        return self._wrapped.id

    @property
    def type(self) -> str:
        """Delegates to the wrapped phase's type property"""
        return self._wrapped.type

    @property
    def is_idle(self) -> bool:
        """Delegates to the wrapped phase's is_idle property"""
        return self._wrapped.is_idle

    @property
    def name(self) -> Optional[str]:
        """Delegates to the wrapped phase's name property"""
        return self._wrapped.name

    @property
    def attributes(self):
        """Delegates to the wrapped phase's attributes property"""
        return self._wrapped.attributes

    @property
    def variables(self):
        """Delegates to the wrapped phase's variables property"""
        return self._wrapped.variables

    def detail(self) -> PhaseDetail:
        """Delegates to the wrapped phase's detail method"""
        return self._wrapped.detail()

    @property
    def control(self) -> PhaseControl:
        """Delegates to the wrapped phase's control property"""
        return self._wrapped.control

    @property
    def children(self) -> List[Phase]:
        """Delegates to the wrapped phase's children property"""
        return self._wrapped.children

    def find_phase_control(self, phase_filter) -> Optional[PhaseControl]:
        """Delegates to the wrapped phase's find_phase_control method"""
        return self._wrapped.find_phase_control(phase_filter)

    def find_phase_control_by_id(self, phase_id: str) -> Optional[PhaseControl]:
        """Delegates to the wrapped phase's find_phase_control_by_id method"""
        return self._wrapped.find_phase_control_by_id(phase_id)

    def run(self, ctx: Optional[C]):
        """Delegates to the wrapped phase's run method"""
        return self._wrapped.run(ctx)

    def stop(self, reason=StopReason.STOPPED):
        """Delegates to the wrapped phase's stop method"""
        return self._wrapped.stop(reason)

    @property
    def created_at(self):
        """Delegates to the wrapped phase's created_at property"""
        return self._wrapped.created_at

    @property
    def started_at(self):
        """Delegates to the wrapped phase's started_at property"""
        return self._wrapped.started_at

    @property
    def termination(self):
        """Delegates to the wrapped phase's termination property"""
        return self._wrapped.termination

    def add_phase_observer(self, observer, *, priority=DEFAULT_OBSERVER_PRIORITY):
        """Delegates to the wrapped phase's add_phase_observer method"""
        self._wrapped.add_phase_observer(observer, priority=priority)

    def remove_phase_observer(self, observer):
        """Delegates to the wrapped phase's remove_phase_observer method"""
        self._wrapped.remove_phase_observer(observer)


class BasePhase(Phase[C], ABC):
    """Base implementation providing common functionality for V2 phases"""

    def __init__(self, phase_id: str, phase_type: str, name: Optional[str] = None, children=()):
        self._id = phase_id
        self._type = phase_type
        self._children = list(children)
        self._name = name
        self._created_at: datetime = utc_now()
        self._state_lock = Lock()
        self._notification = ObservableNotification[PhaseTransitionObserver]()
        self._ctx = None
        self._started_at: Optional[datetime] = None
        self._termination: Optional[TerminationInfo] = None
        self._stop_reason: Optional[StopReason] = None
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
        with self._state_lock:
            if self.termination:
                return
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
                        self._termination = TerminationInfo(child.termination.status, utc_now(),
                                                            child.termination.message)
                        break
                if not self._termination:
                    self._termination = TerminationInfo(TerminationStatus.COMPLETED, utc_now())
            self._notification.observer_proxy.new_phase_transition(
                PhaseTransitionEvent(self.detail(), Stage.ENDED, self._termination.terminated_at))

    def run_child(self, child):
        if self._termination:  # TODO Do we need to also check whether the phase is started
            raise RuntimeError("Parent already terminated")
        if child not in self._children:
            self._children.append(child)
            child.add_phase_observer(self._notification.observer_proxy)
        child.run(self._ctx)

    def run_children(self):
        for child in self._children:
            self.run_child(child)

    @abstractmethod
    def _stop_started_run(self, reason):
        pass

    def _stop_children(self, reason):
        for child in self._children:
            child.stop(reason)

    def stop(self, reason=StopReason.STOPPED):
        with self._state_lock:
            if self.termination:
                return
            self._stop_reason = reason
            if not self._started_at:
                self._termination = TerminationInfo(reason.termination_status, utc_now())
                self._notification.observer_proxy.new_phase_transition(
                    PhaseTransitionEvent(self.detail(), Stage.ENDED, self._termination.terminated_at))
                return
        self._stop_children(reason)  # TODO Swap stop propagation order
        self._stop_started_run(reason)

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
        super().__init__(phase_id, SequentialPhase.TYPE, name, children)

    def _run(self, ctx: Optional[C]):
        """
        Execute child phases in sequence.
        If any phase fails, execution is terminated.
        """
        for child in self._children:
            if self._stop_reason:
                raise PhaseTerminated(self._stop_reason.termination_status)
            self.run_child(child)
            if child.termination.status != TerminationStatus.COMPLETED:
                raise PhaseTerminated(child.termination.status, child.termination.message)

    def _stop_started_run(self, reason):
        pass


class TimeoutExtension(PhaseDecorator[C], Generic[C]):
    """
    A phase delegate that adds timeout functionality to any phase.

    If the wrapped phase doesn't complete within the specified timeout period,
    it will be stopped and marked as timed out.

    Attributes:
        timeout_sec: Maximum time in seconds to allow the phase to run
    """

    def __init__(self, wrapped: Phase[C], timeout_sec: float):
        """
        Initialize with a delegate phase and timeout value.

        Args:
            wrapped: The Phase implementation to wrap
            timeout_sec: Maximum time in seconds to allow the phase to run
        """
        if timeout_sec <= 0:
            raise ValueError("Timeout seconds value must be positive")
        super().__init__(wrapped)
        self.timeout_sec = timeout_sec
        self._timer = None

    def run(self, ctx: Optional[C]):
        """
        Run the wrapped phase with timeout control.

        If the phase doesn't complete within the specified timeout,
        it will be stopped and a PhaseTerminated exception with
        TerminationStatus.TIMEOUT will be raised.

        Args:
            ctx: The execution context to pass to the wrapped phase
        """
        self._timer = Timer(self.timeout_sec, self._on_timeout)
        self._timer.daemon = True
        self._timer.start()

        try:
            return super().run(ctx)
        finally:
            self._cancel_timer()

    def _on_timeout(self):
        """Handle the timeout by stopping the wrapped phase."""
        log.warning(f"timeout after_sec=[{self.timeout_sec}] phase=[{super().id}]")
        self.stop(reason=StopReason.TIMEOUT)

    def stop(self, reason=StopReason.STOPPED):
        """
        Stop the phase execution and cancel the timeout timer.

        Args:
            reason: The reason for stopping the phase
        """
        self._cancel_timer()
        super().stop(reason)

    def _cancel_timer(self):
        """Cancel the timeout timer if it's running."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
