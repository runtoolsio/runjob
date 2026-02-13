import functools
import logging
from abc import ABC, abstractmethod
from contextvars import ContextVar
from datetime import datetime, timedelta
from multiprocessing import Lock  # TODO
from threading import Timer
from typing import Optional, Generic, List

from runtools.runcore import err
from runtools.runcore.job import Stage
from runtools.runcore.run import TerminationStatus, TerminationInfo, C, PhaseControl, \
    PhaseDetail, PhaseTransitionObserver, PhaseTransitionEvent, StopReason
from runtools.runcore.util import utc_now
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY, ObservableNotification

log = logging.getLogger(__name__)

_current_phase: ContextVar['BasePhase'] = ContextVar('_current_phase')

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


class ChildPhaseTerminated(PhaseTerminated):
    """Raised by the @phase wrapper when a child phase terminated non-successfully.

    Unlike PhaseTerminated (a phase-internal signal caught by run()), this exception crosses
    phase boundaries so parent code can handle child failures explicitly. Extends PhaseTerminated
    so unhandled cases are caught by the existing ``except PhaseTerminated`` in run().

    Catching hierarchy:
        ``except ChildPhaseTerminated``: only child failures
        ``except PhaseTerminated``: both child failures and own stop/timeout
    """
    pass


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
    """
    Base implementation providing common functionality for phases.

    A phase goes through three stages: CREATED -> RUNNING -> ENDED. ``run(ctx)`` drives the full
    lifecycle and can only be called once. ``stop(reason)`` can be called from any thread at any time
    and is idempotent — it works whether the phase hasn't started yet, is currently running, or has
    already ended.

    Subclass contract:
        ``_run(ctx)``:
            Implement the phase's work. Must be interruptible: when ``_stop_running`` unblocks
            whatever ``_run`` is waiting on, ``_run`` should detect it and either return or raise
            ``PhaseTerminated``. Use ``_raise_if_stopped()`` after waking from any blocking wait.
            Use ``run_child(child)`` to execute children (not ``child.run()`` directly); it raises
            when a child ends unsuccessfully, so failures can be handled explicitly in ``_run``.
            Raise ``PhaseTerminated(status)`` to terminate with a specific status; normal return means
            COMPLETED.

        ``_stop_running(reason)`` (optional override):
            Unblock ``_run`` so it can detect the stop and exit. Does not need to stop children —
            already done by ``stop()`` before this method is called. Must be safe to call from another
            thread. Default is a no-op — override when ``_run`` blocks on something that needs unblocking.

    Key invariants:
        - Children stop before parent: ``stop()`` stops children first (in reverse order). By the time
          ``_run`` detects the stop, all children are already terminated.
        - Pre-terminated children are skipped: ``run()`` returns immediately if already terminated,
          so ``run_child`` on a pre-stopped child is a no-op.

    Thread safety:
        The ``_lifecycle_lock`` guards the start/terminate decision boundary, ensuring that:
        - ``started_at`` is set at most once (phase can be started only once)
        - ``termination`` is set at most once
        - Termination may occur before the phase starts
        - Start may not occur after termination
    """

    def __init__(self, phase_id: str, phase_type: str, name: Optional[str] = None, children=()):
        self._id = phase_id
        self._type = phase_type
        self._children = list(children)
        self._name = name
        self._created_at: datetime = utc_now()
        self._lifecycle_lock = Lock()
        self._notification = ObservableNotification[PhaseTransitionObserver]()
        self._ctx = None
        self._started_at: Optional[datetime] = None
        self._termination: Optional[TerminationInfo] = None
        self._stop_reason: Optional[StopReason] = None
        self._failure_raised: bool = False
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
        with self._lifecycle_lock:
            if self._started_at or self.termination:
                return None
            self._started_at = utc_now()
        self._notification.observer_proxy.new_phase_transition(
            PhaseTransitionEvent(self.detail(), Stage.RUNNING, self._started_at))

        self._ctx = ctx
        term = None
        token = _current_phase.set(self)
        try:
            return self._run(ctx)
        except PhaseTerminated as e:
            msg = e.message
            stack_trace = None
            if not e.termination_status.outcome.is_success:
                if e.__cause__:
                    if not msg:
                        msg = str(e.__cause__)
                    stack_trace = err.stacktrace_str(e.__cause__)
            term = TerminationInfo(e.termination_status, utc_now(), msg, stack_trace)
        except (KeyboardInterrupt, SystemExit) as e:
            self.stop()
            term = TerminationInfo(TerminationStatus.INTERRUPTED, utc_now())
            raise
        except Exception as e:
            self._failure_raised = True
            term = TerminationInfo(TerminationStatus.ERROR, utc_now(), str(e), err.stacktrace_str(e))
            raise
        finally:
            _current_phase.reset(token)
            self._ctx = None
            if term:
                self._termination = term
            else:
                for child in self._children:
                    if child.termination and not child.termination.status.outcome.is_success:
                        # Skip children whose failure was raised as an exception — the parent's
                        # _run() had the chance to handle it. If it returned normally, it did.
                        if getattr(child, '_failure_raised', False):
                            continue
                        self._termination = TerminationInfo(child.termination.status, utc_now(),
                                                            child.termination.message)
                        break
                if not self._termination:
                    self._termination = TerminationInfo(TerminationStatus.COMPLETED, utc_now())
            self._notification.observer_proxy.new_phase_transition(
                PhaseTransitionEvent(self.detail(), Stage.ENDED, self._termination.terminated_at))

    def run_child(self, child):
        # Dynamically created children (e.g. @phase functions) don't exist when stop() calls
        # _stop_children(), so they would run despite the parent being stopped. This check ensures
        # stop propagates at the next run_child boundary.
        self._raise_if_stopped()
        if child not in self._children:
            self._children.append(child)
            child.add_phase_observer(self._notification.observer_proxy)
        return child.run(self._ctx)

    def _stop_running(self, reason):
        """Signal ``_run()`` to stop. Override when ``_run()`` blocks on something that needs unblocking."""
        pass

    def _raise_if_stopped(self):
        """Raise PhaseTerminated if the phase has been stopped. Call after waking from a blocking wait."""
        if self._stop_reason:
            raise PhaseTerminated(self._stop_reason.termination_status)

    def _stop_children(self, reason):
        # Stop in reverse order, so a parent executing children sequentially may assume
        # all remaining children are stopped when it detects the current child has stopped.
        for child in reversed(self._children):
            child.stop(reason)

    def stop(self, reason=StopReason.STOPPED):
        # Stop children first to ensure child stopping completes (i.e. termination is set) before
        # the parent's run() returns (when the parent responds to stop by returning before all children run).
        # Wrong order would keep termination state corrupted (parent terminated, child not) for a short period.
        self._stop_children(reason)

        with self._lifecycle_lock:
            if self.termination:
                return
            self._stop_reason = reason
            if not self._started_at:
                self._termination = TerminationInfo(reason.termination_status, utc_now())
                self._notification.observer_proxy.new_phase_transition(
                    PhaseTransitionEvent(self.detail(), Stage.ENDED, self._termination.terminated_at))
                return

        self._stop_running(reason)

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
            self.run_child(child)
            if not child.termination.status.outcome.is_success:
                raise PhaseTerminated(child.termination.status, child.termination.message)


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


class FunctionPhase(BasePhase):
    """A phase that wraps a plain function call."""

    def __init__(self, phase_id: str, phase_type: str, func, args, kwargs, *, name: Optional[str] = None):
        super().__init__(phase_id, phase_type, name)
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def _run(self, ctx):
        return self._func(*self._args, **self._kwargs)


def phase(func=None, phase_type=None):
    """Decorator that turns a plain function into a phase.

    Usage::

        @phase
        def fetch_data(url):
            return requests.get(url).json()

        @phase("CUSTOM_TYPE")
        def transform(data):
            return [item['name'] for item in data]

    When called inside a running phase's ``_run()``, the decorated function automatically creates
    a child ``FunctionPhase``, registers it with the parent via ``run_child()``, and returns the result.
    """
    if func is None:
        # Called as @phase("CUSTOM_TYPE")
        def decorator(f):
            return _make_phase_wrapper(f, phase_type)
        return decorator

    if isinstance(func, str):
        # Called as @phase("CUSTOM_TYPE") — func is actually the type string
        def decorator(f):
            return _make_phase_wrapper(f, func)
        return decorator

    # Called as @phase (no arguments)
    return _make_phase_wrapper(func, None)


def _make_phase_wrapper(func, explicit_type):
    resolved_type = explicit_type or func.__name__.upper()

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            parent = _current_phase.get()
        except LookupError:
            raise RuntimeError(
                f"@phase function '{func.__name__}' called outside a running phase. "
                f"It must be called from within a phase's _run() method."
            )
        child = FunctionPhase(func.__name__, resolved_type, func, args, kwargs)
        result = parent.run_child(child)
        # Uncaught exceptions already propagated with their original type (never reaches here).
        # PhaseTerminated and pre-termination are caught by run() internally, so run_child()
        # returns None silently. Re-raise as ChildPhaseTerminated so parent code can handle it.
        if child.termination and not child.termination.status.outcome.is_success:
            # Mark so parent's scanning skips this child — the failure is being surfaced
            # as an exception, giving _run() the chance to handle it.
            child._failure_raised = True
            raise ChildPhaseTerminated(child.termination.status, child.termination.message)
        return result

    return wrapper
