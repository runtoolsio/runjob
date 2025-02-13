import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from multiprocessing import Lock  # TODO
from threading import Event
from typing import Any, Dict, Optional, Generic, List

from runtools.runcore.job import Stage
from runtools.runcore.run import Phase, TerminationStatus, TerminationInfo, Fault, RunState, C, PhaseControl, \
    PhaseDetail, PhaseTransitionObserver, PhaseTransitionEvent
from runtools.runcore.util import utc_now
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY, ObservableNotification

log = logging.getLogger(__name__)

UNCAUGHT_PHASE_EXEC_EXCEPTION = "UNCAUGHT_PHASE_RUN_EXCEPTION"


class PhaseCompletionError(Exception):

    def __init__(self, phase_id):
        super().__init__(f"Phase '{phase_id}' execution did not complete successfully")
        self.phase_id = phase_id


class PhaseTypeMismatchError(Exception):

    def __init__(self, phase_id, expected_type, actual_type):
        super().__init__(f"Phase '{phase_id}' has unexpected type: expected {expected_type}, got {actual_type}")


class PhaseV2(ABC, Generic[C]):

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
    def find_phase_control(self, phase_id: str, phase_type: str = None) -> Optional[PhaseControl]:
        pass

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


class ExecutionTerminated(Exception):

    def __init__(self, termination_status: TerminationStatus):
        self.termination_status = termination_status


class BasePhase(PhaseV2[C], ABC):
    """Base implementation providing common functionality for V2 phases"""

    def __init__(self, phase_id: str, phase_type: str, run_state: RunState, name: Optional[str] = None):
        self._id = phase_id
        self._type = phase_type
        self._run_state = run_state
        self._name = name
        self._created_at: datetime = utc_now()
        self._started_at: Optional[datetime] = None
        self._termination: Optional[TerminationInfo] = None
        self._notification = ObservableNotification[PhaseTransitionObserver]()

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
    def children(self) -> List[PhaseV2]:
        return []

    def find_phase_control(self, phase_id: str, phase_type: str = None) -> Optional[PhaseControl]:
        """
        Find phase control by phase ID, searching recursively through all children.

        Args:
            phase_id: The ID of the phase to find
            phase_type: The type of the found phase (TODO exception)

        Returns:
            PhaseControl for the matching phase, or None if not found
        """
        phase = None
        if self.id == phase_id:
            phase = self
        else:
            for child in self.children:
                if child.id == phase_id:
                    phase = child
                    break
                if phase_control := child.find_phase_control(phase_id, phase_type):
                    return phase_control

        if not phase:
            return None
        if phase_type and phase.type != phase_type:
            raise PhaseTypeMismatchError(phase.id, phase_type, phase.type)

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

        term = None
        try:
            self._run(ctx)
        except ExecutionTerminated as e:
            fault = None
            if e.__cause__:
                fault = Fault.from_exception("EXECUTION_TERMINATED", e.__cause__)
            term = TerminationInfo(e.termination_status, utc_now(), fault)
            raise PhaseCompletionError(self.id) from e
        except Exception as e:
            fault = Fault.from_exception(UNCAUGHT_PHASE_EXEC_EXCEPTION, e)
            term = TerminationInfo(TerminationStatus.FAILED, utc_now(), fault)
            raise PhaseCompletionError(self.id) from e
        except (KeyboardInterrupt, SystemExit) as e:
            self.stop()
            term = TerminationInfo(TerminationStatus.INTERRUPTED, utc_now())
            raise e
        finally:
            if term:
                self._termination = term
            else:
                self._termination = TerminationInfo(TerminationStatus.COMPLETED, utc_now())
            self._notification.observer_proxy.new_phase_transition(
                PhaseTransitionEvent(self.detail(), Stage.ENDED, self._termination.terminated_at))

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

    def __init__(self, phase_id: str, children: List[PhaseV2[C]], name: Optional[str] = None):
        super().__init__(phase_id, SequentialPhase.TYPE, RunState.EXECUTING, name)
        self._children = children
        self._current_child: Optional[PhaseV2[C]] = None
        self._stop_lock = Lock()
        self._stopped = False
        for c in children:
            c.add_phase_observer(self._notification.observer_proxy)

    @property
    def children(self) -> List[PhaseV2[C]]:
        return self._children.copy()

    def _run(self, ctx: Optional[C]):
        """
        Execute child phases in sequence.
        If any phase fails, execution is terminated.
        """
        try:
            for child in self._children:
                with self._stop_lock:
                    if self._stopped:
                        raise ExecutionTerminated(TerminationStatus.STOPPED)
                    self._current_child = child
                child.run(ctx)
                if child.termination.status != TerminationStatus.COMPLETED:
                    raise ExecutionTerminated(child.termination.status)
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


def unique_phases_to_dict(phases) -> Dict[str, Phase]:
    id_to_phase = {}
    for phase in phases:
        if phase.id in id_to_phase:
            raise ValueError(f"Duplicate phase found: {phase.id}")
        id_to_phase[phase.id] = phase
    return id_to_phase


class DelegatingPhase(Phase[C]):
    """
    Base class for phases that delegate to a wrapped phase.
    Subclasses can override methods to add behavior while maintaining delegation.
    """

    def __init__(self, wrapped_phase: Phase[C]):
        self.wrapped = wrapped_phase

    @property
    def id(self):
        return self.wrapped.id

    @property
    def type(self) -> str:
        return self.wrapped.type

    @property
    def run_state(self) -> RunState:
        return self.wrapped.run_state

    @property
    def stop_status(self):
        return self.wrapped.stop_status

    def run(self, ctx: Optional[C]):
        return self.wrapped.run(ctx)

    def stop(self):
        return self.wrapped.stop()


class NoOpsPhase(Phase[Any], ABC):

    def __init__(self, phase_id, phase_type, run_state, stop_status):
        self._id = phase_id
        self._type = phase_type
        self._state = run_state
        self._stop_status = stop_status

    @property
    def id(self):
        return self._id

    @property
    def type(self) -> str:
        return self._type

    @property
    def run_state(self) -> RunState:
        return self._state

    @property
    def stop_status(self):
        return self._stop_status

    def run(self, ctx):
        """No activity on run"""
        pass

    def stop(self):
        """Nothing to stop"""
        pass


class InitPhase(NoOpsPhase):
    ID = 'init'
    TYPE = 'INIT'

    def __init__(self):
        super().__init__(InitPhase.ID, InitPhase.TYPE, RunState.CREATED, TerminationStatus.STOPPED)


class ExecutingPhase(Phase[C], ABC):
    """
    Abstract base class for phases that execute some work.
    Implementations should provide concrete run() and stop() methods
    that handle their specific execution mechanics.
    """
    TYPE = 'EXEC'

    def __init__(self, phase_id: str, phase_name: Optional[str] = None):
        self._id = phase_id
        self._name = phase_name

    @property
    def id(self):
        return self._id

    @property
    def type(self) -> str:
        return ExecutingPhase.TYPE

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def run_state(self) -> RunState:
        return RunState.EXECUTING

    @property
    def stop_status(self):
        return TerminationStatus.STOPPED

    @abstractmethod
    def run(self, ctx):
        """
        Execute the phase's work.

        Args:
            ctx (C): Context object related to the given run

        Raises:
            TerminateRun: If execution is stopped or interrupted
            FailedRun: If execution fails
        """
        pass

    @abstractmethod
    def stop(self):
        """Stop the currently running execution"""
        pass


class TerminalPhase(NoOpsPhase):
    ID = 'terminal'
    TYPE = 'TERMINAL'

    def __init__(self):
        super().__init__(TerminalPhase.ID, TerminationStatus.NONE, RunState.ENDED, TerminationStatus.NONE)


class WaitWrapperPhase(DelegatingPhase[C]):

    def __init__(self, wrapped_phase: Phase[C]):
        super().__init__(wrapped_phase)
        self._run_event = Event()

    def wait(self, timeout):
        self._run_event.wait(timeout)

    def run(self, ctx):
        self._run_event.set()
        super().run(ctx)


class RunContext(ABC):
    """Members to be added later"""
