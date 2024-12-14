import logging
from abc import ABC, abstractmethod
from copy import copy
from threading import Condition, Event
from typing import Any, Dict, Iterable, Optional, Callable, Tuple, Generic

from runtools.runcore import util
from runtools.runcore.common import InvalidStateError
from runtools.runcore.run import Phase, PhaseRun, PhaseInfo, Lifecycle, TerminationStatus, TerminationInfo, Run, \
    TerminateRun, FailedRun, RunError, RunState, E

log = logging.getLogger(__name__)


def unique_phases_to_dict(phases) -> Dict[str, Phase]:
    id_to_phase = {}
    for phase in phases:
        if phase.id in id_to_phase:
            raise ValueError(f"Duplicate phase found: {phase.id}")
        id_to_phase[phase.id] = phase
    return id_to_phase


class AbstractPhase(Phase[E], ABC):
    """
    TODO repr
    """

    def __init__(self, phase_id: str, phase_name: Optional[str] = None, *,
                 protection_id=None, last_protected_phase=None):
        self._phase_id = phase_id
        self._phase_name = phase_name
        self._protection_id = protection_id
        self._last_protected_phase = last_protected_phase

    @property
    def id(self):
        return self._phase_id

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
    def name(self):
        return self._phase_name

    def info(self) -> PhaseInfo:
        return PhaseInfo(
            self.id, self.type, self.run_state, self._phase_name, self._protection_id, self._last_protected_phase)

    @property
    @abstractmethod
    def stop_status(self):
        pass

    @abstractmethod
    def run(self, env: E, ctx):
        pass

    @abstractmethod
    def stop(self):
        pass


class NoOpsPhase(AbstractPhase[Any], ABC):

    def __init__(self, phase_id, stop_status):
        super().__init__(phase_id)
        self._stop_status = stop_status

    @property
    def stop_status(self):
        return self._stop_status

    def run(self, env, ctx):
        """No activity on run"""
        pass

    def stop(self):
        """Nothing to stop"""
        pass


class InitPhase(NoOpsPhase):
    ID = 'Init'
    TYPE = 'INIT'

    def __init__(self):
        super().__init__(InitPhase.ID, TerminationStatus.STOPPED)

    @property
    def type(self) -> str:
        return InitPhase.TYPE

    @property
    def run_state(self) -> RunState:
        return RunState.CREATED


class ExecutingPhase(AbstractPhase[E], ABC):
    """
    Abstract base class for phases that execute some work.
    Implementations should provide concrete run() and stop() methods
    that handle their specific execution mechanics.
    """
    TYPE = 'EXEC'

    def __init__(self, phase_id: str, phase_name: Optional[str] = None):
        super().__init__(phase_id, phase_name)

    @property
    def type(self) -> str:
        return ExecutingPhase.TYPE

    @property
    def run_state(self) -> RunState:
        return RunState.EXECUTING

    @property
    def stop_status(self):
        return TerminationStatus.STOPPED

    @abstractmethod
    def run(self, env: E, ctx):
        """
        Execute the phase's work.

        Args:
            env (E): External run environment
            ctx (RunContext): Context object related to the given phase

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
    ID = 'term'
    TYPE = 'TERMINAL'

    def __init__(self):
        super().__init__(TerminalPhase.ID, TerminationStatus.NONE)

    @property
    def type(self) -> str:
        return TerminalPhase.TYPE

    @property
    def run_state(self) -> RunState:
        return RunState.ENDED


class WaitWrapperPhase(AbstractPhase[E]):

    def __init__(self, wrapped_phase: Phase[E]):
        super().__init__(wrapped_phase.id)
        self.wrapped_phase = wrapped_phase
        self._run_event = Event()

    @property
    def type(self) -> str:
        return self.wrapped_phase.type

    @property
    def run_state(self) -> RunState:
        return self.wrapped_phase.run_state

    @property
    def stop_status(self):
        return self.wrapped_phase.stop_status

    def wait(self, timeout):
        self._run_event.wait(timeout)

    def run(self, env, ctx):
        self._run_event.set()
        self.wrapped_phase.run(env, ctx)

    def stop(self):
        self.wrapped_phase.stop()


class RunContext(ABC):
    """Members to be added later"""


class Phaser(Generic[E]):

    def __init__(self, phases: Iterable[Phase[E]], lifecycle=None, *, timestamp_generator=util.utc_now):
        self._key_to_phase: Dict[str, Phase[E]] = unique_phases_to_dict(phases)
        self._timestamp_generator = timestamp_generator
        self._transition_lock = Condition()
        self.transition_hook: Optional[Callable[[PhaseRun, PhaseRun, int], None]] = None
        # Guarded by the transition/state lock:
        self._lifecycle = lifecycle or Lifecycle()
        self._current_phase = None
        self._stop_status = TerminationStatus.NONE
        self._abort = False
        self._termination: Optional[TerminationInfo] = None
        # ----------------------- #

    @property
    def current_phase(self):
        return self._current_phase

    def get_phase(self, phase_id, phase_type: str = None):
        phase = self._key_to_phase.get(phase_id)
        if phase is None:
            raise KeyError(f"No phase found with id '{phase_id}'")

        if phase_type is not None and phase.type != phase_type:
            raise ValueError(f"Phase type mismatch: Expected '{phase_type}', but found '{phase.type}'")

        return phase

    @property
    def phases(self) -> Dict[str, Phase]:
        return self._key_to_phase.copy()

    def _term_info(self, termination_status, failure=None, error=None):
        return TerminationInfo(termination_status, self._timestamp_generator(), failure, error)

    def run_info(self) -> Run:
        with self._transition_lock:
            phases = tuple(p.info() for p in self._key_to_phase.values())
            return Run(phases, copy(self._lifecycle), self._termination)

    def prime(self):
        with self._transition_lock:
            if self._current_phase:
                raise InvalidStateError("Primed already")
            self._next_phase(InitPhase())

    def run(self, environment: Optional[E] = None):
        if not self._current_phase:
            raise InvalidStateError('Prime not executed before run')

        class _RunContext(RunContext):
            pass

        for phase in self._key_to_phase.values():
            with self._transition_lock:
                if self._abort:
                    return

                self._next_phase(phase)

            term_info, exc = self._run_handle_errors(phase, environment, _RunContext())

            with self._transition_lock:
                if self._stop_status:
                    self._termination = self._term_info(self._stop_status)
                elif term_info:
                    self._termination = term_info

                if isinstance(exc, BaseException):
                    assert self._termination
                    self._next_phase(TerminalPhase())
                    raise exc
                if self._termination:
                    self._next_phase(TerminalPhase())
                    return

        with self._transition_lock:
            self._termination = self._term_info(TerminationStatus.COMPLETED)
            self._next_phase(TerminalPhase())

    def _run_handle_errors(self, phase: Phase[E], environment: E, run_ctx: RunContext) \
            -> Tuple[Optional[TerminationInfo], Optional[BaseException]]:
        try:
            phase.run(environment, run_ctx)
            return None, None
        except TerminateRun as e:
            return self._term_info(e.term_status), None
        except FailedRun as e:
            return self._term_info(TerminationStatus.FAILED, failure=e.fault), None
        except Exception as e:
            # TODO print exception
            run_error = RunError(e.__class__.__name__, str(e))
            return self._term_info(TerminationStatus.ERROR, error=run_error), e
        except KeyboardInterrupt as e:
            log.warning('keyboard_interruption')
            phase.stop()
            return self._term_info(TerminationStatus.INTERRUPTED), e
        except SystemExit as e:
            # Consider UNKNOWN (or new state DETACHED?) if there is possibility the execution is not completed
            term_status = TerminationStatus.COMPLETED if e.code == 0 else TerminationStatus.FAILED
            return self._term_info(term_status), e

    def _next_phase(self, phase):
        """
        Impl note: The execution must be guarded by the phase lock (except terminal phase)
        """
        assert self._current_phase != phase

        self._current_phase = phase
        self._lifecycle.add_phase_run(PhaseRun(phase.id, phase.run_state, self._timestamp_generator()))
        if self.transition_hook:
            self.execute_transition_hook_safely(self.transition_hook)
        with self._transition_lock:
            self._transition_lock.notify_all()

    def execute_transition_hook_safely(self, transition_hook: Optional[Callable[[PhaseRun, PhaseRun, int], None]]):
        with self._transition_lock:
            lc = copy(self._lifecycle)
            transition_hook(lc.previous_run, lc.current_run, lc.phase_count)

    def stop(self):
        with self._transition_lock:
            if self._termination:
                return

            self._stop_status = self._current_phase.stop_status if self._current_phase else TerminationStatus.STOPPED
            if not self._current_phase or (type(self._current_phase) == InitPhase):
                # Not started yet
                self._abort = True  # Prevent phase transition...
                self._termination = self._term_info(self._stop_status)
                self._next_phase(TerminalPhase())

        self._current_phase.stop()

    def wait_for_transition(self, phase_id=None, run_state=RunState.NONE, *, timeout=None):
        with self._transition_lock:
            while True:
                for run in self._lifecycle.phase_runs:
                    if run.phase_id == phase_id or run.run_state == run_state:
                        return True

                if not self._transition_lock.wait(timeout):
                    return False
                if not phase_id and not run_state:
                    return True
