import logging
import traceback
from abc import ABC, abstractmethod
from copy import copy
from threading import Condition, Event
from typing import Any, Dict, Iterable, Optional, Callable, Tuple, Generic

import sys

from runtools.runcore import util
from runtools.runcore.common import InvalidStateError
from runtools.runcore.run import Phase, PhaseRun, Lifecycle, TerminationStatus, TerminationInfo, Run, \
    TerminateRun, FailedRun, Fault, RunState, C, PhaseExecutionError

log = logging.getLogger(__name__)


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
    ID = 'Init'
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
    ID = 'term'
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


UNCAUGHT_PHASE_RUN_EXCEPTION = "UNCAUGHT_PHASE_RUN_EXCEPTION"


class Phaser(Generic[C]):

    def __init__(self, phases: Iterable[Phase[C]], lifecycle=None, *, timestamp_generator=util.utc_now):
        self.transition_hook: Optional[Callable[[PhaseRun, PhaseRun, int], None]] = None
        self._key_to_phase: Dict[str, Phase[C]] = unique_phases_to_dict(phases)
        self._timestamp_generator = timestamp_generator
        self._lock = Condition()
        # Guarded by the lock:
        self._lifecycle = lifecycle or Lifecycle()
        self._started = False
        self._current_phase = None
        self._abort = False
        self._stop_status = TerminationStatus.NONE
        self._termination_info: Optional[TerminationInfo] = None
        # ----------------------- #

    @property
    def current_phase(self):
        return self._current_phase

    def get_phase(self, phase_id, phase_type: str = None):
        phase = self._key_to_phase.get(phase_id)
        if phase is None:
            raise KeyError(f"No phase found with id '{phase_id}'")

        if phase_type is not None and phase.type != phase_type:
            # TODO More specific exc
            raise ValueError(f"Phase type mismatch: Expected '{phase_type}', but found '{phase.type}'")

        return phase

    @property
    def phases(self) -> Dict[str, Phase]:
        return self._key_to_phase.copy()

    def _term_info(self, termination_status, failure=None, error=None):
        return TerminationInfo(termination_status, self._timestamp_generator(), failure, error)

    def snapshot(self) -> Run:
        with self._lock:
            phases = tuple(p.info for p in self._key_to_phase.values())
            return Run(phases, copy(self._lifecycle), self._termination_info)

    def prime(self):
        with self._lock:
            if self._abort:
                return
            if self._current_phase:
                raise InvalidStateError("Primed already")
            self._next_phase(InitPhase())

        self._execute_transition_hook()

    def run(self, ctx: Optional[C] = None):
        if self._current_phase is None:
            raise InvalidStateError('Prime not executed before run')

        with self._lock:
            if self._started:
                raise InvalidStateError('The run has been already started')
            if self._abort:
                return
            self._started = True

        term_info, exc = None, None
        for phase in self._key_to_phase.values():
            with self._lock:
                if term_info or exc or self._stop_status:
                    break

                self._next_phase(phase)

            self._execute_transition_hook()  # Hook exec without lock
            term_info, exc = self._run_handle_errors(phase, ctx)

        with self._lock:
            # Set termination info and terminal phase 'atomically'
            if self._stop_status:
                term_info = self._term_info(self._stop_status)
            self._termination_info = term_info or self._term_info(TerminationStatus.COMPLETED)
            self._next_phase(TerminalPhase())

        self._execute_transition_hook()  # Hook exec without lock

        if exc:
            raise exc

    def _next_phase(self, phase):
        """
        Impl note: The execution must be guarded by the phase lock (except terminal phase)
        """
        assert self._current_phase != phase

        self._current_phase = phase
        self._lifecycle.add_phase_run(PhaseRun(phase.id, phase.run_state, self._timestamp_generator()))
        self._lock.notify_all()

    def _run_handle_errors(self, phase: Phase[C], ctx: Optional[C]) \
            -> Tuple[Optional[TerminationInfo], Optional[BaseException]]:
        try:
            phase.run(ctx)
            return None, None
        except TerminateRun as e:
            return self._term_info(e.term_status), None
        except FailedRun as e:
            return self._term_info(TerminationStatus.FAILED, failure=e.fault), None
        except Exception as e:
            run_error = Fault.from_exception(UNCAUGHT_PHASE_RUN_EXCEPTION, e)
            wrapped_exc = PhaseExecutionError(phase.id)
            wrapped_exc.__cause__ = e
            return self._term_info(TerminationStatus.ERROR, error=run_error), wrapped_exc
        except KeyboardInterrupt as e:
            phase.stop()
            return self._term_info(TerminationStatus.INTERRUPTED), e
        except SystemExit as e:
            # Consider UNKNOWN (or new state DETACHED?) if there is possibility the execution is not completed
            term_status = TerminationStatus.COMPLETED if e.code == 0 else TerminationStatus.FAILED
            return self._term_info(term_status), e

    def _execute_transition_hook(self):
        if self.transition_hook is None:
            return

        try:
            self.transition_hook(self._lifecycle.previous_run, self._lifecycle.current_run, self._lifecycle.phase_count)
        except Exception:
            # The exception is not propagated to prevent breaking the run
            # The implementer should prevent the hook to raise any exceptions
            # TODO Add the exception to the run context
            traceback.print_exc(file=sys.stderr)

    def stop(self):
        with self._lock:
            if self._termination_info:
                return

            self._stop_status = self._current_phase.stop_status if self._current_phase else TerminationStatus.STOPPED
            if not self._started:
                self._abort = True
                self._termination_info = self._term_info(self._stop_status)
                self._next_phase(TerminalPhase())

        self._current_phase.stop()  # Set stop status prevents next phase execution
        if self._abort:
            self._execute_transition_hook()

    def wait_for_transition(self, phase_id=None, run_state=RunState.NONE, *, timeout=None):
        with self._lock:
            while True:
                for run in self._lifecycle.phase_runs:
                    if run.phase_id == phase_id or run.run_state == run_state:
                        return True

                if not self._lock.wait(timeout):
                    return False
                if not phase_id and not run_state:
                    return True
