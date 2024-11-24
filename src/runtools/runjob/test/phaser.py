from threading import Event
from typing import Optional

from runtools.runcore.common import InvalidStateError
from runtools.runcore.run import RunState, TerminationStatus, TerminateRun
from runtools.runjob.phaser import RunContext, AbstractPhase


class FakeRunContext(RunContext):

    def __init__(self):
        self.output = []

    @property
    def task_tracker(self):
        return None

    def new_output(self, output, is_err=False):
        self.output.append((output, is_err))


class TestPhase(AbstractPhase):

    def __init__(self, phase_id='test_phase', *, wait=False, output_text=None, raise_exc=None):
        super().__init__(phase_id)
        self.fail = False
        self.failed_run = None
        self.exception = raise_exc
        self.wait: Optional[Event] = Event() if wait else None
        self.completed = False
        self.output_text = output_text

    @property
    def type(self) -> str:
        return 'TEST'

    @property
    def run_state(self) -> RunState:
        return RunState.PENDING if self.wait else RunState.EXECUTING

    @property
    def stop_status(self):
        if self.wait:
            return TerminationStatus.CANCELLED
        else:
            return TerminationStatus.STOPPED

    def release(self):
        if self.wait:
            self.wait.set()
        else:
            raise InvalidStateError('Wait not set')

    def run(self, run_ctx):
        if self.wait:
            self.wait.wait(2)
        if self.output_text:
            run_ctx.new_output(self.output_text)
        if self.exception:
            raise self.exception
        if self.failed_run:
            raise self.failed_run
        if self.fail:
            raise TerminateRun(TerminationStatus.FAILED)
        self.completed = True

    def stop(self):
        if self.wait:
            self.wait.set()
