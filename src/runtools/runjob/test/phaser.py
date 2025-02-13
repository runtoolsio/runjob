from threading import Event
from typing import Optional

from runtools.runcore.common import InvalidStateError
from runtools.runcore.output import OutputLine
from runtools.runcore.run import RunState, TerminationStatus, TerminateRun, control_api, Phase
from runtools.runjob.instance import JobInstanceContext
from runtools.runjob.output import OutputSink, OutputContext
from runtools.runjob.phaser import BasePhase, ExecutionTerminated


class FakeContext(OutputContext, OutputSink):

    @property
    def output_sink(self):
        return self

    def _process_output(self, output_line):
        pass


class TestPhase(Phase[JobInstanceContext]):

    def __init__(self, phase_id='test_phase', *, wait=False, output_text=None, raise_exc=None):
        self._id = phase_id
        self.wait: Optional[Event] = Event() if wait else None
        self.output_text = output_text
        self.exception = raise_exc
        self.fail = False
        self.failed_run = None
        self.completed = False

    @property
    def id(self):
        return self._id

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

    @control_api
    def release(self):
        if self.wait:
            self.wait.set()
        else:
            raise InvalidStateError('Wait not set')

    def run(self, ctx: Optional[JobInstanceContext]):
        if self.wait:
            self.wait.wait(2)
        if ctx and self.output_text:
            ctx.output_sink.new_output(OutputLine(self.output_text, False))
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


class TestPhaseV2(BasePhase[JobInstanceContext]):
    """
    Test implementation of a V2 Phase for use in testing scenarios.
    Supports waiting, output generation, and various failure modes.
    """
    TYPE = 'TEST'

    def __init__(self, phase_id: str = 'test_phase', *,
                 wait: bool = False,
                 output_text: Optional[str] = None,
                 raise_exc = None,
                 fail = False,
                 name: Optional[str] = None):
        super().__init__(phase_id, TestPhaseV2.TYPE, RunState.PENDING if wait else RunState.EXECUTING, name)
        self.wait: Optional[Event] = Event() if wait else None
        self.output_text = output_text
        self.exception = raise_exc
        self.fail = fail
        self.completed = False

    def release(self):
        """Release a waiting phase"""
        if self.wait:
            self.wait.set()
        else:
            raise InvalidStateError('Wait not set')

    def _run(self, ctx: Optional[JobInstanceContext]):
        if self.wait:
            self.wait.wait(2)

        if ctx and self.output_text and isinstance(ctx, OutputContext):
            ctx.output_sink.new_output(OutputLine(self.output_text, False))

        if self.exception:
            raise self.exception

        if self.fail:
            raise ExecutionTerminated(TerminationStatus.FAILED)

        self.completed = True

    def stop(self):
        """Stop the phase execution by setting the wait event if present"""
        if self.wait:
            self.wait.set()
