from threading import Event
from typing import Optional

from runtools.runcore.common import InvalidStateError
from runtools.runcore.output import OutputLine
from runtools.runcore.run import RunState, TerminationStatus, control_api
from runtools.runjob.instance import JobInstanceContext
from runtools.runjob.output import OutputSink, OutputContext
from runtools.runjob.phase import BasePhase, ExecutionTerminated


class FakeContext(OutputContext, OutputSink):

    @property
    def output_sink(self):
        return self

    def _process_output(self, output_line):
        pass


class TestPhase(BasePhase[JobInstanceContext]):
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
        super().__init__(phase_id, TestPhase.TYPE, RunState.PENDING if wait else RunState.EXECUTING, name)
        self.wait: Optional[Event] = Event() if wait else None
        self.output_text = output_text
        self.exception = raise_exc
        self.fail = fail
        self.completed = False

    @control_api
    def release(self):
        """Release a waiting phase"""
        if self.wait:
            self.wait.set()
        else:
            raise InvalidStateError('Wait not set')

    @control_api
    @property
    def is_released(self):
        return self.wait.is_set()

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
