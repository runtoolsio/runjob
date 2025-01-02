from threading import Event
from typing import Optional

from runtools.runcore.common import InvalidStateError
from runtools.runcore.run import RunState, TerminationStatus, TerminateRun, control_api, Phase
from runtools.runjob.output import OutputSink
from runtools.runjob.phaser import RunContext
from runtools.runjob.track import MonitoredEnvironment, StatusTracker


class TestEnvironment(MonitoredEnvironment):

    @property
    def status_tracker(self):
        return StatusTracker()

    @property
    def output_sink(self):
        class TestSink(OutputSink):
            def _process_output(self, output_line):
                pass

        return TestSink()


class FakeRunContext(RunContext):

    def __init__(self):
        self.output = []

    @property
    def status_tracker(self):
        return None

    def new_output(self, output, is_err=False):
        self.output.append((output, is_err))


class TestPhase(Phase):

    def __init__(self, phase_id='test_phase', *, wait=False, output_text=None, raise_exc=None):
        self._id = phase_id
        self.fail = False
        self.failed_run = None
        self.exception = raise_exc
        self.wait: Optional[Event] = Event() if wait else None
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

    def run(self, env, run_ctx):
        if self.wait:
            self.wait.wait(2)
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
