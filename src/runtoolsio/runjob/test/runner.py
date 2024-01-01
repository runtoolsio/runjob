from runtoolsio.runcore import PhaseNames, ExecutingPhase, Phaser
from runtoolsio.runcore.run import Lifecycle
from runtoolsio.runcore.test.job import AbstractBuilder

from runtoolsio.runjob.coordination import ApprovalPhase
from runtoolsio.runjob.runner import RunnerJobInstance
from runtoolsio.runjob.test.execution import TestExecution


class TestJobInstanceBuilder(AbstractBuilder):

    def __init__(self, job_id='j1', run_id=None, system_params=None, user_params=None):
        super().__init__(job_id, run_id, system_params, user_params)
        self.phases = []

    def add_approval_phase(self, name=PhaseNames.APPROVAL):
        self.phases.append(ApprovalPhase(name, 2))
        return self

    def add_exec_phase(self, name='EXEC', *, output_text=None):
        self.phases.append(ExecutingPhase(name, TestExecution(wait=True, output_text=output_text)))
        return self

    def build(self) -> RunnerJobInstance:
        lifecycle = Lifecycle()
        phaser = Phaser(self.phases, lifecycle)
        return RunnerJobInstance(self.metadata.job_id, phaser, lifecycle, run_id=self.metadata.run_id,
                                 **self.metadata.user_params)
