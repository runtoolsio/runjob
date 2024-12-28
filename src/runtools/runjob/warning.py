import re
from threading import Timer
from typing import Sequence

from runtools.runcore import util
from runtools.runcore.job import (JobRun, InstanceTransitionObserver, JobInstance, JobInstanceMetadata,
                                  InstanceOutputObserver)
from runtools.runcore.output import OutputLine
from runtools.runcore.run import RunState, PhaseRun


def exec_time_exceeded(job_instance: JobInstance, warning_name: str, time: float):
    job_instance.add_observer_transition(_ExecTimeWarning(job_instance, warning_name, time))


def output_matches(job_instance: JobInstance, warning_name: str, regex: str):
    job_instance.add_observer_output(_OutputMatchesWarning(job_instance, warning_name, regex))


def register(job_instance: JobInstance, *, warn_times: Sequence[str] = (), warn_outputs: Sequence[str] = ()):
    for warn_time in warn_times:
        time = util.parse_duration_to_sec(warn_time)
        exec_time_exceeded(job_instance, f"run_time>{time}s", time)

    for warn_output in warn_outputs:
        output_matches(job_instance, f"output=~{warn_output}", warn_output)


class _ExecTimeWarning(InstanceTransitionObserver):

    def __init__(self, job_instance, text, time: float):
        self.job_instance = job_instance
        self.text = text
        self.time = time
        self.timer = None

    def new_instance_phase(self, job_run: JobRun, previous_phase: PhaseRun, new_phase: PhaseRun, ordinal: int):
        if new_phase.run_state == RunState.ENDED:
            if self.timer is not None:
                self.timer.cancel()
        elif ordinal == 2:
            assert self.timer is None
            self.timer = Timer(self.time, self._check)
            self.timer.start()

    def _check(self):
        if self.job_instance.snapshot().lifecycle.run_state != RunState.ENDED:
            self.job_instance.status_tracker.warning(self.text)

    def __repr__(self):
        return "{}({!r}, {!r}, {!r})".format(
            self.__class__.__name__, self.job_instance, self.text, self.time)


class _OutputMatchesWarning(InstanceOutputObserver):

    def __init__(self, job_instance, text, regex):
        self.job_instance = job_instance
        self.text = text
        self.regex = re.compile(regex)

    def new_instance_output(self, instance_meta: JobInstanceMetadata, output_line: OutputLine):
        m = self.regex.search(output_line.text)
        if m:
            self.job_instance.status_tracker.warning(self.text)
