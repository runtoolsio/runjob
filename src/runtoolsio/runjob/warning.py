import re
from threading import Timer
from typing import Sequence

from tarotools.taro import util
from tarotools.taro.job import JobRun, InstanceTransitionObserver, JobInstance, \
    InstanceOutputObserver, JobInstanceMetadata
from tarotools.taro.run import RunState, PhaseRun, PhaseMetadata


def exec_time_exceeded(job_instance: JobInstance, warning_name: str, time: float):
    job_instance.add_observer_transition(_ExecTimeWarning(job_instance, warning_name, time))


def output_matches(job_instance: JobInstance, warning_name: str, regex: str):
    job_instance.add_observer_output(_OutputMatchesWarning(job_instance, warning_name, regex))


def register(job_instance: JobInstance, *, warn_times: Sequence[str] = (), warn_outputs: Sequence[str] = ()):
    for warn_time in warn_times:
        time = util.parse_duration_to_sec(warn_time)
        exec_time_exceeded(job_instance, f"exec_time>{time}s", time)

    for warn_output in warn_outputs:
        output_matches(job_instance, f"output=~{warn_output}", warn_output)


class _ExecTimeWarning(InstanceTransitionObserver):

    def __init__(self, job_instance, name, time: float):
        self.job_instance = job_instance
        self.name = name
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
        if self.job_instance.job_run_info().run.lifecycle.run_state != RunState.ENDED:
            self.job_instance.task_tracker.warning(f"Slow execution - exceeded {self.time} seconds")

    def __repr__(self):
        return "{}({!r}, {!r}, {!r})".format(
            self.__class__.__name__, self.job_instance, self.name, self.time)


class _OutputMatchesWarning(InstanceOutputObserver):

    def __init__(self, job_instance, w_id, regex):
        self.job_instance = job_instance
        self.id = w_id
        self.regex = re.compile(regex)

    def new_instance_output(self, instance_meta: JobInstanceMetadata, phase: PhaseMetadata, output: str, is_err: bool):
        m = self.regex.search(output)
        if m:
            self.job_instance.task_tracker.warning(output)
