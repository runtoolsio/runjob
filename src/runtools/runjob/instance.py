"""
This module contains the default implementation of the `RunnableJobInstance` interface from the `inst` module.
A job instance is executed by calling the `run` method. The method call returns after the instance terminates.
It is possible to register observers directly in the module. These are then notified about events from all
job instances. An alternative for multi-observing is to use the featured context from the `featurize` module.

This implementation adds a few features not explicitly defined in the interface:

Coordination
------------
Job instances can be coordinated with each other or with any external logic. Coordinated instances are typically
affected in ways that might require them to wait for a condition, or they might be discarded if necessary. The
coordination logic heavily relies on global synchronization.

Some examples of coordination include:
  - Pending latch:
    Several instances can be started simultaneously by sending a release request to the corresponding group.
  - Serial execution:
    Instances of the same job or group execute sequentially.
  - Parallel execution limit:
    Only N number of instances can execute in parallel.
  - Overlap forbidden:
    Parallel execution of instances of the same job can be restricted.
  - Dependency:
    An instance can start only if another specific instance is active.

Global Synchronization
----------------------
For coordination to function correctly, a mechanism that allows the coordinator to utilize some form of
shared lock is often essential. Job instances attempt to acquire this lock each time the coordination logic runs.
The lock remains held when the execution state changes. This is crucial because the current state of coordinated
instances often dictates the coordination action, and using a lock ensures that the 'check-then-act' sequence
on the execution states is atomic, preventing race conditions.


IMPLEMENTATION NOTES

State lock
----------
1. Atomic update of the job instance state (i.e. error object + failure state)
2. Consistent execution state notification order
3. No race condition on state observer notify on register

"""
import logging

from runtools.runcore import util
from runtools.runcore.job import (JobInstance, JobRun, InstanceTransitionObserver,
                                  InstanceOutputObserver, JobInstanceMetadata)
from runtools.runcore.output import Output, TailNotSupportedError, Mode
from runtools.runcore.run import PhaseRun, Outcome, RunState
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY, ObservableNotification
from runtools.runjob import Phaser
from runtools.runjob.track import MonitoredEnvironment, StatusTracker
from runtools.runjob.output import OutputSink, InMemoryTailBuffer

log = logging.getLogger(__name__)


def log_observer_error(observer, args, exc):
    log.error("event=[observer_error] observer=[%s], args=[%s] error=[%s]", observer, args, exc, exc_info=True)


_transition_observer = ObservableNotification[InstanceTransitionObserver](error_hook=log_observer_error)
_output_observers = ObservableNotification[InstanceOutputObserver](error_hook=log_observer_error)


class _JobOutput(Output, OutputSink):

    def __init__(self, metadata, tail_buffer=None):
        super().__init__()
        self.metadata = metadata
        self.tail_buffer = tail_buffer
        self.output_notification = ObservableNotification[InstanceOutputObserver](error_hook=log_observer_error)

    def _process_output(self, output_line):
        if self.tail_buffer:
            self.tail_buffer.add_line(output_line)
        self.output_notification.observer_proxy.new_instance_output(self.metadata, output_line)

    def tail(self, mode: Mode = Mode.TAIL, max_lines: int = 0):
        if not self.tail_buffer:
            raise TailNotSupportedError

        return self.tail_buffer.get_lines(mode, max_lines)


class JobEnvironment(MonitoredEnvironment):

    def __init__(self, status_tracker, output: _JobOutput):
        self._status_tracker = status_tracker
        self._output: _JobOutput = output

    @property
    def status_tracker(self):
        return self._status_tracker

    @property
    def output_sink(self) -> OutputSink:
        return self._output


def create(job_id, phases, status_tracker=None,
           *, instance_id=None, run_id=None, tail_buffer=None,
           **user_params) -> JobInstance:
    if not job_id:
        raise ValueError("Job ID is mandatory")

    instance_id = instance_id or util.unique_timestamp_hex()
    run_id = run_id or instance_id
    phaser = Phaser(phases)
    tail_buffer = tail_buffer or InMemoryTailBuffer(max_capacity=10)
    status_tracker = status_tracker or StatusTracker()
    inst = _JobInstance(job_id, instance_id, run_id, phaser, tail_buffer, status_tracker, user_params)
    return inst


class _JobInstance(JobInstance):

    def __init__(self, job_id, instance_id, run_id, phaser, tail_buffer, status_tracker, user_params):
        parameters = {}
        self._metadata = JobInstanceMetadata(job_id, run_id or instance_id, instance_id, parameters, user_params)
        self._phaser = phaser
        self._output = _JobOutput(self._metadata, tail_buffer)
        self._environment = JobEnvironment(status_tracker, self._output)

        self._transition_notification = (
            ObservableNotification[InstanceTransitionObserver](error_hook=log_observer_error))

        # TODO Move the below out of constructor?
        self._phaser.transition_hook = self._transition_hook
        self._phaser.prime()  # TODO

    def _log(self, event: str, msg: str = '', *params):
        return ("event=[{}] job_run=[{}@{}] " + msg).format(
            event, self._metadata.job_id, self._metadata.run_id, *params)

    @property
    def metadata(self):
        return self._metadata

    @property
    def status_tracker(self):
        return self._environment.status_tracker

    @property
    def current_phase(self):
        return self._phaser.current_phase_id

    @property
    def phases(self):
        return self._phaser.phases

    @property
    def output(self):
        return self._environment.output_sink

    def get_phase(self, phase_id: str, phase_type: str = None):
        return self._phaser.get_phase(phase_id, phase_type)

    def job_run(self) -> JobRun:
        tracked_task = self._environment.status_tracker.to_status() if self.status_tracker else None
        return JobRun(self.metadata, self._phaser.run_info(), tracked_task)

    def run(self):
        self._transition_notification.add_observer(_transition_observer.observer_proxy)
        self._output.output_notification.add_observer(_output_observers.observer_proxy)

        try:
            self._phaser.run(self._environment)
        finally:
            self._transition_notification.remove_observer(_transition_observer.observer_proxy)
            self._output.output_notification.remove_observer(_output_observers.observer_proxy)

    def stop(self):
        """
        Cancel not yet started execution or stop started execution.
        Due to synchronous design there is a small window when an execution can be stopped before it is started.
        All execution implementations must cope with such scenario.
        """
        self._phaser.stop()

    def interrupted(self):
        """
        Cancel not yet started execution or interrupt started execution.
        Due to synchronous design there is a small window when an execution can be interrupted before it is started.
        All execution implementations must cope with such scenario.
        """
        self._phaser.stop()  # TODO Interrupt

    def wait_for_transition(self, phase_name=None, run_state=RunState.NONE, *, timeout=None):
        return self._phaser.wait_for_transition(phase_name, run_state, timeout=timeout)

    def add_observer_transition(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, notify_on_register=False):
        if notify_on_register:
            def add_and_notify_callback(*args):
                self._transition_notification.add_observer(observer, priority)
                observer.new_instance_phase(self._phaser.run_info(), *args)

            self._phaser.execute_transition_hook_safely(add_and_notify_callback)
        else:
            self._transition_notification.add_observer(observer, priority)

    def remove_observer_transition(self, callback):
        self._transition_notification.remove_observer(callback)

    def _transition_hook(self, old_phase: PhaseRun, new_phase: PhaseRun, ordinal):
        """Executed under phaser transition lock"""
        snapshot = self.job_run()
        termination = snapshot.termination

        log.info(self._log('new_phase', "new_phase=[{}] prev_phase=[{}] run_state=[{}]",
                           new_phase.phase_id, old_phase.phase_id, new_phase.run_state.name))

        if termination:
            if termination.status.is_outcome(Outcome.NON_SUCCESS):
                log.warning(self._log('run_incomplete', "termination_status=[{}]", termination.status.name))

            if termination.error:
                log.error(self._log('unexpected_error', "error_type=[{}] reason=[{}]",
                                    termination.error.category, termination.error.reason))
            elif termination.failure:
                log.warning(self._log('run_failed', "error_type=[{}] reason=[{}]",
                                      termination.failure.category, termination.failure.reason))

            log.info(self._log('run_terminated', "termination_status=[{}]", termination.status.name))

        self._transition_notification.observer_proxy.new_instance_phase(snapshot, old_phase, new_phase, ordinal)

    def add_observer_output(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._output.output_notification.add_observer(observer, priority)

    def remove_observer_output(self, observer):
        self._output.output_notification.remove_observer(observer)


def register_transition_observer(observer, priority=DEFAULT_OBSERVER_PRIORITY):
    global _transition_observer
    _transition_observer.add_observer(observer, priority)


def deregister_transition_observer(observer):
    global _transition_observer
    _transition_observer.remove_observer(observer)
