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
from contextlib import contextmanager
from contextvars import ContextVar
from threading import Thread
from typing import Callable, Optional, List, Iterable

from runtools.runcore.job import (JobInstance, JobRun, InstanceOutputObserver, JobInstanceMetadata,
                                  InstanceTransitionObserver,
                                  InstanceTransitionEvent, InstanceOutputEvent, InstanceLifecycleObserver,
                                  InstanceLifecycleEvent)
from runtools.runcore.run import Outcome, Fault, PhaseTransitionEvent, Stage, StopReason
from runtools.runcore.util import utc_now
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY, ObservableNotification
from runtools.runjob.output import OutputContext, OutputSink, InMemoryTailBuffer, OutputRouter
from runtools.runjob.phase import SequentialPhase, Phase
from runtools.runjob.track import StatusTracker

log = logging.getLogger(__name__)

ROOT_PHASE_ID = "root"

LIFECYCLE_OBSERVER_ERROR = "LIFECYCLE_OBSERVER_ERROR"
TRANSITION_OBSERVER_ERROR = "TRANSITION_OBSERVER_ERROR"
OUTPUT_OBSERVER_ERROR = "OUTPUT_OBSERVER_ERROR"

TransitionObserverErrorHandler = Callable[[InstanceTransitionObserver, InstanceTransitionEvent, Exception], None]
OutputObserverErrorHandler = Callable[[InstanceOutputObserver, InstanceOutputEvent, Exception], None]

current_job_instance: ContextVar[Optional[JobInstanceMetadata]] = ContextVar('current_job_instance', default=None)


class _JobInstanceLogFilter(logging.Filter):
    """Filter that only allows logs from the current job instance"""

    def __init__(self, instance_id):
        super().__init__()
        self.instance_id = instance_id

    def filter(self, record):
        metadata = current_job_instance.get()
        if metadata is None:
            return False

        return self.instance_id == metadata.instance_id


class JobInstanceContext(OutputContext):

    def __init__(self, metadata, environment, status_tracker, output_sink):
        self._metadata = metadata
        self._environment = environment
        self._status_tracker = status_tracker
        self._output_sink = output_sink

    @property
    def metadata(self) -> JobInstanceMetadata:
        return self._metadata

    @property
    def environment(self):
        return self._environment

    @property
    def status_tracker(self) -> StatusTracker:
        return self._status_tracker

    @property
    def output_sink(self) -> OutputSink:
        return self._output_sink


JobInstanceHook = Callable[[JobInstanceContext], None]


def create(instance_id, environment, phases=None,
           *, root_phase=None,
           output_sink=None, output_router=None, status_tracker=None,
           pre_run_hook: Optional[JobInstanceHook] = None,
           post_run_hook: Optional[JobInstanceHook] = None,
           stage_observers: Iterable[InstanceLifecycleObserver] = (),
           transition_observer_error_handler: Optional[TransitionObserverErrorHandler] = None,
           output_observer_error_handler: Optional[OutputObserverErrorHandler] = None,
           **user_params) -> JobInstance:
    """

    Example of pre_run_hook:
        ```
        def output_to_status_setup_hook(ctx: JobInstanceContext):
            parsers = [IndexParser({'event': 0})]
            ctx.output_sink.add_observer(OutputToStatusTransformer(ctx.status_tracker, parsers=parsers))
        ```
    :return:
    """
    if not instance_id:
        raise ValueError("Instance ID is mandatory")
    if phases is not None and root_phase is not None:
        raise ValueError("Cannot provide both 'phases' and 'root_phase'. They are mutually exclusive.")
    if phases is None and root_phase is None:
        raise ValueError("Must provide either 'phases' (for a sequential job) or a 'root_phase'.")

    if phases:
        root_phase = SequentialPhase(ROOT_PHASE_ID, phases)
    output_sink = output_sink or OutputSink()
    output_router = output_router or OutputRouter(tail_buffer=InMemoryTailBuffer(max_capacity=50))
    status_tracker = status_tracker or StatusTracker()
    inst = _JobInstance(instance_id, root_phase, environment, output_sink, output_router, status_tracker,
                        pre_run_hook, post_run_hook,
                        transition_observer_error_handler, output_observer_error_handler,
                        user_params)
    for so in stage_observers:
        inst.add_observer_lifecycle(so)
    # noinspection PyProtectedMember
    inst._post_created()
    return inst


class _JobInstance(JobInstance):

    def __init__(self, instance_id, root_phase, env, output_sink, output_router, status_tracker,
                 pre_run_hook, post_run_hook,
                 phase_update_observer_err_hook, output_observer_err_hook,
                 user_params):
        self._metadata = JobInstanceMetadata(instance_id, user_params)
        self._root_phase: Phase = root_phase
        self._output_sink: OutputSink = output_sink
        self._output_router = output_router
        self._ctx = JobInstanceContext(self._metadata, env, status_tracker, self._output_sink)
        self._pre_run_hook = pre_run_hook
        self._post_run_hook = post_run_hook

        self._lifecycle_notification = (
            ObservableNotification[InstanceLifecycleObserver](error_hook=phase_update_observer_err_hook,
                                                              force_reraise=True))
        self._transition_notification = (
            ObservableNotification[InstanceTransitionObserver](error_hook=phase_update_observer_err_hook,
                                                               force_reraise=True))
        self._output_notification = (
            ObservableNotification[InstanceOutputObserver](error_hook=output_observer_err_hook, force_reraise=True))
        self._faults: List[Fault] = []

    def _post_created(self):
        self._root_phase.add_phase_observer(self._on_phase_update)

    def _log(self, event: str, msg: str = '', *params):
        return ("{} instance=[{}] env=[{}] " + msg).format(
            event, self._metadata.instance_id, self._ctx.environment.env_id if self._ctx.environment else 'N/A',
            *params)

    @property
    def metadata(self):
        return self._metadata

    @property
    def status_tracker(self):
        return self._ctx.status_tracker

    def find_phase_control(self, phase_filter):
        return self._root_phase.find_phase_control(phase_filter)

    @property
    def output(self):
        return self._output_router

    def snapshot(self) -> JobRun:
        root_phase_detail = self._root_phase.detail()
        status = self._ctx.status_tracker.to_status() if self.status_tracker else None
        return JobRun(
            self.metadata,
            root_phase_detail.lifecycle,
            root_phase_detail.children,
            self._output_router.locations if self._output_router else None,
            tuple(self._faults),
            status
        )

    @contextmanager
    def _job_instance_context(self):
        token = current_job_instance.set(self.metadata)
        try:
            yield
        finally:
            current_job_instance.reset(token)

    def _process_output(self, output_line):
        event = InstanceOutputEvent(self._metadata, output_line, utc_now())
        try:
            self._output_notification.observer_proxy.instance_output_update(event)
        except ExceptionGroup as eg:
            log.error("[output_observer_error]", exc_info=eg)
            for e in eg.exceptions:
                self._faults.append(Fault.from_exception(OUTPUT_OBSERVER_ERROR, e))

    def run(self):
        with self._job_instance_context():
            with self._output_router as router:
                with self._output_sink.observer_context(self._process_output, router):
                    with self._output_sink.capture_logs_from(logging.getLogger(), log_filter=_JobInstanceLogFilter(self.id)):
                        try:
                            self._exec_pre_run_hook()
                            self._root_phase.run(self._ctx)
                        finally:
                            self._exec_post_run_hook()

    def _exec_pre_run_hook(self):
        if self._pre_run_hook:
            try:
                self._pre_run_hook(self._ctx)
            except Exception as e:
                log.error(self._log('pre_run_hook_error', "error=[{}]", str(e)), exc_info=True)

    def _exec_post_run_hook(self):
        if self._post_run_hook:
            try:
                self._post_run_hook(self._ctx)
            except Exception as e:
                log.error(self._log('post_run_hook_error', "error=[{}]", str(e)), exc_info=True)

    def run_in_new_thread(self, daemon=False):
        """
        Run the job.

        This method is not expected to raise any errors. In case of any failure the error details can be retrieved
        by calling `exec_error` method.
        """

        t = Thread(target=self.run, daemon=daemon)
        t.start()

    def stop(self, reason=StopReason.STOPPED):
        """
        Cancel not yet started execution or stop started execution.
        Due to synchronous design there is a small window when an execution can be stopped before it is started.
        All execution implementations must cope with such scenario.
        """
        self._root_phase.stop(reason)

    def add_observer_lifecycle(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, reply_last_event=False):
        self._lifecycle_notification.add_observer(observer, priority)
        if reply_last_event:
            pass  # TODO

    def remove_observer_lifecycle(self, observer):
        self._lifecycle_notification.remove_observer(observer)

    def add_observer_transition(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._transition_notification.add_observer(observer, priority)

    def remove_observer_transition(self, observer):
        self._transition_notification.remove_observer(observer)

    def _on_phase_update(self, e: PhaseTransitionEvent):
        log.debug(self._log('instance_phase_update', "event=[{}]", e))

        is_root_phase = e.phase_detail.phase_id == self._root_phase.id
        if is_root_phase:
            if e.new_stage == Stage.RUNNING:
                log.info(self._log('run_started'))
            log.debug(self._log('instance_lifecycle_update', "new_stage=[{}]", e.new_stage))

            if term := e.phase_detail.lifecycle.termination:
                if term.status.is_outcome(Outcome.NON_SUCCESS):
                    log.warning(self._log('run_unsuccessful', "termination=[{}]", term))
                else:
                    log.info(self._log('run_successful', "termination=[{}]", term))

        snapshot = self.snapshot()
        if is_root_phase:
            try:
                event = InstanceLifecycleEvent(self.metadata, snapshot, e.new_stage, e.timestamp)
                self._lifecycle_notification.observer_proxy.instance_lifecycle_update(event)
            except ExceptionGroup as eg:
                log.error("[stage_observer_error]", exc_info=eg)
                for exc in eg.exceptions:
                    self._faults.append(Fault.from_exception(LIFECYCLE_OBSERVER_ERROR, exc))
        try:
            event = InstanceTransitionEvent(self.metadata, snapshot, is_root_phase, e.phase_detail.phase_id,
                                            e.new_stage, e.timestamp)
            self._transition_notification.observer_proxy.new_instance_transition(event)
        except ExceptionGroup as eg:
            log.error("[transition_observer_error]", exc_info=eg)
            for exc in eg.exceptions:
                self._faults.append(Fault.from_exception(TRANSITION_OBSERVER_ERROR, exc))

    def add_observer_output(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._output_notification.add_observer(observer, priority)

    def remove_observer_output(self, observer):
        self._output_notification.remove_observer(observer)
