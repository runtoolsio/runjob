"""
Default implementation of the JobInstance interface from runcore.

This module provides:
- _JobInstance: Concrete implementation that wraps a root phase with lifecycle management
- JobInstanceContext: Runtime context passed to phases (metadata, environment, status, output)
- create(): Factory function for creating job instances

Job instances execute by calling run(), which:
1. Sets up the execution context (current instance, output routing, log capture)
2. Executes the root phase
3. Notifies observers of lifecycle and transition events
"""
import logging
from contextlib import contextmanager
from contextvars import ContextVar
from threading import Thread
from typing import Callable, Optional, List, Iterable, override

from runtools.runcore.job import (JobInstance, JobRun, JobInstanceMetadata, InstanceNotifications,
                                  InstanceObservableNotifications, InstanceTransitionEvent, InstanceOutputEvent,
                                  InstanceLifecycleObserver, InstanceLifecycleEvent)
from runtools.runcore.run import Fault, PhaseTransitionEvent, Stage, StopReason
from runtools.runcore.util import utc_now
from runtools.runjob.output import OutputContext, OutputSink, InMemoryTailBuffer, OutputRouter
from runtools.runjob.phase import SequentialPhase, Phase
from runtools.runjob.track import StatusTracker

log = logging.getLogger(__name__)

ROOT_PHASE_ID = "root"

LIFECYCLE_OBSERVER_ERROR = "LIFECYCLE_OBSERVER_ERROR"
TRANSITION_OBSERVER_ERROR = "TRANSITION_OBSERVER_ERROR"
OUTPUT_OBSERVER_ERROR = "OUTPUT_OBSERVER_ERROR"

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
           lifecycle_observers: Iterable[InstanceLifecycleObserver] = (),
           lifecycle_error_hook=None, transition_error_hook=None, output_error_hook=None,
           **user_params) -> JobInstance:
    """Create a job instance.

    Args:
        instance_id: Unique identifier for this instance
        environment: Environment configuration
        phases: List of phases for sequential execution (mutually exclusive with root_phase)
        root_phase: Custom root phase (mutually exclusive with phases)
        output_sink: Custom output sink (created automatically if not provided).
                     For text parsing, provide OutputSink(ParsingPreprocessor([KVParser()])).
        output_router: Custom output router (created automatically if not provided)
        status_tracker: Custom status tracker (created automatically if not provided)
        pre_run_hook: Hook called before execution starts
        post_run_hook: Hook called after execution completes
        lifecycle_observers: Observers for lifecycle events
        lifecycle_error_hook: Error handler for lifecycle observer errors
        transition_error_hook: Error handler for transition observer errors
        output_error_hook: Error handler for output observer errors
        **user_params: Additional user-defined parameters stored in metadata
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

    # Auto-wire status tracker as output observer
    output_sink.add_observer(status_tracker)

    inst = _JobInstance(instance_id, root_phase, environment, output_sink, output_router, status_tracker,
                        pre_run_hook, post_run_hook,
                        lifecycle_error_hook, transition_error_hook, output_error_hook,
                        user_params)
    for so in lifecycle_observers:
        inst.notifications.add_observer_lifecycle(so)
    # noinspection PyProtectedMember
    inst._post_created()
    return inst


class _JobInstance(JobInstance):

    def __init__(self, instance_id, root_phase, env, output_sink, output_router, status_tracker,
                 pre_run_hook, post_run_hook,
                 lifecycle_error_hook, transition_error_hook, output_error_hook,
                 user_params):
        self._notifications = InstanceObservableNotifications(
            lifecycle_error_hook=lifecycle_error_hook,
            transition_error_hook=transition_error_hook,
            output_error_hook=output_error_hook,
            force_reraise=True)
        self._metadata = JobInstanceMetadata(instance_id, user_params)
        self._root_phase: Phase = root_phase
        self._output_sink: OutputSink = output_sink
        self._output_router = output_router
        self._ctx = JobInstanceContext(self._metadata, env, status_tracker, self._output_sink)
        self._pre_run_hook = pre_run_hook
        self._post_run_hook = post_run_hook
        self._faults: List[Fault] = []

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        return self._notifications

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

    def to_run(self) -> JobRun:
        status = self._ctx.status_tracker.to_status() if self.status_tracker else None
        return JobRun(
            self.metadata,
            self._root_phase.detail(),
            self._output_router.locations if self._output_router else None,
            tuple(self._faults),
            status
        )

    @contextmanager
    def _instance_scope(self):
        token = current_job_instance.set(self.metadata)
        try:
            yield
        finally:
            current_job_instance.reset(token)

    def _process_output(self, output_line):
        event = InstanceOutputEvent(self._metadata, output_line, utc_now())
        try:
            self._notifications.output_notification.observer_proxy.instance_output_update(event)
        except ExceptionGroup as eg:
            log.error("[output_observer_error]", exc_info=eg)
            for e in eg.exceptions:
                self._faults.append(Fault.from_exception(OUTPUT_OBSERVER_ERROR, e))

    def run(self):
        with self._instance_scope():
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

    def _on_phase_update(self, e: PhaseTransitionEvent):
        log.debug(self._log('instance_phase_update', "event=[{}]", e))

        is_root_phase = e.phase_detail.phase_id == self._root_phase.id
        if is_root_phase:
            if e.new_stage == Stage.RUNNING:
                log.info(self._log('run_started'))
            log.debug(self._log('instance_lifecycle_update', "new_stage=[{}]", e.new_stage))

            if term := e.phase_detail.lifecycle.termination:
                if term.status.outcome.is_success:
                    log.info(self._log('run_successful', "termination=[{}]", term))
                else:
                    log.warning(self._log('run_unsuccessful', "termination=[{}]", term))

        snapshot = self.to_run()
        if is_root_phase:
            try:
                event = InstanceLifecycleEvent(self.metadata, snapshot, e.new_stage, e.timestamp)
                self._notifications.lifecycle_notification.observer_proxy.instance_lifecycle_update(event)
            except ExceptionGroup as eg:
                log.error("[stage_observer_error]", exc_info=eg)
                for exc in eg.exceptions:
                    self._faults.append(Fault.from_exception(LIFECYCLE_OBSERVER_ERROR, exc))
        try:
            event = InstanceTransitionEvent(self.metadata, snapshot, is_root_phase, e.phase_detail.phase_id,
                                            e.new_stage, e.timestamp)
            self._notifications.transition_notification.observer_proxy.instance_transition_update(event)
        except ExceptionGroup as eg:
            log.error("[transition_observer_error]", exc_info=eg)
            for exc in eg.exceptions:
                self._faults.append(Fault.from_exception(TRANSITION_OBSERVER_ERROR, exc))

