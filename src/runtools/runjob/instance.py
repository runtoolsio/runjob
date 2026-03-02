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
from typing import Optional, List, override

from runtools.runcore.job import (JobInstance, JobRun, JobInstanceMetadata, InstanceNotifications,
                                  InstanceObservableNotifications, InstancePhaseEvent, InstanceOutputEvent,
                                  InstanceLifecycleEvent, InstanceControlEvent, ControlAction, InstanceStatusEvent)
from runtools.runcore.run import Fault, PhaseTransitionEvent, JobCompletionError, Stage, StopReason
from runtools.runcore.util import utc_now
from runtools.runjob.output import OutputContext, OutputSink, InMemoryTailBuffer, OutputRouter
from runtools.runjob.phase import Phase
from runtools.runjob.track import StatusTracker

log = logging.getLogger(__name__)

LIFECYCLE_OBSERVER_ERROR = "LIFECYCLE_OBSERVER_ERROR"
PHASE_OBSERVER_ERROR = "PHASE_OBSERVER_ERROR"
OUTPUT_OBSERVER_ERROR = "OUTPUT_OBSERVER_ERROR"
CONTROL_OBSERVER_ERROR = "CONTROL_OBSERVER_ERROR"
STATUS_OBSERVER_ERROR = "STATUS_OBSERVER_ERROR"

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


def create(instance_id, environment, root_phase, *,
           activate=True,
           output_sink=None, output_router=None, tail_buffer_size=2 * 1024 * 1024,
           status_tracker=None,
           error_hook=None,
           **user_params) -> JobInstance:
    """Create a job instance.

    Args:
        instance_id: Unique identifier for this instance
        environment: Environment configuration
        root_phase: Root phase of the job instance
        activate: Wire phase observers and status tracking. Set to ``False`` when the caller
                  needs to control activation timing (e.g. after a duplicate check).
        output_sink: Custom output sink (created automatically if not provided).
                     For text parsing, provide OutputSink(ParsingPreprocessor([KVParser()])).
        output_router: Output router for tail buffering and storage. Built from env config when created via
                       node; defaults to tail-only router for direct usage.
        tail_buffer_size: Max bytes for the default tail buffer (default 2 MB). Ignored if output_router is provided.
        status_tracker: Custom status tracker (created automatically if not provided)
        error_hook: Error handler for observer errors
        **user_params: Additional user-defined parameters stored in metadata
    """
    if not instance_id:
        raise ValueError("Instance ID is mandatory")
    output_sink = output_sink or OutputSink()
    if output_router is None:
        tail_buffer = InMemoryTailBuffer(max_bytes=tail_buffer_size) if tail_buffer_size else None
        output_router = OutputRouter(tail_buffer=tail_buffer)
    status_tracker = status_tracker or StatusTracker()

    # Auto-wire status tracker as output observer
    output_sink.add_observer(status_tracker)

    inst = _JobInstance(instance_id, root_phase, environment, output_sink, output_router, status_tracker,
                        error_hook, user_params)
    if activate:
        inst.activate()
    return inst


class _JobInstance(JobInstance):

    def __init__(self, instance_id, root_phase, env, output_sink, output_router, status_tracker,
                 error_hook, user_params):
        self._notifications = InstanceObservableNotifications(error_hook=error_hook, force_reraise=True)
        self._metadata = JobInstanceMetadata(instance_id, user_params)
        self._root_phase: Phase = root_phase
        self._output_sink: OutputSink = output_sink
        self._output_router = output_router
        self._ctx = JobInstanceContext(self._metadata, env, status_tracker, self._output_sink)
        self._faults: List[Fault] = []

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        return self._notifications

    def activate(self):
        """Wire phase observers and status tracking. Must be called before ``run()``.

        Separated from construction so that callers can control timing. Nodes call this after the
        duplicate check and hook registration to avoid leaving stale observers on a shared phase
        if an earlier step fails.
        """
        self._root_phase.add_phase_observer(self._on_phase_update)
        self._ctx.status_tracker._on_change = self._on_status_change
        return self

    def notify_created(self):
        """Fire the CREATED lifecycle event. Call after ``activate()`` and all observers are registered."""
        log.info(self._log('instance_created'))
        try:
            event = InstanceLifecycleEvent(self.snap(), Stage.CREATED, utc_now())
            self._notifications.lifecycle_notification.observer_proxy.instance_lifecycle_update(event)
        except ExceptionGroup as eg:
            log.error("[lifecycle_observer_error]", exc_info=eg)
            for exc in eg.exceptions:
                self._faults.append(Fault.from_exception(LIFECYCLE_OBSERVER_ERROR, exc))
        return self

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

    def snap(self) -> JobRun:
        status = self._ctx.status_tracker.to_status() if self.status_tracker else None
        return JobRun(
            self.metadata,
            self._root_phase.snap(),
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
                    log_filter = _JobInstanceLogFilter(self.id)
                    with self._output_sink.capture_logs_from(logging.getLogger(), log_filter=log_filter):
                        try:
                            retval = self._root_phase.run(self._ctx)
                        except Exception as e:
                            raise JobCompletionError(self._root_phase.termination) from e

                        termination = self._root_phase.termination
                        if not termination.status.outcome.is_success:
                            raise JobCompletionError(termination)
                        return retval

    def run_in_new_thread(self, daemon=False):
        t = Thread(target=self.run, daemon=daemon)
        t.start()
        return t

    def stop(self, reason=StopReason.STOPPED):
        self._root_phase.stop(reason)
        try:
            event = InstanceControlEvent(self.snap(), ControlAction.STOP_REQUESTED, utc_now())
            self._notifications.control_notification.observer_proxy.instance_control_update(event)
        except ExceptionGroup as eg:
            log.error("[control_observer_error]", exc_info=eg)
            for exc in eg.exceptions:
                self._faults.append(Fault.from_exception(CONTROL_OBSERVER_ERROR, exc))

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

        snapshot = self.snap()
        if is_root_phase:
            try:
                event = InstanceLifecycleEvent(snapshot, e.new_stage, e.timestamp)
                self._notifications.lifecycle_notification.observer_proxy.instance_lifecycle_update(event)
            except ExceptionGroup as eg:
                log.error("[stage_observer_error]", exc_info=eg)
                for exc in eg.exceptions:
                    self._faults.append(Fault.from_exception(LIFECYCLE_OBSERVER_ERROR, exc))
        try:
            event = InstancePhaseEvent(snapshot, is_root_phase, e.phase_detail.phase_id,
                                       e.new_stage, e.timestamp)
            self._notifications.phase_notification.observer_proxy.instance_phase_update(event)
        except ExceptionGroup as eg:
            log.error("[phase_observer_error]", exc_info=eg)
            for exc in eg.exceptions:
                self._faults.append(Fault.from_exception(PHASE_OBSERVER_ERROR, exc))

    def _on_status_change(self):
        try:
            event = InstanceStatusEvent(self.snap(), utc_now())
            self._notifications.status_notification.observer_proxy.instance_status_update(event)
        except ExceptionGroup as eg:
            log.error("[status_observer_error]", exc_info=eg)
            for exc in eg.exceptions:
                self._faults.append(Fault.from_exception(STATUS_OBSERVER_ERROR, exc))
