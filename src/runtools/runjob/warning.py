"""
Warning extensions for monitoring job execution and triggering alerts.

Provides two layers of warning functionality:

Phase decorators (for wrapping individual phases):
    TimeWarningExtension — triggers a warning when phase execution exceeds a time threshold.
    OutputWarningExtension — triggers warnings when phase output matches regex patterns.

Job instance observers (for monitoring the entire job lifecycle):
    exec_time_exceeded() — registers a time-based warning observer on a job instance.
    output_matches() — registers an output pattern warning observer on a job instance.
    register() — convenience function to register multiple warnings from duration strings and regex patterns.
"""

import logging
import re
from threading import Timer
from typing import Generic, Optional
from typing import Sequence, List

from runtools.runcore import util
from runtools.runcore.job import (JobInstance, InstanceOutputObserver, InstanceLifecycleObserver,
                                  InstanceLifecycleEvent,
                                  InstanceOutputEvent)
from runtools.runcore.output import OutputLine
from runtools.runcore.run import Stage, C, StopReason
from runtools.runjob.output import OutputContext
from runtools.runjob.phase import PhaseDecorator

log = logging.getLogger(__name__)


class TimeWarningExtension(PhaseDecorator[C], Generic[C]):
    """
    A phase decorator that adds a single warning threshold to any phase.

    This extension monitors the execution time of the wrapped phase and triggers
    a warning when the specified time threshold is exceeded.
    """

    def __init__(self, wrapped, warning_seconds: float, warning_text: str = None):
        """
        Initialize with a delegate phase and a single warning threshold.

        Args:
            wrapped: The Phase implementation to wrap
            warning_seconds: Time in seconds at which to issue a warning
            warning_text: Text for the warning; if None, a default is generated
        """
        super().__init__(wrapped)
        if warning_seconds <= 0:
            raise ValueError("Warning threshold must be positive")

        self.warning_seconds = warning_seconds
        self.warning_text = warning_text or f"Run time exceeded {warning_seconds}s for phase `{wrapped.id}`"
        self._timer = None

    def run(self, ctx: Optional[C]):
        """
        Run the wrapped phase with a warning timer.

        Args:
            ctx: The execution context to pass to the wrapped phase
        """
        if not ctx or not hasattr(ctx, 'status_tracker') or not ctx.status_tracker:
            log.warning(
                f"status_tracker_unavailable phase=[{super().id}] result=[Time warning {self.warning_seconds}s ignored]")
        else:
            self._timer = Timer(self.warning_seconds, self._trigger_warning, args=[ctx])
            self._timer.daemon = True
            self._timer.start()

        try:
            return super().run(ctx)
        finally:
            self._cancel_timer()

    def _trigger_warning(self, ctx):
        """
        Trigger a warning when the time threshold is reached.

        Args:
            ctx: The execution context, used to access the status tracker
        """
        log.warning(
            f"time_warning warn_sec=[{self.warning_seconds}] warn_text=[{self.warning_text}] phase=[{super().id}]")
        ctx.status_tracker.warning(self.warning_text)

    def _cancel_timer(self):
        """Cancel the warning timer if it's running."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def stop(self, reason=StopReason.STOPPED):
        """
        Stop the phase execution and cancel the warning timer.

        Args:
            reason: The reason for stopping the phase
        """
        self._cancel_timer()
        super().stop(reason)


class OutputWarningExtension(PhaseDecorator[C], Generic[C]):
    """
    A phase decorator that monitors output for patterns and triggers warnings.
    This extension observes the output of the wrapped phase and generates a warning
    when output matching any of the specified patterns is detected.
    """

    def __init__(self, wrapped, patterns: List[str], warning_template: str = None):
        """
        Initialize the extension with output patterns to monitor.

        Args:
            wrapped: The Phase implementation which output will be monitored
            patterns: List of regex patterns to match against output
            warning_template: Custom warning message (default: "$LINE")
                              Available placeholders:
                              - $MATCH: The specific text that matched the pattern
                              - $LINE: The entire output line where the match was found
        """
        super().__init__(wrapped)
        if not patterns:
            raise ValueError("At least one pattern must be provided")

        self.patterns = [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
        self.warning_template = warning_template or "$LINE"

    def run(self, ctx: Optional[C]):
        """
        Run the wrapped phase with output monitoring.

        Args:
            ctx: The execution context to pass to the wrapped phase
        """
        if not ctx or not hasattr(ctx, 'status_tracker') or not ctx.status_tracker or not isinstance(ctx,
                                                                                                     OutputContext):
            log.warning(f"incompatible_run_context phase=[{super().id}] result=[Output warning ignored]")
            return super().run(ctx)

        def pattern_output_observer(output_line: OutputLine):
            for pattern in self.patterns:
                match = pattern.search(output_line.message)
                if match:
                    warning_text = self.warning_template
                    if "$MATCH" in warning_text:
                        warning_text = warning_text.replace("$MATCH", match.group(0))
                    if "$LINE" in warning_text:
                        warning_text = warning_text.replace("$LINE", output_line.message)

                    log.warning(
                        f"output_warning pattern=[{pattern.pattern}] match=[{match.group(0)}]"
                        f" line=[{output_line.message}] phase=[{self.id}]")
                    ctx.status_tracker.warning(warning_text)

        with ctx.output_sink.observer_context(pattern_output_observer):
            return super().run(ctx)


def exec_time_exceeded(job_instance: JobInstance, warning_name: str, time: float):
    """Register a warning that triggers when job execution time exceeds the given threshold.

    Args:
        job_instance: The job instance to monitor.
        warning_name: Descriptive name for the warning event.
        time: Time threshold in seconds.
    """
    job_instance.notifications.add_observer_lifecycle(_ExecTimeWarning(job_instance, warning_name, time))


def output_matches(job_instance: JobInstance, warning_name: str, regex: str):
    """Register a warning that triggers when job output matches the given regex pattern.

    Args:
        job_instance: The job instance to monitor.
        warning_name: Descriptive name for the warning event.
        regex: Regular expression pattern to match against output lines.
    """
    job_instance.notifications.add_observer_output(_OutputMatchesWarning(job_instance, warning_name, regex))


def register(job_instance: JobInstance, *, warn_times: Sequence[str] = (), warn_outputs: Sequence[str] = ()):
    """Register multiple warnings on a job instance from duration strings and regex patterns.

    Args:
        job_instance: The job instance to monitor.
        warn_times: Duration strings (e.g., "30s", "5m") for execution time warnings.
        warn_outputs: Regex patterns for output matching warnings.
    """
    for warn_time in warn_times:
        time = util.parse_duration_to_sec(warn_time)
        exec_time_exceeded(job_instance, f"run_time>{time}s", time)

    for warn_output in warn_outputs:
        output_matches(job_instance, f"output=~{warn_output}", warn_output)


class _ExecTimeWarning(InstanceLifecycleObserver):
    """Lifecycle observer that triggers a warning when job execution time exceeds a threshold."""

    def __init__(self, job_instance, text, time: float):
        self.job_instance = job_instance
        self.text = text
        self.time = time
        self.timer = None

    def instance_lifecycle_update(self, event: InstanceLifecycleEvent):
        if event.new_stage == Stage.ENDED:
            if self.timer is not None:
                self.timer.cancel()
        elif event.new_stage == Stage.RUNNING:
            assert self.timer is None
            self.timer = Timer(self.time, self._check)
            self.timer.daemon = True
            self.timer.start()

    def _check(self):
        if not self.job_instance.snap().lifecycle.termination:
            self.job_instance.status_tracker.warning(self.text)

    def __repr__(self):
        return "{}({!r}, {!r}, {!r})".format(
            self.__class__.__name__, self.job_instance, self.text, self.time)


class _OutputMatchesWarning(InstanceOutputObserver):
    """Output observer that triggers a warning when job output matches a regex pattern."""

    def __init__(self, job_instance, text, regex):
        self.job_instance = job_instance
        self.text = text
        self.regex = re.compile(regex)

    def instance_output_update(self, event: InstanceOutputEvent):
        m = self.regex.search(event.output_line.message)
        if m:
            self.job_instance.status_tracker.warning(self.text)
