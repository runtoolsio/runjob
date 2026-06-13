"""
Warning extensions for monitoring job execution and triggering alerts.

Phase decorators for wrapping individual phases:
    TimeWarningExtension — triggers a warning when phase execution exceeds a time threshold.
    OutputWarningExtension — triggers warnings when phase output matches regex patterns.
"""

import logging
import re
from threading import Timer
from typing import Generic, List, Optional

from runtools.runcore.output import OutputLine
from runtools.runcore.run import C, StopReason
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
        if not ctx or not hasattr(ctx, 'tracker') or not ctx.tracker:
            log.warning("Tracker unavailable phase=%s", super().id, extra={"phase": super().id})
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
        log.warning("Time warning triggered", extra={"seconds": self.warning_seconds, "phase": super().id})
        ctx.tracker.warning(self.warning_text)

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
        if not ctx or not hasattr(ctx, 'tracker') or not ctx.tracker or not isinstance(ctx,
                                                                                                     OutputContext):
            log.warning("Incompatible run context", extra={"phase": super().id})
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

                    log.warning("Output warning match", extra={"pattern": pattern.pattern, "line": output_line.message})
                    ctx.tracker.warning(warning_text)

        with ctx.output_pipeline.observer_context(pattern_output_observer):
            return super().run(ctx)
