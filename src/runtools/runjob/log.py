"""Logging support for runjob — automatic run context injection.

Provides ``RunContextFilter`` which reads ``current_job_instance`` and ``_current_phase``
ContextVars and injects ``instance`` and ``phase`` into every LogRecord.
"""

import logging

from runtools.runjob.instance import current_job_instance
from runtools.runjob.phase import _current_phase


class RunContextFilter(logging.Filter):
    """Injects instance and phase into LogRecords from ContextVars.

    Attach to handlers to get automatic run context on every log record during job execution.
    Skips injection if the attribute is already present (explicit ``extra={}`` wins).
    Fields are None when outside a job run — formatters that skip None values will omit them.
    """

    def filter(self, record):
        if not hasattr(record, 'instance'):
            inst = current_job_instance.get(None)
            record.instance = str(inst.instance_id) if inst else None
        if not hasattr(record, 'phase'):
            phase = _current_phase.get(None)
            record.phase = phase.id if phase else None
        return True
