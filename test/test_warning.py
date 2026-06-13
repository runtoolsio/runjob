import time
from threading import Thread

from runtools.runcore.job import iid
from runtools.runjob import instance
from runtools.runjob.test.phase import TestPhase
from runtools.runjob.track import StatusTracker
from runtools.runjob.warning import TimeWarningExtension


def test_time_warning_triggers_when_phase_exceeds_threshold():
    phase = TimeWarningExtension(TestPhase('p1', wait=True), 0.5)
    job_instance = instance.create(iid('j1'), None, phase, status_tracker=StatusTracker())
    run_thread = Thread(target=job_instance.run)
    run_thread.start()

    time.sleep(0.6)

    job_instance.find_phase_control_by_id('p1').release()
    run_thread.join(1)

    assert job_instance.snap().status.warnings
