from threading import Thread

import pytest
import time

from runtools.runjob import instance
from runtools.runjob import warning
from runtools.runjob.test.phaser import TestPhase
from runtools.runjob.track import StatusTracker


@pytest.fixture
def job_instance():
    return instance.create('j1', [TestPhase('p1', wait=True)], status_tracker=StatusTracker())


def test_exec_time_warning(job_instance):
    warning.exec_time_exceeded(job_instance, 'wid', 0.5)
    run_thread = Thread(target=job_instance.run)
    run_thread.start()

    time.sleep(0.6)

    job_instance.get_phase('p1').release()  # TODO PhaseControl
    run_thread.join(1)
    assert job_instance.status_tracker.to_status().warnings
