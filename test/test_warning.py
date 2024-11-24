from threading import Thread

import pytest
import time

import runtools.runjob
from runtools.runcore.track import TaskTrackerMem
from runtools.runjob import warning
from runtools.runjob.test.phaser import TestPhase


@pytest.fixture
def job_instance():
    return runtools.runjob.job_instance('j1', [TestPhase('p1', wait=True)], task_tracker=TaskTrackerMem())


def test_exec_time_warning(job_instance):
    warning.exec_time_exceeded(job_instance, 'wid', 0.5)
    run_thread = Thread(target=job_instance.run)
    run_thread.start()

    time.sleep(0.6)

    job_instance.get_phase('p1').release()
    run_thread.join(1)
    assert job_instance.task_tracker.tracked_task.warnings
