from threading import Thread

import pytest
import time

import runtools.runner
from runtools.runcore.track import TaskTrackerMem
from runtools.runner import warning, ExecutingPhase
from runtools.runner.test.execution import TestExecution


@pytest.fixture
def execution():
    return TestExecution(wait=True)


@pytest.fixture
def job(execution):
    return runtools.runner.job_instance('j1', [ExecutingPhase('', execution)], task_tracker=TaskTrackerMem())


def test_exec_time_warning(execution, job):
    warning.exec_time_exceeded(job, 'wid', 0.5)
    run_thread = Thread(target=job.run)
    run_thread.start()

    time.sleep(0.6)

    execution.release()
    run_thread.join(1)
    assert job.task_tracker.tracked_task.warnings
