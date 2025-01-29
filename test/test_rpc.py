from typing import List

import pytest

from runtools.runcore import JobRun
from runtools.runcore.client import RemoteCallClient, TargetNotFoundError, RemoteCallResult
from runtools.runcore.criteria import JobRunCriteria
from runtools.runcore.output import OutputLine
from runtools.runcore.run import RunState, TerminationStatus
from runtools.runcore.test.job import FakeJobInstanceBuilder
from runtools.runjob.server import RemoteCallServer

EXEC = 'EXEC'
APPROVAL = 'APPROVAL'


@pytest.fixture
def job_instances():
    j1 = FakeJobInstanceBuilder('j1', 'i1').add_phase(EXEC, RunState.EXECUTING).build()
    j2 = FakeJobInstanceBuilder('j2', 'i2').add_phase(APPROVAL, RunState.PENDING).build()
    yield j1, j2


@pytest.fixture(autouse=True)
def server(job_instances):
    j1, j2 = job_instances
    server = RemoteCallServer()

    server.register_instance(j1)
    j1.next_phase()

    server.register_instance(j2)
    j2.next_phase()

    server.start()
    try:
        yield server
    finally:
        server.close()


def test_target_not_found():
    with pytest.raises(TargetNotFoundError):
        with RemoteCallClient() as c:
            c.call_method('no-server', 'no-method')


def test_active_runs(server):
    with RemoteCallClient() as c:
        j1_run = c.get_active_runs(server.address, JobRunCriteria.job_match('j1'))[0]
        j2_run = c.get_active_runs(server.address, JobRunCriteria.job_match('j2'))[0]
        results: List[RemoteCallResult[List[JobRun]]] = c.collect_active_runs(JobRunCriteria.all())

    assert j1_run.lifecycle.run_state == RunState.EXECUTING
    assert j2_run.lifecycle.run_state == RunState.PENDING

    assert len(results) == 1
    assert results[0].server_address == server.address
    assert len(results[0].retval) == 2
    assert not results[0].error


def test_phase_op_approve(job_instances, server):
    _, j2 = job_instances
    with RemoteCallClient() as c:
        c.exec_phase_op(server.address, j2.instance_id, APPROVAL, 'approve')

    assert j2.get_phase(APPROVAL).approved


def test_stop(job_instances, server):
    j1, j2 = job_instances

    with RemoteCallClient() as c:
        c.stop_instance(server.address, j1.instance_id)

    assert j1.snapshot().termination.status == TerminationStatus.STOPPED
    assert not j2.snapshot().termination


def test_tail(job_instances, server):
    j1, j2 = job_instances
    j1.output.add_line(OutputLine('Meditate, do not delay, lest you later regret it.', False, 'EXEC1'))
    j2.output.add_line(OutputLine('Escape...', True, 'EXEC2'))
    j2.output.add_line(OutputLine('...samsara!', True, 'EXEC2'))

    with RemoteCallClient() as c:
        output_lines = c.get_output_tail(server.address, j1.instance_id)
        assert output_lines == [OutputLine('Meditate, do not delay, lest you later regret it.', False, 'EXEC1')]

        output_lines = c.get_output_tail(server.address, j2.instance_id)
        assert output_lines == [OutputLine('Escape...', True, 'EXEC2'), OutputLine('...samsara!', True, 'EXEC2')]

        output_lines = c.get_output_tail(server.address, j2.instance_id, max_lines=1)
        assert output_lines == [OutputLine('Escape...', True, 'EXEC2')]
