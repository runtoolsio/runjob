from typing import List

import pytest

from runtools.runcore.client import RemoteCallClient, TargetNotFoundError, RemoteCallResult
from runtools.runcore.criteria import JobRunCriteria
from runtools.runcore.job import JobRun
from runtools.runcore.output import OutputLine
from runtools.runcore.run import TerminationStatus
from runtools.runjob.test.testutil import random_test_socket
from runtools.runjob import instance
from runtools.runjob.server import RemoteCallServer
from runtools.runjob.test.phase import TestPhase

EXEC = 'EXEC'
APPROVAL = 'APPROVAL'


@pytest.fixture
def job_instances():
    j1 = instance.create('j1', [TestPhase(EXEC)])
    j2 = instance.create('j2', [TestPhase(APPROVAL, wait=True)], run_id='i2')
    yield j1, j2


@pytest.fixture(autouse=True)
def server(job_instances):
    j1, j2 = job_instances
    server = RemoteCallServer(random_test_socket())

    server.register_instance(j1)
    server.register_instance(j2)

    server.start()
    try:
        yield server
    finally:
        server.close()


@pytest.fixture
def client(server):
    with RemoteCallClient(lambda: [server.address], random_test_socket()) as client:
        yield client


def test_server_not_found(client, server):
    with pytest.raises(TargetNotFoundError):
        client.call_method('no-server', 'no-method')


def test_instance_not_found(client, server):
    with pytest.raises(TargetNotFoundError):
        client.stop_instance(server.address, 'java-fx')


def test_active_runs(client, server):
    j1_run = client.get_active_runs(server.address, JobRunCriteria.job_match('j1'))[0]
    j2_run = client.get_active_runs(server.address, JobRunCriteria.job_match('j2'))[0]
    results: List[RemoteCallResult[List[JobRun]]] = client.collect_active_runs(JobRunCriteria.all())

    assert j1_run.job_id == 'j1'
    assert j2_run.job_id == 'j2'

    assert len(results) == 1
    assert results[0].server_address == server.address
    assert len(results[0].retval) == 2
    assert not results[0].error


def test_stop(job_instances, client, server):
    j1, j2 = job_instances
    client.stop_instance(server.address, j1.instance_id)

    assert j1.snapshot().lifecycle.termination.status == TerminationStatus.STOPPED
    assert not j2.snapshot().lifecycle.termination


def test_phase_op_release(job_instances, client, server):
    _, j2 = job_instances
    client.exec_phase_op(server.address, j2.instance_id, APPROVAL, 'release')

    assert j2.find_phase_control_by_id(APPROVAL).is_released


def test_tail(job_instances, client, server):
    j1, j2 = job_instances
    j1.output.new_output(OutputLine('Meditate, do not delay, lest you later regret it.', False, 'EXEC1'))
    j2.output.new_output(OutputLine('Escape...', True, 'EXEC2'))
    j2.output.new_output(OutputLine('...samsara!', True, 'EXEC2'))

    output_lines = client.get_output_tail(server.address, j1.instance_id)
    assert output_lines == [OutputLine('Meditate, do not delay, lest you later regret it.', False, 'EXEC1')]

    output_lines = client.get_output_tail(server.address, j2.instance_id)
    assert output_lines == [OutputLine('Escape...', True, 'EXEC2'), OutputLine('...samsara!', True, 'EXEC2')]

    output_lines = client.get_output_tail(server.address, j2.instance_id, max_lines=1)
    assert output_lines == [OutputLine('...samsara!', True, 'EXEC2')]
