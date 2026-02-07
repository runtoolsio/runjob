from typing import List

import pytest

from runtools.runcore.client import LocalInstanceClient, TargetNotFoundError, InstanceCallResult
from runtools.runcore.criteria import JobRunCriteria
from runtools.runcore.job import JobRun, InstanceID, iid
from runtools.runcore.output import OutputLine
from runtools.runcore.run import TerminationStatus
from runtools.runjob import instance
from runtools.runjob.server import LocalInstanceServer
from runtools.runjob.test.phase import TestPhase
from runtools.runjob.test.testutil import random_test_socket

EXEC = 'EXEC'
APPROVAL = 'APPROVAL'


@pytest.fixture
def job_instances():
    j1 = instance.create(iid('j1'), None, TestPhase(EXEC))
    j2 = instance.create(iid('j2'), None, TestPhase(APPROVAL, wait=True))
    yield j1, j2


@pytest.fixture(autouse=True)
def server(job_instances):
    j1, j2 = job_instances
    server = LocalInstanceServer(random_test_socket())

    server.register_instance(j1)
    server.register_instance(j2)

    server.start()
    try:
        yield server
    finally:
        server.close()


@pytest.fixture
def client(server):
    with LocalInstanceClient(lambda: [server.address]) as client:
        yield client


def test_server_not_found(client, server):
    with pytest.raises(TargetNotFoundError):
        client.call_method('no-server', 'no-method')


def test_instance_not_found(client, server):
    with pytest.raises(TargetNotFoundError):
        client.stop_instance(server.address, InstanceID('java', 'fx'))


def test_active_runs(client, server):
    j1_run = client.get_active_runs(server.address, JobRunCriteria.job_match('j1'))[0]
    j2_run = client.get_active_runs(server.address, JobRunCriteria.job_match('j2'))[0]
    results: List[InstanceCallResult[List[JobRun]]] = client.collect_active_runs(JobRunCriteria.all())

    assert j1_run.job_id == 'j1'
    assert j2_run.job_id == 'j2'

    assert len(results) == 1
    assert results[0].server_address == server.address
    assert len(results[0].retval) == 2
    assert not results[0].error


def test_stop(job_instances, client, server):
    j1, j2 = job_instances
    client.stop_instance(server.address, j1.id)

    assert j1.to_run().lifecycle.termination.status == TerminationStatus.STOPPED
    assert not j2.to_run().lifecycle.termination


def test_phase_op_release(job_instances, client, server):
    _, j2 = job_instances
    client.exec_phase_op(server.address, j2.id, APPROVAL, 'release')

    assert j2.find_phase_control_by_id(APPROVAL).is_released


def test_tail(job_instances, client, server):
    j1, j2 = job_instances
    j1.output.new_output(OutputLine('Meditate, do not delay, lest you later regret it.', 1, is_error=False, source='EXEC1'))
    j2.output.new_output(OutputLine('Escape...', 1, is_error=True, source='EXEC2'))
    j2.output.new_output(OutputLine('...samsara!', 2, is_error=True, source='EXEC2'))

    output_lines = client.get_output_tail(server.address, j1.id)
    assert output_lines == [OutputLine('Meditate, do not delay, lest you later regret it.', 1, is_error=False, source='EXEC1')]

    output_lines = client.get_output_tail(server.address, j2.id)
    assert output_lines == [OutputLine('Escape...', 1, is_error=True, source='EXEC2'), OutputLine('...samsara!', 2, is_error=True, source='EXEC2')]

    output_lines = client.get_output_tail(server.address, j2.id, max_lines=1)
    assert output_lines == [OutputLine('...samsara!', 2, is_error=True, source='EXEC2')]
