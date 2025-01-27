import pytest

import runtools.runcore
from runtools.runcore.client import APIClient, ApprovalResult
from runtools.runcore.criteria import parse_criteria, JobRunCriteria
from runtools.runcore.output import OutputLine
from runtools.runcore.run import RunState, TerminationStatus
from runtools.runcore.test.job import FakeJobInstanceBuilder
from runtools.runcore.util.json import ErrorCode
from runtools.runjob.api import APIServer

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
    server = APIServer()

    server.register_instance(j1)
    j1.next_phase()

    server.register_instance(j2)
    j2.next_phase()

    server.start()
    try:
        yield server
    finally:
        server.close()


def test_error_not_found():
    with APIClient() as c:
        _, errors = c.send_request('/no-such-api')
    assert errors[0].response_error.code == ErrorCode.METHOD_NOT_FOUND


def test_instances_api():
    resp = runtools.runcore.get_active_runs()
    instances = {inst.job_id: inst for inst in resp.successful}
    assert instances['j1'].lifecycle.run_state == RunState.EXECUTING
    assert instances['j2'].lifecycle.run_state == RunState.PENDING

    resp_j1 = runtools.runcore.get_active_runs(parse_criteria('j1'))
    resp_j2 = runtools.runcore.get_active_runs(parse_criteria('j2'))
    assert resp_j1.successful[0].job_id == 'j1'
    assert resp_j2.successful[0].job_id == 'j2'

    assert not any([resp.errors, resp_j1.errors, resp_j2.errors])


def test_approve_pending_instance(job_instances):
    instances, errors = runtools.runcore.approve_pending_instances(JobRunCriteria.all(), APPROVAL)

    assert not errors
    assert instances[0].instance_metadata.job_id == 'j1'
    assert instances[0].release_result == ApprovalResult.NOT_APPLICABLE
    assert instances[1].instance_metadata.job_id == 'j2'
    assert instances[1].release_result == ApprovalResult.APPROVED

    _, j2 = job_instances
    assert j2.get_phase(APPROVAL).approved


def test_stop(job_instances, server):
    j1, j2 = job_instances

    with APIClient() as c:
        c.stop_instances(server.server_id, j1.instance_id)

    assert j1.snapshot().termination.status == TerminationStatus.STOPPED
    assert not j2.snapshot().termination


def test_tail(job_instances, server):
    j1, j2 = job_instances
    j1.output.add_line(OutputLine('Meditate, do not delay, lest you later regret it.', False, 'EXEC1'))
    j2.output.add_line(OutputLine('Escape...', True, 'EXEC2'))
    j2.output.add_line(OutputLine('...samsara!', True, 'EXEC2'))

    with APIClient() as c:
        output_lines = c.get_output_tail(server.server_id, j1.instance_id)
        assert output_lines == [OutputLine('Meditate, do not delay, lest you later regret it.', False, 'EXEC1')]

        output_lines = c.get_output_tail(server.server_id, j2.instance_id)
        assert output_lines == [OutputLine('Escape...', True, 'EXEC2'), OutputLine('...samsara!', True, 'EXEC2')]

        output_lines = c.get_output_tail(server.server_id, j2.instance_id, max_lines=1)
        assert output_lines == [OutputLine('Escape...', True, 'EXEC2')]
