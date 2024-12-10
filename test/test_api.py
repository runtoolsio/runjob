import pytest

import runtools.runcore
from runtools.runcore.client import APIClient, ApprovalResult, StopResult
from runtools.runcore.criteria import parse_criteria, JobRunCriteria
from runtools.runcore.output import OutputLine
from runtools.runcore.run import RunState, TerminationStatus
from runtools.runcore.test.job import FakeJobInstanceBuilder
from runtools.runjob.api import APIServer, ErrorCode

EXEC = 'EXEC'
APPROVAL = 'APPROVAL'


@pytest.fixture(autouse=True)
def job_instances():
    server = APIServer()

    j1 = FakeJobInstanceBuilder('j1', 'i1').add_phase(EXEC, RunState.EXECUTING).build()
    server.register_instance(j1)
    j1.next_phase()

    j2 = FakeJobInstanceBuilder('j2', 'i2').add_phase(APPROVAL, RunState.PENDING).build()
    server.register_instance(j2)
    j2.next_phase()

    server.start()
    try:
        yield j1, j2
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


def test_stop(job_instances):
    instances, errors = runtools.runcore.stop_instances(parse_criteria('j1'))
    assert not errors
    assert len(instances) == 1
    assert instances[0].instance_metadata.job_id == 'j1'
    assert instances[0].stop_result == StopResult.STOP_INITIATED

    j1, j2 = job_instances
    assert j1.job_run().termination.status == TerminationStatus.STOPPED
    assert not j2.job_run().termination


def test_tail(job_instances):
    j1, j2 = job_instances
    j1.output.add_line(OutputLine('Meditate, do not delay, lest you later regret it.', False, 'EXEC'))

    instances, errors = runtools.runcore.get_tail()
    assert not errors

    assert instances[0].instance_metadata.job_id == 'j1'
    assert instances[0].output == [OutputLine('Meditate, do not delay, lest you later regret it.', False, 'EXEC')]

    assert instances[1].instance_metadata.job_id == 'j2'
    assert not instances[1].output
