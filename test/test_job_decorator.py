from unittest.mock import patch

import pytest
from runtools.runcore.run import TerminationStatus
from runtools.runjob import node
from runtools.runjob.job import job
from runtools.runjob.phase import phase, ChildPhaseTerminated


@pytest.fixture(autouse=True)
def use_in_process_node():
    """Route node.connect() to an in-process node for all tests."""

    def _connect(env_id=None):
        return node.in_process(env_id or "test")

    with patch("runtools.runjob.job.node.connect", side_effect=_connect):
        yield


def test_basic_execution():
    """@job function returns a JobRun with COMPLETED status."""

    @job
    def simple():
        return 42

    run = simple()
    assert run.lifecycle.termination.status == TerminationStatus.COMPLETED


def test_bare_and_parameterized_forms():
    """Both @job and @job(job_id='x') work."""

    @job
    def bare():
        pass

    @job(job_id="custom")
    def parameterized():
        pass

    run1 = bare()
    run2 = parameterized()
    assert run1.lifecycle.termination.status == TerminationStatus.COMPLETED
    assert run2.lifecycle.termination.status == TerminationStatus.COMPLETED
    assert run2.instance_id.job_id == "custom"


def test_job_id_defaults_to_function_name():
    """Job ID defaults to the function name."""

    @job
    def my_pipeline():
        pass

    run = my_pipeline()
    assert run.instance_id.job_id == "my_pipeline"


def test_explicit_job_id():
    """@job(job_id='etl') overrides the function name."""

    @job(job_id="etl")
    def pipeline():
        pass

    run = pipeline()
    assert run.instance_id.job_id == "etl"


def test_run_id_kwarg():
    """run_id kwarg sets the run ID on the instance."""

    @job
    def pipeline():
        pass

    run = pipeline(run_id="batch-42")
    assert run.instance_id.run_id == "batch-42"


def test_run_id_auto_generated():
    """run_id is auto-generated when not provided."""

    @job
    def pipeline():
        pass

    run = pipeline()
    assert run.instance_id.run_id  # non-empty


def test_function_args_pass_through():
    """Function args are forwarded, run_id/env are intercepted."""
    results = []

    @job
    def process(x, y, mode="fast"):
        results.append((x, y, mode))

    process(1, 2, mode="slow", run_id="r1")
    assert results == [(1, 2, "slow")]


def test_phase_children_inside_job():
    """@phase functions work as children inside a @job function."""

    @phase
    def step_a():
        return "a"

    @phase
    def step_b():
        return "b"

    @job
    def pipeline():
        step_a()
        step_b()

    run = pipeline()
    assert run.lifecycle.termination.status == TerminationStatus.COMPLETED
    children = run.root_phase.children
    assert len(children) == 2
    assert children[0].phase_id == "step_a"
    assert children[1].phase_id == "step_b"


def test_exception_propagation():
    """Exceptions from @job functions propagate to the caller."""

    @job
    def failing():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        failing()


def test_env_kwarg_forwarded():
    """env kwarg at call time is forwarded to node.connect."""
    connect_args = []
    original_connect = node.connect

    def tracking_connect(env_id=None):
        connect_args.append(env_id)
        return node.in_process(env_id or "test")

    with patch("runtools.runjob.job.node.connect", side_effect=tracking_connect):
        @job
        def pipeline():
            pass

        pipeline(env="staging")

    assert connect_args == ["staging"]


def test_env_decorator_default():
    """env from decorator is used when no call-time env is provided."""
    connect_args = []

    def tracking_connect(env_id=None):
        connect_args.append(env_id)
        return node.in_process(env_id or "test")

    with patch("runtools.runjob.job.node.connect", side_effect=tracking_connect):
        @job(env="production")
        def pipeline():
            pass

        pipeline()

    assert connect_args == ["production"]


def test_env_call_overrides_decorator():
    """env at call time overrides the decorator default."""
    connect_args = []

    def tracking_connect(env_id=None):
        connect_args.append(env_id)
        return node.in_process(env_id or "test")

    with patch("runtools.runjob.job.node.connect", side_effect=tracking_connect):
        @job(env="production")
        def pipeline():
            pass

        pipeline(env="staging")

    assert connect_args == ["staging"]


def test_env_param_not_hijacked():
    """env kwarg passes through when the function has an 'env' parameter."""
    received = []

    @job
    def deploy(env):
        received.append(env)

    deploy(env="production")
    assert received == ["production"]


def test_run_id_param_not_hijacked():
    """run_id kwarg passes through when the function has a 'run_id' parameter."""
    received = []

    @job
    def record(run_id):
        received.append(run_id)

    record(run_id="abc-123")
    assert received == ["abc-123"]


def test_env_and_run_id_both_in_signature():
    """Both env and run_id pass through when they're function parameters."""
    received = []

    @job
    def process(env, run_id):
        received.append((env, run_id))

    process(env="staging", run_id="r1")
    assert received == [("staging", "r1")]
