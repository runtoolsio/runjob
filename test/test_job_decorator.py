from unittest.mock import patch

import pytest
from runtools.runcore.job import DuplicateStrategy
from runtools.runcore.run import JobCompletionError, TerminationStatus
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
    """@job function returns a JobResult with COMPLETED status and retval."""

    @job
    def simple():
        return 42

    result = simple()
    assert result.run.lifecycle.termination.status == TerminationStatus.COMPLETED
    assert result.retval == 42


def test_bare_and_parameterized_forms():
    """Both @job and @job(job_id='x') work."""

    @job
    def bare():
        pass

    @job(job_id="custom")
    def parameterized():
        pass

    result1 = bare()
    result2 = parameterized()
    assert result1.run.lifecycle.termination.status == TerminationStatus.COMPLETED
    assert result2.run.lifecycle.termination.status == TerminationStatus.COMPLETED
    assert result2.run.instance_id.job_id == "custom"


def test_job_id_defaults_to_function_name():
    """Job ID defaults to the function name."""

    @job
    def my_pipeline():
        pass

    result = my_pipeline()
    assert result.run.instance_id.job_id == "my_pipeline"


def test_explicit_job_id():
    """@job(job_id='etl') overrides the function name."""

    @job(job_id="etl")
    def pipeline():
        pass

    result = pipeline()
    assert result.run.instance_id.job_id == "etl"


def test_run_id_kwarg():
    """run_id kwarg sets the run ID on the instance."""

    @job
    def pipeline():
        pass

    result = pipeline(run_id="batch-42")
    assert result.run.instance_id.run_id == "batch-42"


def test_run_id_auto_generated():
    """run_id is auto-generated when not provided."""

    @job
    def pipeline():
        pass

    result = pipeline()
    assert result.run.instance_id.run_id  # non-empty


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

    result = pipeline()
    assert result.run.lifecycle.termination.status == TerminationStatus.COMPLETED
    children = result.run.root_phase.children
    assert len(children) == 2
    assert children[0].phase_id == "step_a"
    assert children[1].phase_id == "step_b"


def test_exception_propagation():
    """Exceptions from @job functions are wrapped in JobCompletionError."""

    @job
    def failing():
        raise ValueError("boom")

    with pytest.raises(JobCompletionError) as exc_info:
        failing()

    assert isinstance(exc_info.value.__cause__, ValueError)
    assert str(exc_info.value.__cause__) == "boom"


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


# --- duplicate_strategy forwarding ---

def _capture_create_instance_kwargs():
    """Patch env_node.create_instance to capture kwargs without running anything."""
    captured = {}

    def _connect(env_id=None):
        n = node.in_process(env_id or "test")
        n_enter = n.__enter__
        n_exit = n.__exit__

        class _CapturingCtx:
            def __enter__(self):
                env_node = n_enter()
                original = env_node.create_instance

                def spy(*args, **kwargs):
                    captured['args'] = args
                    captured['kwargs'] = kwargs
                    return original(*args, **kwargs)

                env_node.create_instance = spy
                return env_node

            def __exit__(self, *a):
                return n_exit(*a)

        return _CapturingCtx()

    return captured, _connect


def test_duplicate_strategy_default_is_raise():
    """Bare @job forwards DuplicateStrategy.RAISE to create_instance."""
    captured, connect_spy = _capture_create_instance_kwargs()

    with patch("runtools.runjob.job.node.connect", side_effect=connect_spy):
        @job
        def pipeline():
            pass
        pipeline()

    assert captured['kwargs']['duplicate_strategy'] == DuplicateStrategy.RAISE


def test_duplicate_strategy_call_time_forwarded():
    """Call-time duplicate_strategy reaches create_instance."""
    captured, connect_spy = _capture_create_instance_kwargs()

    with patch("runtools.runjob.job.node.connect", side_effect=connect_spy):
        @job
        def pipeline():
            pass
        pipeline(duplicate_strategy=DuplicateStrategy.ALLOW)

    assert captured['kwargs']['duplicate_strategy'] == DuplicateStrategy.ALLOW


def test_duplicate_strategy_decoration_default():
    """Decoration-time duplicate_strategy is the default when call omits it."""
    captured, connect_spy = _capture_create_instance_kwargs()

    with patch("runtools.runjob.job.node.connect", side_effect=connect_spy):
        @job(duplicate_strategy=DuplicateStrategy.ALLOW)
        def pipeline():
            pass
        pipeline()

    assert captured['kwargs']['duplicate_strategy'] == DuplicateStrategy.ALLOW


def test_duplicate_strategy_call_overrides_decoration():
    """Call-time duplicate_strategy beats decoration-time."""
    captured, connect_spy = _capture_create_instance_kwargs()

    with patch("runtools.runjob.job.node.connect", side_effect=connect_spy):
        @job(duplicate_strategy=DuplicateStrategy.ALLOW)
        def pipeline():
            pass
        pipeline(duplicate_strategy=DuplicateStrategy.RAISE)

    assert captured['kwargs']['duplicate_strategy'] == DuplicateStrategy.RAISE


def test_duplicate_strategy_param_not_hijacked():
    """duplicate_strategy passes through when the function has a parameter with that name."""
    received = []

    @job
    def process(duplicate_strategy):
        received.append(duplicate_strategy)

    process(duplicate_strategy="custom-value")
    assert received == ["custom-value"]


# --- tags ---

def test_tags_at_call_time():
    """tags kwarg at call time persists onto the run's metadata."""

    @job
    def pipeline():
        pass

    result = pipeline(tags=("nightly", "assistant"))
    assert result.run.metadata.tags == ("nightly", "assistant")


def test_tags_at_decoration_time():
    """tags on the decorator apply to every call."""

    @job(tags=("nightly",))
    def pipeline():
        pass

    result = pipeline()
    assert result.run.metadata.tags == ("nightly",)


def test_tags_decoration_and_call_merge():
    """Decoration-time tags + call-time tags merge (deduped)."""

    @job(tags=("nightly",))
    def pipeline():
        pass

    result = pipeline(tags=("urgent", "nightly"))  # 'nightly' duplicated
    assert result.run.metadata.tags == ("nightly", "urgent")


def test_tags_normalized():
    """Raw tag input is normalized at the metadata boundary."""

    @job
    def pipeline():
        pass

    result = pipeline(tags=("#Foo", "BAR"))
    assert result.run.metadata.tags == ("foo", "bar")


def test_tags_decoration_rejects_bare_string():
    """@job(tags="prod") would silently become ("p","r","o","d") without the guard."""
    with pytest.raises(ValueError, match="iterable of strings"):
        @job(tags="prod")
        def pipeline():
            pass


def test_tags_call_rejects_bare_string():
    """pipeline(tags="prod") has the same trap; rejected at the call boundary."""

    @job
    def pipeline():
        pass

    with pytest.raises(ValueError, match="iterable of strings"):
        pipeline(tags="prod")


def test_tags_param_not_hijacked():
    """tags passes through when the function has a 'tags' parameter."""
    received = []

    @job
    def process(tags):
        received.append(tags)

    process(tags=["custom"])
    assert received == [["custom"]]
