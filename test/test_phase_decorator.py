import threading

import pytest
from runtools.runcore.run import TerminationStatus
from runtools.runjob.coord import (
    ApprovalPhase, CheckpointPhase, ConcurrencyGroup, ExecutionQueue, MutualExclusionPhase,
    approval, checkpoint, mutex, queue,
)
from runtools.runjob.phase import BasePhase, ChildPhaseTerminated, PhaseTerminated, SequentialPhase, TimeoutExtension, \
    phase, timeout
from runtools.runjob.test.phase import FakeContext


class HostPhase(BasePhase):
    """A minimal phase that delegates to a callback for its _run() body."""
    __test__ = False

    TYPE = "HOST"

    def __init__(self, run_body, phase_id="host"):
        super().__init__(phase_id, self.TYPE)
        self._run_body = run_body

    def _run(self, ctx):
        return self._run_body()


@pytest.fixture
def ctx():
    return FakeContext()


def test_basic_execution(ctx):
    """@phase function called inside a phase's _run() executes and returns result."""
    @phase
    def greet(name):
        return f"hello {name}"

    host = HostPhase(lambda: greet("world"))
    result = host.run(ctx)

    assert result == "hello world"
    assert host.termination.status == TerminationStatus.COMPLETED


def test_return_value_propagates(ctx):
    """Return value propagates through run() and run_child()."""
    @phase
    def compute():
        return 42

    host = HostPhase(lambda: compute())
    result = host.run(ctx)

    assert result == 42


def test_child_appears_in_parent(ctx):
    """Child appears in parent's children list with correct id and type."""
    @phase
    def my_step():
        return "done"

    host = HostPhase(lambda: my_step())
    host.run(ctx)

    children = host.children
    assert len(children) == 1
    assert children[0].id == "my_step"
    assert children[0].type == "FUNCTION"
    assert children[0].termination.status == TerminationStatus.COMPLETED


def test_type_is_function(ctx):
    """Phase type is always FUNCTION for @phase decorated functions."""
    @phase
    def fetch_data():
        pass

    host = HostPhase(lambda: fetch_data())
    host.run(ctx)

    assert host.children[0].type == "FUNCTION"


def test_explicit_phase_id(ctx):
    """@phase(phase_id="ETL") overrides the auto-derived phase id."""

    @phase(phase_id="ETL")
    def do_work():
        pass

    host = HostPhase(lambda: do_work())
    host.run(ctx)

    assert host.children[0].id == "ETL"
    assert host.children[0].type == "FUNCTION"


def test_nesting(ctx):
    """@phase function calls another @phase function — nesting works correctly."""
    @phase
    def inner():
        return "inner_result"

    @phase
    def outer():
        return inner()

    host = HostPhase(lambda: outer())
    result = host.run(ctx)

    assert result == "inner_result"
    # outer is a child of host
    assert len(host.children) == 1
    outer_phase = host.children[0]
    assert outer_phase.id == "outer"
    # inner is a child of outer
    assert len(outer_phase.children) == 1
    assert outer_phase.children[0].id == "inner"


def test_phase_terminated_propagates(ctx):
    """PhaseTerminated raised inside @phase function works correctly."""
    @phase
    def failing_step():
        raise PhaseTerminated(TerminationStatus.FAILED, "intentional failure")

    host = HostPhase(lambda: failing_step())
    host.run(ctx)

    assert host.children[0].termination.status == TerminationStatus.FAILED
    assert host.termination.status == TerminationStatus.FAILED


def test_handled_child_exception_does_not_fail_parent(ctx):
    """Parent catches original exception from child and still completes."""
    @phase
    def bad_step():
        raise ValueError("boom")

    def run_body():
        try:
            bad_step()
        except ValueError:
            return "recovered"

    host = HostPhase(run_body)
    result = host.run(ctx)

    assert result == "recovered"
    assert host.children[0].termination.status == TerminationStatus.ERROR
    assert host.termination.status == TerminationStatus.COMPLETED


def test_child_phase_terminated_raises_child_phase_terminated(ctx):
    """PhaseTerminated in @phase function is re-raised as ChildPhaseTerminated."""
    @phase
    def maybe_fail():
        raise PhaseTerminated(TerminationStatus.FAILED, "expected")

    host = HostPhase(lambda: maybe_fail())
    host.run(ctx)

    assert host.children[0].termination.status == TerminationStatus.FAILED
    assert host.termination.status == TerminationStatus.FAILED


def test_handled_child_phase_terminated_does_not_fail_parent(ctx):
    """Parent catches ChildPhaseTerminated and continues successfully."""
    @phase
    def maybe_fail():
        raise PhaseTerminated(TerminationStatus.FAILED, "expected")

    def run_body():
        try:
            maybe_fail()
        except ChildPhaseTerminated:
            return "continued"

    host = HostPhase(run_body)
    result = host.run(ctx)

    assert result == "continued"
    assert host.children[0].termination.status == TerminationStatus.FAILED
    assert host.termination.status == TerminationStatus.COMPLETED


def test_error_outside_running_phase():
    """Calling @phase function outside a running phase raises RuntimeError."""
    @phase
    def orphan():
        pass

    with pytest.raises(RuntimeError, match="called outside a running phase"):
        orphan()


def test_multiple_calls_create_separate_children(ctx):
    """Multiple calls to same @phase function create separate children."""
    @phase
    def step(n):
        return n * 2

    def run_body():
        results = []
        for i in range(3):
            results.append(step(i))
        return results

    host = HostPhase(run_body)
    result = host.run(ctx)

    assert result == [0, 2, 4]
    assert len(host.children) == 3
    assert all(c.id == "step" for c in host.children)
    assert all(c.type == "FUNCTION" for c in host.children)


def test_integration_with_sequential_phase(ctx):
    """@phase functions work when called from a SequentialPhase child."""

    @phase
    def compute():
        return 99

    class InnerPhase(BasePhase):
        __test__ = False
        TYPE = "INNER"

        def __init__(self):
            super().__init__("inner", self.TYPE)
            self.result = None

        def _run(self, ctx):
            self.result = compute()

    inner = InnerPhase()
    seq = SequentialPhase("seq", [inner])
    seq.run(ctx)

    assert inner.result == 99
    assert len(inner.children) == 1
    assert inner.children[0].id == "compute"
    assert seq.termination.status == TerminationStatus.COMPLETED


def test_exception_in_phase_function(ctx):
    """Uncaught exception in @phase function propagates with original type."""
    @phase
    def bad_step():
        raise ValueError("something went wrong")

    host = HostPhase(lambda: bad_step())
    with pytest.raises(ValueError, match="something went wrong"):
        host.run(ctx)

    assert host.children[0].termination.status == TerminationStatus.ERROR
    assert host.termination.status == TerminationStatus.ERROR


def test_checkpoint_decorator(ctx):
    """@checkpoint wraps @phase with SequentialPhase([CheckpointPhase, FunctionPhase]); resume allows execution."""
    executed = []

    @checkpoint
    @phase
    def deploy(env):
        executed.append(env)
        return "deployed"

    def run_body():
        return deploy("prod")

    host = HostPhase(run_body)

    def resume_checkpoint():
        # Wait for the checkpoint phase to start running
        while True:
            children = host.children
            if not children:
                continue
            seq = children[0]
            cp = seq.children[0]
            if cp.started_at and not cp.termination:
                cp.resume()
                break

    t = threading.Thread(target=resume_checkpoint, daemon=True)
    t.start()
    host.run(ctx)
    t.join(timeout=5)

    assert executed == ["prod"]

    # Verify structure: host -> SequentialPhase -> [CheckpointPhase, FunctionPhase]
    seq = host.children[0]
    assert seq.type == SequentialPhase.TYPE
    assert seq.id == "deploy_seq"

    cp_phase, fn_phase = seq.children
    assert isinstance(cp_phase, CheckpointPhase)
    assert cp_phase.id == "deploy_checkpoint"
    assert fn_phase.id == "deploy"

    assert host.termination.status == TerminationStatus.COMPLETED


def test_checkpoint_decorator_with_timeout(ctx):
    """@checkpoint(timeout=...) terminates when timeout expires without resume."""
    @checkpoint(timeout=0.1)
    @phase
    def slow_deploy():
        return "should not reach"

    host = HostPhase(lambda: slow_deploy())
    host.run(ctx)

    seq = host.children[0]
    cp_phase = seq.children[0]
    assert cp_phase.termination.status == TerminationStatus.TIMEOUT
    assert host.termination.status == TerminationStatus.TIMEOUT


# --- @timeout ---

def test_timeout_structure():
    """@timeout wraps FunctionPhase with TimeoutExtension."""
    @timeout(30)
    @phase
    def deploy():
        pass

    p = deploy.create_phase()
    assert isinstance(p, TimeoutExtension)
    assert p.timeout_sec == 30
    # TimeoutExtension delegates id to the wrapped FunctionPhase
    assert p.id == "deploy"


def test_timeout_bare_raises():
    """@timeout without parentheses raises TypeError."""
    with pytest.raises(TypeError, match="@timeout requires parentheses"):
        @timeout
        @phase
        def deploy():
            pass


def test_timeout_execution(ctx):
    """@timeout allows execution to complete within the limit."""
    @timeout(5)
    @phase
    def fast():
        return "done"

    host = HostPhase(lambda: fast())
    result = host.run(ctx)

    assert result == "done"
    assert host.termination.status == TerminationStatus.COMPLETED


# --- @approval ---

def test_approval_structure():
    """@approval wraps FunctionPhase with ApprovalPhase."""
    @approval
    @phase
    def deploy():
        pass

    p = deploy.create_phase()
    assert isinstance(p, ApprovalPhase)
    assert p.id == "deploy_approval"
    assert len(p.children) == 1
    assert p.children[0].id == "deploy"
    assert p.children[0].type == "FUNCTION"


def test_approval_with_timeout_structure():
    """@approval(timeout=60) passes timeout to ApprovalPhase."""
    @approval(timeout=60)
    @phase
    def deploy():
        pass

    p = deploy.create_phase()
    assert isinstance(p, ApprovalPhase)
    assert p.id == "deploy_approval"
    assert p._timeout == 60


def test_approval_execution(ctx):
    """@approval blocks until approved, then executes the function phase."""
    executed = []

    @approval
    @phase
    def deploy(env):
        executed.append(env)
        return "deployed"

    host = HostPhase(lambda: deploy("prod"))

    def approve_phase():
        while True:
            children = host.children
            if not children:
                continue
            approval_phase = children[0]
            if approval_phase.started_at and not approval_phase.termination:
                approval_phase.approve()
                break

    t = threading.Thread(target=approve_phase, daemon=True)
    t.start()
    host.run(ctx)
    t.join(timeout=5)

    assert executed == ["prod"]
    assert host.termination.status == TerminationStatus.COMPLETED


def test_approval_timeout_skips(ctx):
    """@approval(timeout=...) skips when timeout expires without approval."""
    @approval(timeout=0.1)
    @phase
    def deploy():
        return "should not reach"

    host = HostPhase(lambda: deploy())
    host.run(ctx)

    approval_phase = host.children[0]
    assert approval_phase.termination.status == TerminationStatus.SKIPPED


# --- @mutex ---

def test_mutex_structure():
    """@mutex wraps FunctionPhase with MutualExclusionPhase."""
    @mutex
    @phase
    def deploy():
        pass

    p = deploy.create_phase()
    assert isinstance(p, MutualExclusionPhase)
    assert p.id == "deploy_mutex"
    assert len(p.children) == 1
    assert p.children[0].id == "deploy"
    assert p.children[0].type == "FUNCTION"


def test_mutex_with_group_structure():
    """@mutex(group="deploys") sets exclusion_group on MutualExclusionPhase."""
    @mutex(group="deploys")
    @phase
    def deploy():
        pass

    p = deploy.create_phase()
    assert isinstance(p, MutualExclusionPhase)
    assert p.id == "deploy_mutex"
    assert p.exclusion_group == "deploys"


def test_mutex_default_group_is_none():
    """Bare @mutex leaves exclusion_group as None (falls back to job_id at runtime)."""
    @mutex
    @phase
    def deploy():
        pass

    p = deploy.create_phase()
    assert p.exclusion_group is None


# --- @queue ---

def test_queue_structure():
    """@queue wraps FunctionPhase with ExecutionQueue."""
    @queue(max_concurrent=2)
    @phase
    def deploy():
        pass

    p = deploy.create_phase()
    assert isinstance(p, ExecutionQueue)
    assert p.id == "deploy_queue"
    assert len(p.children) == 1
    assert p.children[0].id == "deploy"
    assert p.children[0].type == "FUNCTION"


def test_queue_with_group_structure():
    """@queue(group=..., max_concurrent=...) sets concurrency group."""
    @queue(group="deploys", max_concurrent=3)
    @phase
    def deploy():
        pass

    p = deploy.create_phase()
    assert isinstance(p, ExecutionQueue)
    assert p.id == "deploy_queue"
    assert p.execution_group == ConcurrencyGroup("deploys", 3)


def test_queue_default_group_uses_phase_id():
    """@queue without group= defaults group_id to the phase id."""
    @queue(max_concurrent=1)
    @phase
    def deploy():
        pass

    p = deploy.create_phase()
    assert p.execution_group.group_id == "deploy"
    assert p.execution_group.max_executions == 1


# --- Composition ---

def test_timeout_approval_composition():
    """@timeout + @approval produces TimeoutExtension(ApprovalPhase(FunctionPhase))."""
    @timeout(60)
    @approval
    @phase
    def deploy():
        pass

    p = deploy.create_phase()
    # Outermost: TimeoutExtension
    assert isinstance(p, TimeoutExtension)
    assert p.timeout_sec == 60
    # TimeoutExtension delegates id to wrapped ApprovalPhase
    assert p.id == "deploy_approval"

    # Inner: ApprovalPhase
    wrapped = p._wrapped
    assert isinstance(wrapped, ApprovalPhase)
    assert wrapped.id == "deploy_approval"
    assert wrapped.children[0].id == "deploy"


def test_timeout_mutex_composition():
    """@timeout + @mutex produces TimeoutExtension(MutualExclusionPhase(FunctionPhase))."""
    @timeout(30)
    @mutex(group="deploys")
    @phase
    def deploy():
        pass

    p = deploy.create_phase()
    assert isinstance(p, TimeoutExtension)
    assert p.timeout_sec == 30

    wrapped = p._wrapped
    assert isinstance(wrapped, MutualExclusionPhase)
    assert wrapped.exclusion_group == "deploys"
    assert wrapped.children[0].id == "deploy"


def test_checkpoint_approval_composition():
    """@checkpoint + @approval produces SequentialPhase([CheckpointPhase, ApprovalPhase(FunctionPhase)])."""
    @checkpoint
    @approval
    @phase
    def deploy():
        pass

    p = deploy.create_phase()
    # Outermost: SequentialPhase (from checkpoint)
    assert isinstance(p, SequentialPhase)
    assert p.id == "deploy_approval_seq"

    cp_phase, approval_phase = p.children
    assert isinstance(cp_phase, CheckpointPhase)
    assert cp_phase.id == "deploy_approval_checkpoint"
    assert isinstance(approval_phase, ApprovalPhase)
    assert approval_phase.children[0].id == "deploy"


# --- Stop propagation ---

def test_parent_stopped_when_child_cooperatively_stops(ctx):
    """Parent that catches ChildPhaseTerminated still gets STOPPED when it was stopped externally.

    Regression: previously the parent defaulted to COMPLETED because the handled child's
    _failure_raised flag caused the finally-block scan to skip it.
    """
    stop_flag = threading.Event()

    @phase
    def cooperative_child():
        stop_flag.wait()
        raise PhaseTerminated(TerminationStatus.STOPPED, "cooperative stop")

    def run_body():
        try:
            cooperative_child()
        except ChildPhaseTerminated:
            return None

    host = HostPhase(run_body)

    def stop_after_start():
        # Wait until child is running
        while not host.children:
            pass
        host.stop()
        stop_flag.set()

    t = threading.Thread(target=stop_after_start, daemon=True)
    t.start()
    host.run(ctx)
    t.join(timeout=5)

    assert host.termination.status == TerminationStatus.STOPPED
