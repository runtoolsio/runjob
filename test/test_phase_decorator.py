import pytest

from runtools.runcore.run import TerminationStatus
from runtools.runjob.phase import BasePhase, ChildPhaseTerminated, FunctionPhase, PhaseTerminated, SequentialPhase, phase
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

    def _stop_started_run(self, reason):
        pass


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
    assert children[0].type == "MY_STEP"
    assert children[0].termination.status == TerminationStatus.COMPLETED


def test_auto_derived_type(ctx):
    """Phase type is auto-derived from function name: my_func -> MY_FUNC."""
    @phase
    def fetch_data():
        pass

    host = HostPhase(lambda: fetch_data())
    host.run(ctx)

    assert host.children[0].type == "FETCH_DATA"


def test_explicit_type_override(ctx):
    """@phase("CUSTOM") overrides the auto-derived type."""
    @phase("CUSTOM")
    def do_work():
        pass

    host = HostPhase(lambda: do_work())
    host.run(ctx)

    assert host.children[0].type == "CUSTOM"


def test_explicit_type_with_keyword(ctx):
    """@phase(phase_type="CUSTOM") overrides the auto-derived type."""
    @phase(phase_type="EXTRACT")
    def do_work():
        pass

    host = HostPhase(lambda: do_work())
    host.run(ctx)

    assert host.children[0].type == "EXTRACT"


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
    assert all(c.type == "STEP" for c in host.children)


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

        def _stop_started_run(self, reason):
            pass

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
