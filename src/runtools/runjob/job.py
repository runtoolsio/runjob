"""Decorator that turns a plain function into a runnable job.

Usage::

    @job
    def pipeline():
        fetch_data()
        transform()

    result = pipeline()                     # auto-generated run_id, default env
    result = pipeline(run_id="batch-42")    # explicit run_id
    result = pipeline(env="staging")        # override env at call time

    result.run       # JobRun snapshot
    result.retval    # return value of the decorated function
"""
import functools
import inspect
from dataclasses import dataclass
from typing import Any, Iterable, Tuple

from runtools.runcore.job import DuplicateStrategy, JobRun
from runtools.runjob import node
from runtools.runjob.phase import FunctionPhase


def _coerce_tags(value: Iterable[str]) -> Tuple[str, ...]:
    """Tuple-ify a tag iterable; reject a bare string up-front.

    A bare ``str`` is iterable as characters, so ``tags="prod"`` would silently
    become ``("p", "r", "o", "d")``. Catch it here before the value reaches
    ``normalize_tags`` (which already rejects bare strings via the same rule).
    """
    if isinstance(value, str):
        raise ValueError(
            f"tags must be an iterable of strings, got a single str: {value!r}. "
            "Wrap in a list/tuple, e.g. (\"prod\",)."
        )
    return tuple(value)


@dataclass
class JobResult:
    """Result of a @job-decorated function call."""
    run: JobRun
    retval: Any = None


def job(func=None, *, job_id=None, env=None,
        duplicate_strategy: DuplicateStrategy = DuplicateStrategy.RAISE,
        tags: Iterable[str] = ()):
    """Decorator that turns a plain function into a runnable job.

    Can be used bare (``@job``) or with arguments (``@job(job_id="etl", env="staging")``).
    When the decorated function is called, it creates an environment node, wraps the function
    in a ``FunctionPhase`` as root phase, runs the instance, and returns the ``JobRun`` snapshot.

    Structuring a job — phases vs operations:
        Start simple: one ``@job`` (= one root phase) with **operations** inside for
        progress reporting. Operations are the lightweight "what is the job doing
        right now" channel (status tracking) — reach for them by default.

        Add ``@phase`` children only when a piece of work needs its OWN lifecycle —
        i.e. one of:
            - independent fail/retry boundary
            - mutex / queue / approval / checkpoint semantics
            - distinct outcome visible in run inspection

        Heuristic: if removing the structure would silently mask a real failure
        or lose a gate the job depends on, it should be a phase. Otherwise it's
        an operation.

    Args:
        func: The function to decorate (when used as ``@job`` without parentheses).
        job_id: Default job ID at decoration time (defaults to function name).
            Can also be overridden **per-call** via ``job_id=`` kwarg — useful when
            the granularity isn't fixed at code-write time (e.g.
            ``import_catalog_{site}_{locale}`` derived from runtime inputs).
            The per-call value flows into both the DB row identity and the root
            phase's id (and through to any mutex wrapper's id).
        env: Default environment ID (can be overridden at call time via ``env`` kwarg).
        duplicate_strategy: Default duplicate handling. ``ALLOW`` auto-increments
            ``InstanceID.ordinal`` so concurrent runs of ``job_id@run_id`` coexist.
            Overridable per-call via ``duplicate_strategy=...``.
        tags: Default tags applied to every run of this job. Merged with per-call
            ``tags=(...)`` (decoration-time + call-time, deduped at the metadata layer).
    """
    default_tags = _coerce_tags(tags)
    if func is None:
        return lambda f: _JobDecor(f, job_id, env, duplicate_strategy, default_tags)
    return _JobDecor(func, job_id, env, duplicate_strategy, default_tags)


class _JobDecor:

    # Decorator-level kwargs consumed by __call__. Also stripped defensively in
    # create_phase so coordination decorators (@mutex / @queue / @timeout) that
    # invoke the factory directly never leak them into the wrapped function.
    _RESERVED_KWARGS = ('job_id', 'run_id', 'env', 'duplicate_strategy', 'tags')

    def __init__(self, func, job_id, env, duplicate_strategy, tags: Tuple[str, ...]):
        functools.update_wrapper(self, func)
        self._func = func
        self._job_id = job_id or func.__name__
        self._env = env
        self._default_duplicate_strategy = duplicate_strategy
        self._default_tags = tags
        self._func_params = set(inspect.signature(func).parameters)

    def _take(self, kwargs, name, default):
        """Pop ``name`` from kwargs, but only if the wrapped function doesn't declare it.

        If the wrapped function has a parameter with the same name, the value is
        passed through to the function — caller intent is to populate the
        function arg, not configure the decorator.
        """
        if name in self._func_params:
            return default
        return kwargs.pop(name, default)

    def create_phase(self, *args, _phase_id_override=None, **kwargs):
        """Build the root phase for this job's run.

        Same protocol as :class:`runtools.runjob.phase._PhaseDecor.create_phase`
        — coordination decorators (``@mutex``, ``@queue``, ``@timeout``) wrap
        the resulting phase by replacing this method on the decor instance.
        Letting ``@job`` and ``@phase`` share this protocol means stacking
        ``@mutex @job`` works without coordination decorators needing to know
        which kind of decor they wrap.

        ``_phase_id_override`` lets ``__call__`` propagate a per-call ``job_id``
        into the root phase's id, so a dynamic job identity (e.g.
        ``import_catalog_{site}_{locale}``) shows up consistently in the
        phase tree and in any mutex wrapper's id.

        Strips runtools-reserved kwargs defensively so the wrapped function
        never sees them, regardless of who invokes this method. Job-level
        options (tags, duplicate_strategy, env, run_id) are applied by
        :meth:`__call__`, not here — direct callers of create_phase get a
        clean phase but no instance.
        """
        for name in self._RESERVED_KWARGS:
            if name not in self._func_params:
                kwargs.pop(name, None)
        return FunctionPhase(_phase_id_override or self._job_id, self._func, args, kwargs)

    def __call__(self, *args, **kwargs):
        job_id = self._take(kwargs, 'job_id', None) or self._job_id
        run_id = self._take(kwargs, 'run_id', None)
        env = self._take(kwargs, 'env', None) or self._env
        duplicate_strategy = self._take(kwargs, 'duplicate_strategy', self._default_duplicate_strategy)
        call_tags = _coerce_tags(self._take(kwargs, 'tags', ()))
        # Merge: decoration-time + call-time. JobInstanceMetadata.__post_init__
        # normalizes + dedupes, so passing both raw and pre-normalized is safe.
        tags = (*self._default_tags, *call_tags)

        root_phase = self.create_phase(*args, _phase_id_override=job_id, **kwargs)

        with node.connect(env) as env_node:
            inst = env_node.create_instance(
                job_id, run_id, root_phase,
                duplicate_strategy=duplicate_strategy,
                tags=tags,
            )
            retval = inst.run()
            return JobResult(run=inst.snap(), retval=retval)
