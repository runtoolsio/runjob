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

    Args:
        func: The function to decorate (when used as ``@job`` without parentheses).
        job_id: Override job ID (defaults to function name).
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

    def __call__(self, *args, **kwargs):
        run_id = self._take(kwargs, 'run_id', None)
        env = self._take(kwargs, 'env', None) or self._env
        duplicate_strategy = self._take(kwargs, 'duplicate_strategy', self._default_duplicate_strategy)
        call_tags = _coerce_tags(self._take(kwargs, 'tags', ()))
        # Merge: decoration-time + call-time. JobInstanceMetadata.__post_init__
        # normalizes + dedupes, so passing both raw and pre-normalized is safe.
        tags = (*self._default_tags, *call_tags)

        root_phase = FunctionPhase(self._job_id, self._func, args, kwargs)

        with node.connect(env) as env_node:
            inst = env_node.create_instance(
                self._job_id, run_id, root_phase,
                duplicate_strategy=duplicate_strategy,
                tags=tags,
            )
            retval = inst.run()
            return JobResult(run=inst.snap(), retval=retval)
