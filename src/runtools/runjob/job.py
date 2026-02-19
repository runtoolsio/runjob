"""Decorator that turns a plain function into a runnable job.

Usage::

    @job
    def pipeline():
        fetch_data()
        transform()

    run = pipeline()                     # auto-generated run_id, default env
    run = pipeline(run_id="batch-42")    # explicit run_id
    run = pipeline(env="staging")        # override env at call time
"""
import functools
import inspect

from runtools.runcore.job import iid
from runtools.runjob import node
from runtools.runjob.phase import FunctionPhase


def job(func=None, *, job_id=None, env=None):
    """Decorator that turns a plain function into a runnable job.

    Can be used bare (``@job``) or with arguments (``@job(job_id="etl", env="staging")``).
    When the decorated function is called, it creates an environment node, wraps the function
    in a ``FunctionPhase`` as root phase, runs the instance, and returns the ``JobRun`` snapshot.

    Args:
        func: The function to decorate (when used as ``@job`` without parentheses).
        job_id: Override job ID (defaults to function name).
        env: Default environment ID (can be overridden at call time via ``env`` kwarg).
    """
    if func is None:
        return lambda f: _JobDecor(f, job_id, env)
    return _JobDecor(func, job_id, env)


class _JobDecor:

    def __init__(self, func, job_id, env):
        functools.update_wrapper(self, func)
        self._func = func
        self._job_id = job_id or func.__name__
        self._env = env
        self._func_params = set(inspect.signature(func).parameters)

    def __call__(self, *args, **kwargs):
        run_id = kwargs.pop('run_id', None) if 'run_id' not in self._func_params else None
        env = kwargs.pop('env', None) if 'env' not in self._func_params else None
        env = env or self._env

        root_phase = FunctionPhase(self._job_id, self._func, args, kwargs)
        instance_id = iid(self._job_id, run_id) if run_id else iid(self._job_id)

        with node.connect(env) as env_node:
            inst = env_node.create_instance(instance_id, root_phase)
            inst.run()
            return inst.snap()
