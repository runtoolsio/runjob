"""
Public API of this package is imported here, and it is safe to use by plugins.
Any API in submodules (except 'util' module) is a subject to change and doesn't ensure backwards compatibility.

IMPLEMENTATION NOTE:
    Avoid importing any module depending on any external package. This allows to use this module without installing
    additional packages.
"""
from threading import Thread

from runtoolsio.runcore import cfg, log, persistence
from runtoolsio.runcore.job import JobInstance
from runtoolsio.runcore.run import Phaser, PhaseNames
from runtoolsio.runcore.util import lock
from runtoolsio.runjob.execution import ExecutingPhase
from runtoolsio.runjob.featurize import FeaturedContextBuilder
from runtoolsio.runjob.runner import RunnerJobInstance

__version__ = "0.11.0"


def load_config(config=None, **kwargs):
    cfg.load_from_file(config)
    configure(**kwargs)


def configure(**kwargs):
    """
    Args:
        **kwargs:
            log_mode (LogMode, str, bool): Sets a logging mode, see `cfg.LogMode` enum for more information
            log_stdout_level (str): Used only with `LogMode.ENABLED`, registers stdout+stderr handler with given level
            log_file_level (str): Used only with `LogMode.ENABLED`, registers file handler with given level
            log_file_path (str): Custom log file path for the file handler

    For more information about logging see the `log` module documentation.
    """
    cfg.set_variables(**kwargs)
    log.init_by_config()


def execute(job_id, job_execution, coordinations=None, *, instance_id=None):
    plugins_ = cfg.plugins_load if cfg.plugins_enabled else None
    with FeaturedContextBuilder().standard_features(plugins=plugins_).build_as_run() as ctx:
        instance = ctx.add(job_instance(
            job_id,
            job_execution,
            coordinations,
            instance_id=instance_id))
        instance.run()
        return job_instance


def execute_in_new_thread(job_id, job_execution, no_overlap=False, depends_on=None, pending_group=None):
    Thread(target=execute, args=(job_id, job_execution, no_overlap, depends_on, pending_group)).start()


def close():
    persistence.close()


def job_instance(job_id, exec_, *, instance_id=None, **user_params) -> RunnerJobInstance:
    return RunnerJobInstance(job_id, Phaser([ExecutingPhase(PhaseNames.EXEC, exec_)]), run_id=instance_id, user_params=user_params)


def run(job_id, execution, sync_=None, state_locker=lock.default_queue_locker(), *, instance_id=None,
        **user_params) -> JobInstance:
    instance = job_instance(job_id, execution, sync_, state_locker, instance_id=instance_id, user_params=user_params)
    instance.run()
    return instance


def job_instance_uncoordinated(job_id, exec_, *, instance_id=None, **user_params) \
        -> JobInstance:
    return RunnerJobInstance(job_id, Phaser([ExecutingPhase(PhaseNames.EXEC, exec_)]), run_id=instance_id,
                             user_params=user_params)


def run_uncoordinated(job_id, exec_, *, instance_id=None, **user_params) -> JobInstance:
    instance = job_instance_uncoordinated(job_id, exec_, instance_id=instance_id,
                                          user_params=user_params)
    instance.run()
    return instance
