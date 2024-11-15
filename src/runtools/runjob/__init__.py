"""
Public API of this package is imported here, and it is safe to use by plugins.
Any API in submodules (except 'util' module) is a subject to change and doesn't ensure backwards compatibility.

IMPLEMENTATION NOTE:
    Avoid importing any module depending on any external package. This allows to use this module without installing
    additional packages.
"""
from threading import Thread
from typing import List

from runtools.runcore import util, InvalidConfiguration
from runtools.runcore.job import JobInstance
from runtools.runcore.util import lock
from runtools.runcore.util.socket import SocketClient
from runtools.runjob.execution import ExecutingPhase
from runtools.runjob.featurize import FeaturedContextBuilder
from runtools.runjob.phaser import Phaser
from runtools.runjob.runner import RunnerJobInstance

__version__ = "0.11.0"

_plugins = ()
_persistence = ()


def configure(**kwargs):
    # TODO:
    # max age and max records
    # lock_timeout_sec = 10
    # lock_max_check_time_sec = 0.05

    plugins_obj = kwargs.get("plugins", {"enabled": False, "load": ()})
    if plugins_obj.get("enabled", True):
        global _plugins
        _plugins = tuple(plugins_obj.get("load", ()))

    persistence_array = kwargs.get("persistence", ())
    dbs = []  # TODO max age and max records
    for p in persistence_array:
        if "type" not in p:
            raise InvalidConfiguration("Field `type` is mandatory in `persistence` configuration object")
        if not p.get("enabled", True):
            continue
        dbs.append(p)
    global _persistence
    _persistence = tuple(dbs)


def job_instance(job_id, phases, output=None, task_tracker=None, *, run_id=None, instance_id=None, **user_params)\
        -> RunnerJobInstance:
    instance_id = instance_id or util.unique_timestamp_hex()
    with FeaturedContextBuilder().standard_features(plugins=_plugins).build() as ctx:
        phaser = Phaser(phases)
        instance = RunnerJobInstance(job_id, instance_id, phaser, output, task_tracker, run_id=run_id, **user_params)
        return ctx.add(instance)


def run_job(job_id, phases, output=None, task_tracker=None, *, run_id=None, instance_id=None, **user_params):
    job_instance(job_id, phases, output, task_tracker, run_id=run_id, instance_id=instance_id, **user_params).run()


def execute(job_id, job_execution, coordinations=None, *, instance_id=None):
    with FeaturedContextBuilder().standard_features(plugins=_plugins).build_as_run() as ctx:
        instance = ctx.add(job_instance(
            job_id,
            job_execution,
            coordinations,
            instance_id=instance_id))
        instance.run()
        return job_instance


def execute_in_new_thread(job_id, job_execution, no_overlap=False, depends_on=None, pending_group=None):
    Thread(target=execute, args=(job_id, job_execution, no_overlap, depends_on, pending_group)).start()


def run(job_id, execution, sync_=None, state_locker=lock.default_queue_locker(), *, instance_id=None,
        **user_params) -> JobInstance:
    instance = job_instance(job_id, execution, sync_, state_locker, instance_id=instance_id, user_params=user_params)
    instance.run()
    return instance


def job_instance_uncoordinated(job_id, exec_, *, instance_id=None, **user_params) \
        -> JobInstance:
    return RunnerJobInstance(job_id, instance_id, Phaser([ExecutingPhase(job_id, exec_)]), run_id=instance_id,
                             user_params=user_params)


def run_uncoordinated(job_id, exec_, *, instance_id=None, **user_params) -> JobInstance:
    instance = job_instance_uncoordinated(job_id, exec_, instance_id=instance_id, user_params=user_params)
    instance.run()
    return instance


def clean_stale_sockets(file_extension) -> List[str]:
    cleaned = []

    c = SocketClient(file_extension)
    try:
        ping_result = c.ping()
    finally:
        c.close()

    for stale_socket in ping_result.stale_sockets:
        stale_socket.unlink(missing_ok=True)
        cleaned.append(stale_socket.name)

    return cleaned

