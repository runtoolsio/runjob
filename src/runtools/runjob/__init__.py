"""
Public API of this package is imported here, and it is safe to use by plugins.
Any API in submodules (except 'util' module) is a subject to change and doesn't ensure backwards compatibility.

IMPLEMENTATION NOTE:
    Avoid importing any module depending on any external package. This allows to use this module without installing
    additional packages.
"""
from typing import List

from runtools.runcore import InvalidConfiguration
from runtools.runcore.util.socket import SocketClient
from runtools.runjob import instance
from runtools.runjob.featurize import FeaturedContextBuilder
from runtools.runjob.instance import _JobInstance, JobInstanceHook

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
