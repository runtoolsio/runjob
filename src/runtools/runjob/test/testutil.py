from pathlib import Path

from runtools.runcore import paths, util


def ensure_test_runtime_dir():
    return paths.ensure_dirs(paths.runtime_dir())


def random_test_socket():
    return paths.ensure_dirs(paths.runtime_dir()) / Path(util.unique_timestamp_hex() + ".sock")
