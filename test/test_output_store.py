import time
from pathlib import Path

import pytest

from runtools.runcore.job import iid
from runtools.runcore.retention import RetentionPolicy
from runtools.runjob.output import FileOutputStore


@pytest.fixture
def store(tmp_path):
    return FileOutputStore(tmp_path)


def _touch(path: Path, content=""):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def test_create_writer(store, tmp_path):
    writer = store.create_writer(iid('myjob', 'run1'))
    assert writer.file_path == str(tmp_path / 'myjob' / 'run1.jsonl')


def test_enforce_retention_prunes_oldest_files(store, tmp_path):
    job_dir = tmp_path / 'myjob'
    for i in range(5):
        _touch(job_dir / f'run{i}.jsonl', f'data{i}')
        time.sleep(0.01)  # ensure distinct mtime

    store.enforce_retention('myjob', RetentionPolicy(max_runs_per_job=2, max_runs_per_env=-1))

    remaining = sorted(f.name for f in job_dir.iterdir())
    assert remaining == ['run3.jsonl', 'run4.jsonl']


def test_enforce_retention_noop_when_under_limit(store, tmp_path):
    job_dir = tmp_path / 'myjob'
    for i in range(2):
        _touch(job_dir / f'run{i}.jsonl')

    store.enforce_retention('myjob', RetentionPolicy(max_runs_per_job=5, max_runs_per_env=-1))

    assert len(list(job_dir.iterdir())) == 2


def test_enforce_retention_missing_job_dir(store):
    # Should not raise for nonexistent job directory
    store.enforce_retention('nonexistent', RetentionPolicy(max_runs_per_job=1, max_runs_per_env=-1))
