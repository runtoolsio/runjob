import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from runtools.runcore.job import iid
from runtools.runcore.output import OutputLine
from runtools.runcore.output.file import SourceIndex
from runtools.runcore.retention import RetentionPolicy
from runtools.runjob.output.file import FileOutputStore, FileOutputWriter


@pytest.fixture
def store(tmp_path):
    return FileOutputStore(tmp_path)


def _touch(path: Path, content=""):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def test_create_writer(store, tmp_path):
    writer = store.create_writer(iid('myjob', 'run1'))
    assert writer.file_path == tmp_path / 'myjob' / 'run1__1.jsonl'


def test_enforce_retention_prunes_oldest_files(store, tmp_path):
    job_dir = tmp_path / 'myjob'
    for i in range(5):
        _touch(job_dir / f'run{i}.jsonl', f'data{i}')
        os.utime(job_dir / f'run{i}.jsonl', (i, i))

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


def test_enforce_retention_deletes_index_files(store, tmp_path):
    job_dir = tmp_path / 'myjob'
    for i in range(3):
        _touch(job_dir / f'run{i}.jsonl', f'data{i}')
        _touch(job_dir / f'run{i}.jsonl.idx', '{}')
        os.utime(job_dir / f'run{i}.jsonl', (i, i))

    store.enforce_retention('myjob', RetentionPolicy(max_runs_per_job=1, max_runs_per_env=-1))

    remaining = sorted(f.name for f in job_dir.iterdir())
    assert remaining == ['run2.jsonl', 'run2.jsonl.idx']


# --- FileOutputWriter ---

class TestFileOutputWriter:

    def test_writes_jsonl_and_creates_index(self, tmp_path):
        path = tmp_path / "out.jsonl"
        writer = FileOutputWriter(str(path))
        writer.store_line(OutputLine("line1", 1, source="EXEC"))
        writer.store_line(OutputLine("line2", 2, source="EXEC"))
        writer.close()

        with open(path) as f:
            lines = [json.loads(line) for line in f if line.strip()]
        assert len(lines) == 2

        index = SourceIndex.load(path)
        assert index is not None
        assert "EXEC" in index.sources

    def test_no_index_when_no_sources(self, tmp_path):
        path = tmp_path / "out.jsonl"
        writer = FileOutputWriter(str(path))
        writer.store_line(OutputLine("line1", 1))  # source=None
        writer.close()

        assert not SourceIndex.path_for(path).exists()

    def test_close_still_closes_file_on_index_save_failure(self, tmp_path):
        path = tmp_path / "out.jsonl"
        writer = FileOutputWriter(str(path))
        writer.store_line(OutputLine("line1", 1, source="X"))

        with patch.object(SourceIndex, 'save', side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                writer.close()

        assert writer._file.closed

    def test_close_is_idempotent(self, tmp_path):
        path = tmp_path / "out.jsonl"
        writer = FileOutputWriter(str(path))
        writer.store_line(OutputLine("line1", 1))
        writer.close()
        writer.close()  # Should not raise
