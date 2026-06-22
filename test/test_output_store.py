import json
from pathlib import Path
from unittest.mock import patch

import pytest

from runtools.runcore.job import iid
from runtools.runcore.output import OutputLine
from runtools.runcore.output.file import SourceIndex
from runtools.runjob.output.file import FileOutputStore, FileOutputSink


@pytest.fixture
def store(tmp_path):
    return FileOutputStore(tmp_path)


def test_create_sink(store, tmp_path):
    writer = store.create_sink(iid('myjob', 'run1'))
    assert writer.file_path == tmp_path / 'myjob' / 'run1__1.jsonl'


# --- FileOutputSink ---

class TestFileOutputSink:

    def test_writes_jsonl_and_creates_index(self, tmp_path):
        path = tmp_path / "out.jsonl"
        writer = FileOutputSink(str(path), compress=False)
        writer.write_line(OutputLine("line1", 1, source="EXEC"))
        writer.write_line(OutputLine("line2", 2, source="EXEC"))
        writer.close()

        with open(path) as f:
            lines = [json.loads(line) for line in f if line.strip()]
        assert len(lines) == 2

        index = SourceIndex.load(path)
        assert index is not None
        assert "EXEC" in index.sources

    def test_compression(self, tmp_path):
        path = tmp_path / "out.jsonl"
        writer = FileOutputSink(str(path), compress=True)
        writer.write_line(OutputLine("line1", 1, source="EXEC"))
        writer.write_line(OutputLine("line2", 2, source="EXEC"))
        writer.close()

        assert not path.exists()
        gz_path = Path(str(path) + ".gz")
        assert gz_path.exists()
        # Index file still exists (byte offsets apply to decompressed content)
        assert SourceIndex.path_for(path).exists()

    def test_no_index_when_no_sources(self, tmp_path):
        path = tmp_path / "out.jsonl"
        writer = FileOutputSink(str(path), compress=False)
        writer.write_line(OutputLine("line1", 1))  # source=None
        writer.close()

        assert not SourceIndex.path_for(path).exists()

    def test_close_still_closes_file_on_index_save_failure(self, tmp_path):
        path = tmp_path / "out.jsonl"
        writer = FileOutputSink(str(path))
        writer.write_line(OutputLine("line1", 1, source="X"))

        with patch.object(SourceIndex, 'save', side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                writer.close()

        assert writer._file.closed

    def test_close_is_idempotent(self, tmp_path):
        path = tmp_path / "out.jsonl"
        writer = FileOutputSink(str(path))
        writer.write_line(OutputLine("line1", 1))
        writer.close()
        writer.close()  # Should not raise
