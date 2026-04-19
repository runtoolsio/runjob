"""File-backed output store — JSONL writer with source indexing.

Write-side module contract:
    create_store(env_id, config) -> FileOutputStore
"""

import json
import logging
import os
from pathlib import Path
from typing import List

from runtools.runcore.job import InstanceID
from runtools.runcore.output import OutputLine, OutputLocation, OutputStorageConfig
from runtools.runcore.output.file import FileOutputBackend, SourceIndex, SourceIndexBuilder, _resolve_base_dir
from runtools.runcore.retention import RetentionPolicy
from runtools.runjob.output import OutputWriter, OutputStore

log = logging.getLogger(__name__)


class FileOutputWriter(OutputWriter):

    def __init__(self, file_path: str, append: bool = False, encoding: str = "utf-8"):
        self.file_path = Path(file_path)
        self._encoding = encoding
        self._closed = False
        self._index_builder = SourceIndexBuilder()

        os.makedirs(self.file_path.parent, exist_ok=True)
        self._file = open(self.file_path, "ab" if append else "wb")

    @property
    def location(self):
        return OutputLocation.for_file(self.file_path)

    def store_line(self, line: OutputLine):
        self._write_line(line)
        self._file.flush()

    def store_lines(self, lines: List[OutputLine]):
        for line in lines:
            self._write_line(line)
        self._file.flush()

    def _write_line(self, line: OutputLine):
        raw = (json.dumps(line.serialize(), ensure_ascii=False) + "\n").encode(self._encoding)
        self._index_builder.track(line.source, len(raw))
        self._file.write(raw)

    def close(self):
        if self._closed:
            return
        try:
            if index := self._index_builder.build():
                index.save(self.file_path)
        finally:
            self._closed = True
            self._file.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class FileOutputStore(FileOutputBackend, OutputStore):
    """File-backed output store. Inherits read from FileOutputBackend, adds write + retention."""

    def __init__(self, base_dir: Path):
        super().__init__(base_dir)

    def create_writer(self, instance_id: InstanceID) -> FileOutputWriter:
        path = self._output_path(instance_id)
        os.makedirs(path.parent, exist_ok=True)
        return FileOutputWriter(str(path))

    def enforce_retention(self, job_id: str, policy: RetentionPolicy):
        job_dir = self._base_dir / job_id
        if not job_dir.is_dir():
            return

        files = sorted(job_dir.glob("*.jsonl"), key=lambda f: f.stat().st_mtime, reverse=True)
        if policy.max_runs_per_job >= 0:
            for f in files[policy.max_runs_per_job:]:
                try:
                    f.unlink()
                    SourceIndex.path_for(f).unlink(missing_ok=True)
                except OSError:
                    log.warning("Failed to delete output file", extra={"path": str(f)}, exc_info=True)


def create_store(env_id: str, config: OutputStorageConfig) -> FileOutputStore:
    """Write-side module factory — part of the output store module contract."""
    base_dir = _resolve_base_dir(env_id, config, create=True)
    return FileOutputStore(base_dir)
