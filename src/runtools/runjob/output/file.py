"""File-backed output store — JSONL writer with source indexing.

Write-side module contract:
    create_store(env_id, config) -> FileOutputStore
"""

import gzip
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List

from runtools.runcore.job import InstanceID
from runtools.runcore.output import OutputLine, OutputLocation, OutputStorageConfig
from runtools.runcore.output.file import FileOutputBackend, SourceIndexBuilder, _parse_file_config, _resolve_base_dir
from runtools.runjob.output import OutputSink, OutputStore

log = logging.getLogger(__name__)


def _gz_path(jsonl_path: Path) -> Path:
    return jsonl_path.with_suffix(jsonl_path.suffix + '.gz')


class FileOutputSink(OutputSink):

    def __init__(self, file_path: str, append: bool = False, encoding: str = "utf-8", compress: bool = True):
        self.file_path = Path(file_path)
        self._encoding = encoding
        self._compress = compress
        self._closed = False
        self._index_builder = SourceIndexBuilder()

        os.makedirs(self.file_path.parent, exist_ok=True)
        self._file = open(self.file_path, "ab" if append else "wb")

    @property
    def location(self):
        return OutputLocation.for_file(self.file_path)

    def write_line(self, line: OutputLine):
        self._write_line(line)
        self._file.flush()

    def write_lines(self, lines: List[OutputLine]):
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
        if self._compress and self.file_path.exists():
            new_path = _compress_file(self.file_path)
            if new_path is not None:
                self.file_path = new_path

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class FileOutputStore(FileOutputBackend, OutputStore):
    """File-backed output store. Inherits read from FileOutputBackend, adds write."""

    def __init__(self, base_dir: Path, *, compress: bool = True):
        super().__init__(base_dir)
        self._compress = compress

    def create_sink(self, instance_id: InstanceID, *, created_at: datetime) -> FileOutputSink:
        del created_at  # unused by the file backend
        path = self._output_path(instance_id)
        os.makedirs(path.parent, exist_ok=True)
        return FileOutputSink(str(path), compress=self._compress)


def _compress_file(path: Path) -> Path | None:
    """Gzip a file in place: write .gz.tmp, atomic rename, delete original. Returns .gz path on success."""
    gz_path = _gz_path(path)
    tmp_path = Path(str(gz_path) + ".tmp")
    try:
        with open(path, "rb") as f_in, gzip.open(tmp_path, "wb") as f_out:
            while chunk := f_in.read(64 * 1024):
                f_out.write(chunk)
        os.replace(tmp_path, gz_path)
    except OSError:
        log.warning("Failed to compress output file", extra={"path": str(path)}, exc_info=True)
        tmp_path.unlink(missing_ok=True)
        return None
    try:
        path.unlink()
    except OSError:
        log.warning("Compressed file created but failed to remove original", extra={"path": str(path)}, exc_info=True)
    return gz_path


def create_store(env_id: str, config: OutputStorageConfig) -> FileOutputStore:
    """Write-side module factory — part of the output store module contract."""
    file_cfg = _parse_file_config(config)
    base_dir = _resolve_base_dir(env_id, file_cfg, create=True)
    return FileOutputStore(base_dir, compress=file_cfg.compress)
