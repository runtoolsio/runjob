"""S3-backed output store — buffer-and-flush JSONL writer with retention.

Write-side module contract:
    create_store(env_id, config) -> S3OutputStore

Strategy: per-run writer accumulates OutputLines in memory. On close(), serializes
to JSONL bytes, optionally gzips, and PUTs once to S3. One object per run.

Object metadata captured at PUT time (``runtools-created-at``, ``runtools-run-id``,
``runtools-ordinal``) drives retention ordering — never rely on S3 LastModified,
which can drift via replication, copy, or out-of-order PUTs.
"""

import gzip
import json
import logging
from datetime import datetime
from typing import List

from runtools.runcore.job import InstanceID
from runtools.runcore.output import (
    OutputLine, OutputLocation, OutputStorageConfig, format_timestamp, parse_timestamp,
)
from runtools.runcore.output.s3 import (
    S3OutputBackend,
    build_client,
    object_key,
    parse_s3_config,
)
from runtools.runcore.retention import RetentionPolicy
from runtools.runjob.output import OutputStore, OutputWriter

try:
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError as e:
    raise ImportError(
        "S3 output store requires boto3. Install with: pip install runtoolsio-runjob[s3]"
    ) from e

log = logging.getLogger(__name__)

META_CREATED_AT = "runtools-created-at"
META_RUN_ID = "runtools-run-id"
META_ORDINAL = "runtools-ordinal"
_DELETE_BATCH_MAX = 1000


class S3OutputWriter(OutputWriter):
    """Buffers OutputLines in memory, flushes to S3 as a single object on close."""

    def __init__(self, client, bucket: str, prefix: str, instance_id: InstanceID, *,
                 created_at: datetime, compress: bool = True):
        """Build a writer for one run's output.

        Args:
            client: boto3 S3 client.
            bucket: Target bucket name.
            prefix: Key prefix under the bucket (may be empty for bucket root).
            instance_id: Fully-qualified InstanceID. ``run_id`` and ``ordinal``
                are written into S3 metadata; the full triple drives the object
                key via :func:`object_key`.
            created_at: Canonical run creation timestamp (typically equal to
                ``root_phase.created_at`` and the partial DB row's ``created``).
                Written into S3 metadata to drive retention sort independently
                of S3 LastModified.
            compress: Gzip the payload on close. Affects ``Content-Encoding``
                and the chosen ``.jsonl`` vs ``.jsonl.gz`` suffix.
        """
        self._client = client
        self._bucket = bucket
        self._key = object_key(prefix, instance_id, compress=compress)
        self._instance_id = instance_id
        self._created_at = created_at
        self._compress = compress
        self._buffer: List[OutputLine] = []
        self._closed = False

    @property
    def location(self):
        return OutputLocation.for_s3(self._bucket, self._key)

    def store_line(self, line: OutputLine):
        self._buffer.append(line)

    def store_lines(self, lines: List[OutputLine]):
        self._buffer.extend(lines)

    def close(self):
        if self._closed:
            return
        try:
            self._put_object()
        except (ClientError, BotoCoreError):
            log.warning("Failed to upload output to S3",
                        extra={"bucket": self._bucket, "key": self._key}, exc_info=True)
            # Don't mark closed — buffer is still intact and a retry can succeed.
            raise
        self._closed = True

    def _put_object(self):
        payload = b"".join(
            (json.dumps(line.serialize(), ensure_ascii=False) + "\n").encode("utf-8")
            for line in self._buffer
        )
        kwargs = {
            "Bucket": self._bucket,
            "Key": self._key,
            "Body": gzip.compress(payload) if self._compress else payload,
            "ContentType": "application/x-ndjson",
            "Metadata": {
                META_CREATED_AT: format_timestamp(self._created_at),
                META_RUN_ID: self._instance_id.run_id,
                META_ORDINAL: str(self._instance_id.ordinal),
            },
        }
        if self._compress:
            kwargs["ContentEncoding"] = "gzip"
        self._client.put_object(**kwargs)


class S3OutputStore(S3OutputBackend, OutputStore):
    """S3 output store. Inherits read from S3OutputBackend, adds write + retention."""

    def __init__(self, client, bucket: str, prefix: str = "", *, compress: bool = True):
        super().__init__(client, bucket, prefix)
        self._compress = compress

    def create_writer(self, instance_id: InstanceID, *,
                      created_at: datetime) -> S3OutputWriter:
        return S3OutputWriter(
            self._client, self._bucket, self._prefix, instance_id,
            created_at=created_at, compress=self._compress,
        )

    def enforce_retention(self, job_id: str, policy: RetentionPolicy):
        if policy.max_runs_per_job < 0:
            return

        prefix = f"{self._prefix}/{job_id}/" if self._prefix else f"{job_id}/"
        # sort key = (epoch_seconds, key_name) — numeric primary so mixed metadata
        # (...Z) and fallback (...+00:00) values compare correctly, plus a key-name
        # tiebreaker for collisions at S3's 1s LastModified precision.
        keys_with_sort: list[tuple[str, tuple[float, str]]] = []
        paginator = self._client.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                for obj in page.get("Contents") or ():
                    sort_value = self._sort_key_for(obj)
                    keys_with_sort.append((obj["Key"], (sort_value, obj["Key"])))
        except (ClientError, BotoCoreError):
            log.warning("Failed to list S3 objects for retention",
                        extra={"bucket": self._bucket, "prefix": prefix}, exc_info=True)
            return

        keys_with_sort.sort(key=lambda kv: kv[1], reverse=True)
        to_delete = [k for k, _ in keys_with_sort[policy.max_runs_per_job:]]
        if not to_delete:
            return

        for i in range(0, len(to_delete), _DELETE_BATCH_MAX):
            batch = to_delete[i: i + _DELETE_BATCH_MAX]
            try:
                self._client.delete_objects(
                    Bucket=self._bucket,
                    Delete={"Objects": [{"Key": k} for k in batch]},
                )
            except (ClientError, BotoCoreError):
                log.warning("Failed to delete S3 objects for retention",
                            extra={"bucket": self._bucket, "count": len(batch)}, exc_info=True)

    def _sort_key_for(self, list_entry: dict) -> float:
        """Epoch-seconds sort key. Prefers our metadata; falls back to LastModified.

        Returns a float so retention sorts numerically — string isoformats from
        metadata (``...Z``) and LastModified (``...+00:00``) would otherwise
        misorder lexicographically. Returns 0.0 when neither is parseable, so
        broken entries sort to the oldest position and get pruned first.
        """
        try:
            head = self._client.head_object(Bucket=self._bucket, Key=list_entry["Key"])
            meta = head.get("Metadata") or {}
            if created := meta.get(META_CREATED_AT):
                if dt := parse_timestamp(created):
                    return dt.timestamp()
        except (ClientError, BotoCoreError):
            log.debug("HEAD failed during retention sort, falling back to LastModified",
                      extra={"key": list_entry.get("Key")})
        last_modified = list_entry.get("LastModified")
        return last_modified.timestamp() if last_modified else 0.0


def create_store(env_id: str, config: OutputStorageConfig) -> S3OutputStore:
    """Write-side module factory — part of the output store module contract."""
    del env_id  # bucket+prefix are explicit; env_id not used for S3
    s3_cfg = parse_s3_config(config)
    client = build_client(s3_cfg)
    return S3OutputStore(client, s3_cfg.bucket, s3_cfg.prefix, compress=s3_cfg.compress)
