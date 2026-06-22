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
from typing import List

from runtools.runcore.job import InstanceID
from runtools.runcore.output import OutputLine, OutputLocation, OutputStorageConfig
from runtools.runcore.output.s3 import (
    S3OutputBackend,
    build_client,
    object_key,
    parse_s3_config,
)
from runtools.runjob.output import OutputStore, OutputSink

try:
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError as e:
    raise ImportError(
        "S3 output store requires boto3. Install with: pip install runtoolsio-runjob[s3]"
    ) from e

log = logging.getLogger(__name__)


class S3OutputSink(OutputSink):
    """Buffers OutputLines in memory, flushes to S3 as a single object on close."""

    def __init__(self, client, bucket: str, prefix: str, instance_id: InstanceID, *,
                 compress: bool = True):
        """Build a writer for one run's output.

        Args:
            client: boto3 S3 client.
            bucket: Target bucket name.
            prefix: Key prefix under the bucket (may be empty for bucket root).
            instance_id: Fully-qualified InstanceID — drives the object key via
                :func:`object_key`.
            compress: Gzip the payload on close. Affects ``Content-Encoding``
                and the chosen ``.jsonl`` vs ``.jsonl.gz`` suffix.
        """
        self._client = client
        self._bucket = bucket
        self._key = object_key(prefix, instance_id, compress=compress)
        self._compress = compress
        self._buffer: List[OutputLine] = []
        self._closed = False

    @property
    def location(self):
        return OutputLocation.for_s3(self._bucket, self._key)

    def write_line(self, line: OutputLine):
        self._buffer.append(line)

    def write_lines(self, lines: List[OutputLine]):
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
        }
        if self._compress:
            kwargs["ContentEncoding"] = "gzip"
        self._client.put_object(**kwargs)


class S3OutputStore(S3OutputBackend, OutputStore):
    """S3 output store. Inherits read from S3OutputBackend, adds write."""

    def __init__(self, client, bucket: str, prefix: str = "", *, compress: bool = True):
        super().__init__(client, bucket, prefix)
        self._compress = compress

    def create_sink(self, instance_id: InstanceID) -> S3OutputSink:
        return S3OutputSink(
            self._client, self._bucket, self._prefix, instance_id, compress=self._compress,
        )


def create_store(env_id: str, config: OutputStorageConfig) -> S3OutputStore:
    """Write-side module factory — part of the output store module contract."""
    del env_id  # bucket+prefix are explicit; env_id not used for S3
    s3_cfg = parse_s3_config(config)
    client = build_client(s3_cfg)
    return S3OutputStore(client, s3_cfg.bucket, s3_cfg.prefix, compress=s3_cfg.compress)
