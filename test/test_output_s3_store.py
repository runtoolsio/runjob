"""Tests for S3 output store + writer."""
import gzip
import json
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from runtools.runcore.job import iid
from runtools.runcore.output import OutputLine, OutputStorageConfig
from runtools.runcore.util.dt import utc_now
from runtools.runjob.output.s3 import (
    S3OutputStore,
    S3OutputSink,
    create_store,
    META_CREATED_AT,
    META_ORDINAL,
    META_RUN_ID,
)

BUCKET = "test-runtools-output"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def store(s3_client):
    return S3OutputStore(s3_client, BUCKET, prefix="prod", compress=True)


def test_writer_location_uri_compressed_suffix(s3_client):
    writer = S3OutputSink(s3_client, BUCKET, "prod", iid("j", "r"),
                            created_at=utc_now(), compress=True)
    assert writer.location.uri == f"s3://{BUCKET}/prod/j/r__1.jsonl.gz"
    assert writer.location.uri.endswith(".jsonl.gz")


def test_writer_location_uri_plain_suffix(s3_client):
    writer = S3OutputSink(s3_client, BUCKET, "prod", iid("j", "r"),
                            created_at=utc_now(), compress=False)
    assert writer.location.uri == f"s3://{BUCKET}/prod/j/r__1.jsonl"
    assert writer.location.uri.endswith(".jsonl")
    assert not writer.location.uri.endswith(".jsonl.gz")


def test_create_sink_uses_compress_setting(store):
    writer = store.create_sink(iid("myjob", "run1"), created_at=utc_now())
    assert writer.location.uri.endswith(".jsonl.gz")  # store created with compress=True


def test_create_sink_plain_when_compress_disabled(s3_client):
    store = S3OutputStore(s3_client, BUCKET, prefix="prod", compress=False)
    writer = store.create_sink(iid("myjob", "run1"), created_at=utc_now())
    assert writer.location.uri.endswith(".jsonl")
    assert not writer.location.uri.endswith(".jsonl.gz")


def test_close_puts_object_with_canonical_created_at_metadata(s3_client, store):
    canonical = datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc)
    writer = store.create_sink(iid("myjob", "run1"), created_at=canonical)
    writer.write_line(OutputLine("hello", 1, source="EXEC"))
    writer.close()

    head = s3_client.head_object(Bucket=BUCKET, Key="prod/myjob/run1__1.jsonl.gz")
    # Boto3 normalizes user-defined metadata keys to lowercase
    meta = {k.lower(): v for k, v in head["Metadata"].items()}
    assert meta[META_RUN_ID] == "run1"
    assert meta[META_ORDINAL] == "1"
    assert meta[META_CREATED_AT] == "2026-04-28T12:00:00.000Z"
    assert head.get("ContentEncoding") == "gzip"
    assert head.get("ContentType") == "application/x-ndjson"


def test_close_writes_decodable_jsonl(s3_client, store):
    writer = store.create_sink(iid("myjob", "run1"), created_at=utc_now())
    writer.write_line(OutputLine("a", 1, source="EXEC"))
    writer.write_line(OutputLine("b", 2, source="EXEC"))
    writer.close()

    obj = s3_client.get_object(Bucket=BUCKET, Key="prod/myjob/run1__1.jsonl.gz")
    raw = gzip.decompress(obj["Body"].read())
    lines = [json.loads(ln) for ln in raw.decode("utf-8").splitlines() if ln.strip()]
    assert [ln["message"] for ln in lines] == ["a", "b"]


def test_close_does_not_mark_closed_when_upload_fails(s3_client, store):
    """If put_object fails transiently, the writer must remain re-closeable.

    Marking _closed=True before a successful PUT would strand the buffered
    output — a retry of close() would silently no-op.
    """
    writer = store.create_sink(iid("myjob", "run1"), created_at=utc_now())
    writer.write_line(OutputLine("hello", 1, source="EXEC"))

    from unittest.mock import patch
    from botocore.exceptions import ClientError

    err = ClientError({"Error": {"Code": "InternalError", "Message": "transient"}}, "PutObject")
    with patch.object(s3_client, "put_object", side_effect=err):
        with pytest.raises(ClientError):
            writer.close()
    assert writer._closed is False  # buffer still owned, retry allowed

    # Retry against the real client succeeds and writes the buffer
    writer.close()
    assert writer._closed is True
    obj = s3_client.get_object(Bucket=BUCKET, Key="prod/myjob/run1__1.jsonl.gz")
    raw = gzip.decompress(obj["Body"].read())
    assert b"hello" in raw


def test_close_logs_and_reraises_botocore_transport_errors(s3_client, store, caplog):
    """Transport errors (BotoCoreError subclasses) must surface the same as ClientError:
    one writer-emitted log line, then re-raise, with buffer retained for retry.
    """
    import logging
    from unittest.mock import patch
    from botocore.exceptions import EndpointConnectionError

    writer = store.create_sink(iid("myjob", "run1"), created_at=utc_now())
    writer.write_line(OutputLine("hello", 1))

    err = EndpointConnectionError(endpoint_url="https://s3.example/")
    with patch.object(s3_client, "put_object", side_effect=err):
        with caplog.at_level(logging.WARNING, logger="runtools.runjob.output.s3"):
            with pytest.raises(EndpointConnectionError):
                writer.close()

    assert writer._closed is False  # buffer retained
    assert any("Failed to upload output to S3" in r.message for r in caplog.records)


def test_close_is_idempotent(s3_client, store):
    writer = store.create_sink(iid("myjob", "run1"), created_at=utc_now())
    writer.write_line(OutputLine("once", 1))
    writer.close()
    writer.close()  # Should not raise or duplicate

    # Verify object exists and has one entry
    obj = s3_client.get_object(Bucket=BUCKET, Key="prod/myjob/run1__1.jsonl.gz")
    raw = gzip.decompress(obj["Body"].read())
    lines = [ln for ln in raw.decode("utf-8").splitlines() if ln.strip()]
    assert len(lines) == 1


def test_roundtrip_via_backend(s3_client, store):
    writer = store.create_sink(iid("myjob", "run1"), created_at=utc_now())
    writer.write_line(OutputLine("hello", 1, source="EXEC", level="INFO",
                                 fields={"foo": "bar"}))
    writer.write_line(OutputLine("world", 2, source="EXEC"))
    writer.close()

    # Use the store itself as the backend (it inherits from S3OutputBackend)
    result = store.read_output(iid("myjob", "run1"))
    assert [ol.message for ol in result] == ["hello", "world"]
    assert result[0].fields == {"foo": "bar"}
    assert result[0].level == "INFO"


def test_create_store_factory(s3_client):
    cfg = OutputStorageConfig(type="s3", bucket=BUCKET, prefix="prod", region="us-east-1")
    store = create_store("env1", cfg)
    assert store._bucket == BUCKET
    assert store._prefix == "prod"
    assert store._compress is True


def test_canonical_created_at_propagates_to_metadata(s3_client, store):
    """The exact value passed via create_sink() ends up in S3 metadata.

    This locks in the contract that S3 metadata reflects the caller-provided
    ``created_at`` (typically root_phase.created_at), not a writer-internal
    clock — keeping S3 retention metadata consistent with DB.runs.created and
    JobRun.lifecycle.created_at, which all derive from the same source.
    """
    # An odd, non-now timestamp guarantees we're not just matching utc_now()
    canonical = datetime(2024, 7, 15, 9, 30, 45, 123000, tzinfo=timezone.utc)
    writer = store.create_sink(iid("myjob", "run1"), created_at=canonical)
    writer.write_line(OutputLine("x", 1))
    writer.close()

    head = s3_client.head_object(Bucket=BUCKET, Key="prod/myjob/run1__1.jsonl.gz")
    meta = {k.lower(): v for k, v in head["Metadata"].items()}
    # format_timestamp output is the canonical wire format
    assert meta[META_CREATED_AT] == "2024-07-15T09:30:45.123Z"
