"""Tests for S3 output store + writer."""
import gzip
import json
from datetime import datetime, timedelta, timezone

import boto3
import pytest
from moto import mock_aws

from runtools.runcore.job import iid
from runtools.runcore.output import OutputLine, OutputStorageConfig
from runtools.runcore.retention import RetentionPolicy
from runtools.runcore.util.dt import utc_now
from runtools.runjob.output.s3 import (
    S3OutputStore,
    S3OutputWriter,
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
    writer = S3OutputWriter(s3_client, BUCKET, "prod", iid("j", "r"),
                            created_at=utc_now(), compress=True)
    assert writer.location.uri == f"s3://{BUCKET}/prod/j/r__1.jsonl.gz"
    assert writer.location.uri.endswith(".jsonl.gz")


def test_writer_location_uri_plain_suffix(s3_client):
    writer = S3OutputWriter(s3_client, BUCKET, "prod", iid("j", "r"),
                            created_at=utc_now(), compress=False)
    assert writer.location.uri == f"s3://{BUCKET}/prod/j/r__1.jsonl"
    assert writer.location.uri.endswith(".jsonl")
    assert not writer.location.uri.endswith(".jsonl.gz")


def test_create_writer_uses_compress_setting(store):
    writer = store.create_writer(iid("myjob", "run1"), created_at=utc_now())
    assert writer.location.uri.endswith(".jsonl.gz")  # store created with compress=True


def test_create_writer_plain_when_compress_disabled(s3_client):
    store = S3OutputStore(s3_client, BUCKET, prefix="prod", compress=False)
    writer = store.create_writer(iid("myjob", "run1"), created_at=utc_now())
    assert writer.location.uri.endswith(".jsonl")
    assert not writer.location.uri.endswith(".jsonl.gz")


def test_close_puts_object_with_canonical_created_at_metadata(s3_client, store):
    canonical = datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc)
    writer = store.create_writer(iid("myjob", "run1"), created_at=canonical)
    writer.store_line(OutputLine("hello", 1, source="EXEC"))
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
    writer = store.create_writer(iid("myjob", "run1"), created_at=utc_now())
    writer.store_line(OutputLine("a", 1, source="EXEC"))
    writer.store_line(OutputLine("b", 2, source="EXEC"))
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
    writer = store.create_writer(iid("myjob", "run1"), created_at=utc_now())
    writer.store_line(OutputLine("hello", 1, source="EXEC"))

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

    writer = store.create_writer(iid("myjob", "run1"), created_at=utc_now())
    writer.store_line(OutputLine("hello", 1))

    err = EndpointConnectionError(endpoint_url="https://s3.example/")
    with patch.object(s3_client, "put_object", side_effect=err):
        with caplog.at_level(logging.WARNING, logger="runtools.runjob.output.s3"):
            with pytest.raises(EndpointConnectionError):
                writer.close()

    assert writer._closed is False  # buffer retained
    assert any("Failed to upload output to S3" in r.message for r in caplog.records)


def test_close_is_idempotent(s3_client, store):
    writer = store.create_writer(iid("myjob", "run1"), created_at=utc_now())
    writer.store_line(OutputLine("once", 1))
    writer.close()
    writer.close()  # Should not raise or duplicate

    # Verify object exists and has one entry
    obj = s3_client.get_object(Bucket=BUCKET, Key="prod/myjob/run1__1.jsonl.gz")
    raw = gzip.decompress(obj["Body"].read())
    lines = [ln for ln in raw.decode("utf-8").splitlines() if ln.strip()]
    assert len(lines) == 1


def test_roundtrip_via_backend(s3_client, store):
    writer = store.create_writer(iid("myjob", "run1"), created_at=utc_now())
    writer.store_line(OutputLine("hello", 1, source="EXEC", level="INFO",
                                 fields={"foo": "bar"}))
    writer.store_line(OutputLine("world", 2, source="EXEC"))
    writer.close()

    # Use the store itself as the backend (it inherits from S3OutputBackend)
    result = store.read_output(iid("myjob", "run1"))
    assert [ol.message for ol in result] == ["hello", "world"]
    assert result[0].fields == {"foo": "bar"}
    assert result[0].level == "INFO"


def test_retention_sorts_by_metadata_not_lastmodified(s3_client, store):
    """LastModified order ≠ created_at order. Retention must use the metadata."""
    # Write three runs in REVERSE chronological order (i.e. oldest run PUT last).
    # If retention sorted by LastModified, it would keep the wrong objects.
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    runs = [
        ("run3", base + timedelta(days=2)),  # newest created_at, but PUT first
        ("run2", base + timedelta(days=1)),
        ("run1", base),                       # oldest created_at, but PUT last
    ]
    for run_id, created_at in runs:
        writer = store.create_writer(iid("myjob", run_id), created_at=created_at)
        writer.store_line(OutputLine(run_id, 1))
        writer.close()

    # Keep only the 2 newest by created_at: run3 and run2
    store.enforce_retention("myjob", RetentionPolicy(max_runs_per_job=2, max_runs_per_env=-1))

    remaining_keys = {
        o["Key"] for o in s3_client.list_objects_v2(
            Bucket=BUCKET, Prefix="prod/myjob/").get("Contents", [])
    }
    assert remaining_keys == {
        "prod/myjob/run3__1.jsonl.gz",
        "prod/myjob/run2__1.jsonl.gz",
    }


def test_retention_sort_key_returns_numeric_epoch(s3_client, store):
    """_sort_key_for must return a float (epoch seconds), not a formatted string.

    String isoformats from metadata (...Z) and LastModified (...+00:00) would
    misorder lexicographically. This test pins the contract that the sort key
    is numeric so chronological order is preserved across formats.
    """
    # Tagged object: metadata uses ...Z format
    canonical = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    writer = store.create_writer(iid("myjob", "tagged"), created_at=canonical)
    writer.store_line(OutputLine("x", 1))
    writer.close()

    # Fallback object: no metadata, sort falls back to LastModified (...+00:00)
    s3_client.put_object(
        Bucket=BUCKET, Key="prod/myjob/untagged__1.jsonl.gz",
        Body=gzip.compress(b'{"message":"y","_rt":{"n":1}}\n'),
        ContentType="application/x-ndjson", ContentEncoding="gzip",
    )

    # Both sort keys must be floats — proves the lex-sort bug is gone
    listing = s3_client.list_objects_v2(Bucket=BUCKET, Prefix="prod/myjob/").get("Contents", [])
    sort_keys = [store._sort_key_for(obj) for obj in listing]
    assert all(isinstance(k, float) for k in sort_keys)
    assert all(k > 0 for k in sort_keys)
    # Tagged should equal canonical.timestamp() exactly
    tagged_entry = next(o for o in listing if "tagged" in o["Key"] and "untagged" not in o["Key"])
    assert store._sort_key_for(tagged_entry) == canonical.timestamp()


def test_retention_falls_back_to_lastmodified_without_metadata(s3_client, store):
    """Objects without runtools-created-at metadata fall back to LastModified order."""
    # Write 3 raw objects with NO metadata. moto sets LastModified to PUT time —
    # we PUT them in chronological order so LastModified is also chronological.
    for i in range(3):
        s3_client.put_object(
            Bucket=BUCKET,
            Key=f"prod/myjob/run{i}__1.jsonl.gz",
            Body=gzip.compress(b'{"message":"x","_rt":{"n":1}}\n'),
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
        )

    store.enforce_retention("myjob", RetentionPolicy(max_runs_per_job=1, max_runs_per_env=-1))

    remaining_keys = {
        o["Key"] for o in s3_client.list_objects_v2(
            Bucket=BUCKET, Prefix="prod/myjob/").get("Contents", [])
    }
    # The last-PUT object (run2) has the newest LastModified
    assert remaining_keys == {"prod/myjob/run2__1.jsonl.gz"}


def test_retention_noop_under_limit(s3_client, store):
    writer = store.create_writer(iid("myjob", "run1"), created_at=utc_now())
    writer.store_line(OutputLine("x", 1))
    writer.close()

    store.enforce_retention("myjob", RetentionPolicy(max_runs_per_job=10, max_runs_per_env=-1))

    keys = [o["Key"] for o in s3_client.list_objects_v2(
        Bucket=BUCKET, Prefix="prod/myjob/").get("Contents", [])]
    assert len(keys) == 1


def test_retention_with_empty_prefix(s3_client):
    """Retention should work when prefix is empty (objects at bucket root)."""
    store = S3OutputStore(s3_client, BUCKET, prefix="", compress=True)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i, run_id in enumerate(["a", "b", "c"]):
        writer = store.create_writer(iid("rootjob", run_id),
                                     created_at=base + timedelta(days=i))
        writer.store_line(OutputLine("x", 1))
        writer.close()

    store.enforce_retention("rootjob", RetentionPolicy(max_runs_per_job=1, max_runs_per_env=-1))

    remaining = {o["Key"] for o in s3_client.list_objects_v2(
        Bucket=BUCKET, Prefix="rootjob/").get("Contents", [])}
    assert remaining == {"rootjob/c__1.jsonl.gz"}


def test_retention_swallows_botocore_transport_errors(s3_client, store):
    """enforce_retention is best-effort. A transport error (BotoCoreError) during
    list/delete must be logged and swallowed — same as ClientError — so background
    maintenance never crashes the calling scheduler.
    """
    writer = store.create_writer(iid("myjob", "run1"), created_at=utc_now())
    writer.store_line(OutputLine("x", 1))
    writer.close()

    from unittest.mock import patch
    from botocore.exceptions import EndpointConnectionError

    err = EndpointConnectionError(endpoint_url="https://s3.example/")
    with patch.object(s3_client, "get_paginator") as gp:
        gp.return_value.paginate.side_effect = err
        # Must not raise
        store.enforce_retention("myjob",
                                RetentionPolicy(max_runs_per_job=1, max_runs_per_env=-1))


def test_create_store_factory(s3_client):
    cfg = OutputStorageConfig(type="s3", bucket=BUCKET, prefix="prod", region="us-east-1")
    store = create_store("env1", cfg)
    assert store._bucket == BUCKET
    assert store._prefix == "prod"
    assert store._compress is True


def test_canonical_created_at_propagates_to_metadata(s3_client, store):
    """The exact value passed via create_writer() ends up in S3 metadata.

    This locks in the contract that S3 metadata reflects the caller-provided
    ``created_at`` (typically root_phase.created_at), not a writer-internal
    clock — keeping S3 retention metadata consistent with DB.runs.created and
    JobRun.lifecycle.created_at, which all derive from the same source.
    """
    # An odd, non-now timestamp guarantees we're not just matching utc_now()
    canonical = datetime(2024, 7, 15, 9, 30, 45, 123000, tzinfo=timezone.utc)
    writer = store.create_writer(iid("myjob", "run1"), created_at=canonical)
    writer.store_line(OutputLine("x", 1))
    writer.close()

    head = s3_client.head_object(Bucket=BUCKET, Key="prod/myjob/run1__1.jsonl.gz")
    meta = {k.lower(): v for k, v in head["Metadata"].items()}
    # format_timestamp output is the canonical wire format
    assert meta[META_CREATED_AT] == "2024-07-15T09:30:45.123Z"
