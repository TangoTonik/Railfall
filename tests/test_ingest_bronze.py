"""Unit tests for the bronze landing step, against an in-memory S3 (moto)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from railfall.ingest.bronze import bronze_prefix, land, read_manifest

BUCKET = "railfall-lake"
DAY = date(2026, 11, 5)
NOW = datetime(2026, 11, 5, 10, 30, tzinfo=UTC)
PAYLOAD = (Path(__file__).parent / "fixtures" / "sncf" / "stations_sample.json").read_bytes()
URL = "https://example.test/exports/json"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def _land(s3, payload: bytes = PAYLOAD, **overrides):
    kwargs = dict(
        source="sncf",
        dataset="stations",
        payload=payload,
        rows=5,
        source_url=URL,
        ingest_date=DAY,
        now=NOW,
    )
    kwargs.update(overrides)
    return land(s3, BUCKET, **kwargs)


def _keys(s3) -> list[str]:
    return sorted(o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", []))


def test_prefix_is_hive_partitioned_by_ingest_date() -> None:
    assert bronze_prefix("sncf", "stations", DAY) == ("bronze/sncf/stations/ingest_date=2026-11-05")


@pytest.mark.parametrize("bad", ["", "a/b", ".hidden"])
def test_prefix_rejects_names_that_would_escape_the_layout(bad: str) -> None:
    with pytest.raises(ValueError, match="invalid"):
        bronze_prefix(bad, "stations", DAY)


def test_first_landing_writes_payload_and_manifest(s3) -> None:
    landing = _land(s3)

    assert landing.status == "written"
    assert _keys(s3) == [
        "bronze/sncf/stations/ingest_date=2026-11-05/_manifest.json",
        "bronze/sncf/stations/ingest_date=2026-11-05/stations.json",
    ]
    stored = s3.get_object(Bucket=BUCKET, Key=landing.key)
    assert stored["Body"].read() == PAYLOAD
    assert stored["ContentType"] == "application/json"
    assert stored["Metadata"]["sha256"] == landing.sha256

    manifest = read_manifest(s3, BUCKET, landing.manifest_key)
    assert manifest["rows"] == 5
    assert manifest["size_bytes"] == len(PAYLOAD)
    assert manifest["source_url"] == URL
    assert manifest["landed_at"] == "2026-11-05T10:30:00+00:00"
    assert "status" not in manifest


def test_same_bytes_same_day_is_a_no_op(s3) -> None:
    _land(s3)
    later = datetime(2026, 11, 5, 18, 0, tzinfo=UTC)
    again = _land(s3, now=later)

    assert again.status == "skipped"
    assert len(_keys(s3)) == 2
    # The manifest still describes the first landing.
    assert read_manifest(s3, BUCKET, again.manifest_key)["landed_at"].startswith("2026-11-05T10")


def test_republished_source_replaces_the_day_instead_of_duplicating(s3) -> None:
    first = _land(s3)
    second = _land(s3, payload=PAYLOAD + b"\n", rows=5)

    assert second.status == "replaced"
    assert second.sha256 != first.sha256
    assert len(_keys(s3)) == 2
    assert read_manifest(s3, BUCKET, second.manifest_key)["sha256"] == second.sha256


def test_a_new_day_gets_its_own_partition(s3) -> None:
    _land(s3)
    _land(s3, ingest_date=date(2026, 11, 6))

    assert len(_keys(s3)) == 4
