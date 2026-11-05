"""Unit tests for reading bronze back and checking it, against an in-memory S3 (moto)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from railfall.ingest.bronze import land
from railfall.ingest.sncf import PUNCTUALITY, STATIONS, validate_export
from railfall.ingest.verify import list_source, verify_dataset, verify_source

BUCKET = "railfall-lake"
DAY = date(2026, 11, 5)
NOW = datetime(2026, 11, 5, 10, 30, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures" / "sncf"
PAYLOADS = {
    "stations": (STATIONS, (FIXTURES / "stations_sample.json").read_bytes()),
    "punctuality": (PUNCTUALITY, (FIXTURES / "punctuality_sample.json").read_bytes()),
}


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def _land_all(s3, day: date = DAY) -> None:
    for name, (dataset, payload) in PAYLOADS.items():
        rows = validate_export(dataset, payload, min_rows=1).rows
        land(
            s3,
            BUCKET,
            source="sncf",
            dataset=name,
            payload=payload,
            rows=rows,
            source_url="https://example.test",
            ingest_date=day,
            now=NOW,
        )


def _manifest_key(name: str) -> str:
    return f"bronze/sncf/{name}/ingest_date={DAY.isoformat()}/_manifest.json"


def test_clean_landing_verifies(s3) -> None:
    _land_all(s3)

    checks = verify_source(s3, BUCKET, "sncf", DAY, min_rows=1)

    assert [c.dataset for c in checks] == ["punctuality", "stations"]
    assert all(c.ok for c in checks), [c.problems for c in checks]
    expected = {n: len(json.loads(p)) for n, (_, p) in PAYLOADS.items()}
    assert {c.dataset: c.rows for c in checks} == expected


def test_real_row_floor_applies_by_default(s3) -> None:
    # Fixtures are tiny: with the production floor they must be rejected.
    _land_all(s3)

    check = verify_dataset(s3, BUCKET, "sncf", STATIONS, DAY)

    assert not check.ok
    assert "expected at least 2000" in check.problems[0]


def test_missing_partition_fails(s3) -> None:
    _land_all(s3)

    check = verify_dataset(s3, BUCKET, "sncf", STATIONS, date(2026, 11, 6), min_rows=1)

    assert not check.ok
    assert check.problems == ["nothing under bronze/sncf/stations/ingest_date=2026-11-06/"]


def test_stray_object_in_partition_fails(s3) -> None:
    _land_all(s3)
    stray = f"bronze/sncf/stations/ingest_date={DAY.isoformat()}/stations (1).json"
    s3.put_object(Bucket=BUCKET, Key=stray, Body=b"[]")

    check = verify_dataset(s3, BUCKET, "sncf", STATIONS, DAY, min_rows=1)

    assert not check.ok
    assert "unexpected objects" in check.problems[0]


def test_payload_tampered_after_landing_fails(s3) -> None:
    _land_all(s3)
    key = f"bronze/sncf/stations/ingest_date={DAY.isoformat()}/stations.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=b'[{"nom": "x"}]')

    check = verify_dataset(s3, BUCKET, "sncf", STATIONS, DAY, min_rows=1)

    assert not check.ok
    joined = " | ".join(check.problems)
    assert "manifest sha256" in joined
    assert "object metadata sha256" in joined
    assert "missing 'codes_uic'" in joined


def test_manifest_row_count_mismatch_fails(s3) -> None:
    _land_all(s3)
    key = _manifest_key("punctuality")
    manifest = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    manifest["rows"] += 1
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(manifest).encode())

    check = verify_dataset(s3, BUCKET, "sncf", PUNCTUALITY, DAY, min_rows=1)

    assert not check.ok
    assert check.problems == [
        f"manifest rows {manifest['rows']} != payload rows {manifest['rows'] - 1}"
    ]


def test_unknown_source_or_dataset_is_rejected(s3) -> None:
    with pytest.raises(ValueError, match="unknown source"):
        verify_source(s3, BUCKET, "meteo", DAY)
    with pytest.raises(ValueError, match="unknown sncf dataset"):
        verify_source(s3, BUCKET, "sncf", DAY, names=["trains"])


def test_list_source_spans_every_ingest_day(s3) -> None:
    _land_all(s3)
    _land_all(s3, date(2026, 11, 6))
    s3.put_object(Bucket=BUCKET, Key="bronze/sncfx/other.json", Body=b"[]")

    keys = list_source(s3, BUCKET, "sncf")

    assert len(keys) == 8
    assert all(k.startswith("bronze/sncf/") for k in keys)
