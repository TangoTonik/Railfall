"""Reading Météo-France bronze back: same partition rules as SNCF, csv.gz contract."""

from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from railfall.ingest import meteo, verify
from railfall.ingest.bronze import land

BUCKET = "railfall-lake"
DAY = date(2026, 11, 6)
NOW = datetime(2026, 11, 6, 13, 0, tzinfo=UTC)
PARIS = Path(__file__).parent / "fixtures" / "meteo" / "Q_75_latest-2025-2026_RR-T-Vent.csv.gz"
PAYLOAD = PARIS.read_bytes()
PREFIX = f"bronze/meteo/daily_75_latest/ingest_date={DAY.isoformat()}"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def _land(s3, *, dataset: str = "daily_75_latest", payload: bytes = PAYLOAD) -> None:
    land(
        s3,
        BUCKET,
        source="meteo",
        dataset=dataset,
        payload=payload,
        rows=meteo.validate_file("75", PAYLOAD, min_rows=1).rows,
        source_url="https://example.test/Q_75.csv.gz",
        ingest_date=DAY,
        fmt="csv.gz",
        now=NOW,
    )


def test_landed_file_verifies(s3) -> None:
    _land(s3)

    (check,) = verify.verify_meteo(s3, BUCKET, DAY, departments=["75"], min_rows=1)

    assert check.ok, check.problems
    assert check.dataset == "daily_75_latest"
    assert check.key == f"{PREFIX}/daily_75_latest.csv.gz"
    assert check.rows == meteo.validate_file("75", PAYLOAD, min_rows=1).rows


def test_department_codes_are_normalised(s3) -> None:
    _land(s3)

    checks = verify.verify_meteo(s3, BUCKET, DAY, departments=[75, "75"], min_rows=1)

    assert [c.dataset for c in checks] == ["daily_75_latest"]


def test_missing_department_is_reported_per_department(s3) -> None:
    _land(s3)

    ok, missing = verify.verify_meteo(s3, BUCKET, DAY, departments=["75", "33"], min_rows=1)

    assert ok.ok
    assert not missing.ok
    assert missing.problems == [
        f"nothing under bronze/meteo/daily_33_latest/ingest_date={DAY.isoformat()}/"
    ]


def test_real_row_floor_applies_by_default(s3) -> None:
    _land(s3)

    (check,) = verify.verify_meteo(s3, BUCKET, DAY, departments=["75"])

    assert not check.ok
    assert f"expected at least {meteo.MIN_ROWS}" in check.problems[0]


def test_file_stored_under_the_wrong_department_fails(s3) -> None:
    # Paris stations (75xxxxxx) landed as Gironde: the contract catches it on read-back.
    _land(s3, dataset="daily_33_latest")

    (check,) = verify.verify_meteo(s3, BUCKET, DAY, departments=["33"], min_rows=1)

    assert not check.ok
    assert "not in 33" in check.problems[0]


def test_truncated_gzip_fails(s3) -> None:
    _land(s3)
    key = f"{PREFIX}/daily_75_latest.csv.gz"
    s3.put_object(Bucket=BUCKET, Key=key, Body=PAYLOAD[: len(PAYLOAD) // 2])

    (check,) = verify.verify_meteo(s3, BUCKET, DAY, departments=["75"], min_rows=1)

    joined = " | ".join(check.problems)
    assert "manifest sha256" in joined
    assert "not a complete gzip file" in joined


def test_rows_dropped_after_landing_fail_the_manifest(s3) -> None:
    _land(s3)
    lines = gzip.decompress(PAYLOAD).decode().splitlines(keepends=True)
    shorter = gzip.compress("".join(lines[:-1]).encode())
    key = f"{PREFIX}/daily_75_latest.csv.gz"
    s3.put_object(Bucket=BUCKET, Key=key, Body=shorter)

    (check,) = verify.verify_meteo(s3, BUCKET, DAY, departments=["75"], min_rows=1)

    rows = len(lines) - 1
    assert f"manifest rows {rows} != payload rows {rows - 1}" in check.problems


def test_second_period_is_its_own_partition(s3) -> None:
    _land(s3)

    checks = verify.verify_meteo(
        s3, BUCKET, DAY, departments=["75"], periods=["latest", "previous"], min_rows=1
    )

    assert [(c.dataset, c.ok) for c in checks] == [
        ("daily_75_latest", True),
        ("daily_75_previous", False),
    ]


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"departments": []}, "no Météo-France department"),
        ({"departments": ["99"]}, "department"),
        ({"departments": ["75"], "periods": ["2024"]}, "unknown period"),
    ],
)
def test_bad_arguments_raise(s3, kwargs, match) -> None:
    with pytest.raises(ValueError, match=match):
        verify.verify_meteo(s3, BUCKET, DAY, **kwargs)


def test_meteo_has_no_fixed_dataset_list(s3) -> None:
    with pytest.raises(ValueError, match="use verify_meteo"):
        verify.verify_source(s3, BUCKET, "meteo", DAY)


def test_list_source_covers_meteo(s3) -> None:
    _land(s3)

    assert verify.list_source(s3, BUCKET, "meteo") == [
        f"{PREFIX}/_manifest.json",
        f"{PREFIX}/daily_75_latest.csv.gz",
    ]


def _cli(s3, monkeypatch, *argv: str) -> int:
    monkeypatch.setattr("railfall.ingest.run.lake_client", lambda settings: s3)
    monkeypatch.setenv("LAKE_BUCKET", BUCKET)
    return verify.main(["meteo", "--date", DAY.isoformat(), *argv])


def test_cli_uses_configured_departments(s3, monkeypatch, capsys) -> None:
    _land(s3)
    monkeypatch.setenv("METEO_DEPARTMENTS", "75,33")

    code = _cli(s3, monkeypatch, "--json")

    out = json.loads(capsys.readouterr().out)
    assert code == 1  # 33 was never landed; 75 fails the real row floor
    assert [c["dataset"] for c in out] == ["daily_75_latest", "daily_33_latest"]


def test_cli_rejects_dataset_flag_for_meteo(s3, monkeypatch) -> None:
    with pytest.raises(SystemExit) as exc:
        _cli(s3, monkeypatch, "--dataset", "stations")
    assert exc.value.code == 2
