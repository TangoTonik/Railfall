"""Unit tests for the ingestion runner: fake HTTP, in-memory S3 (moto)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from functools import partial
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from railfall.ingest import run
from railfall.ingest.http import Download, DownloadError
from railfall.ingest.sncf import PUNCTUALITY, STATIONS

BUCKET = "railfall-lake"
DAY = date(2026, 11, 5)
NOW = datetime(2026, 11, 5, 10, 30, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures" / "sncf"
PAYLOADS = {
    PUNCTUALITY.export_url(): (FIXTURES / "punctuality_sample.json").read_bytes(),
    STATIONS.export_url(): (FIXTURES / "stations_sample.json").read_bytes(),
}


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def fake_fetcher(payloads=PAYLOADS, calls=None):
    def _fetch(url: str) -> Download:
        if calls is not None:
            calls.append(url)
        if url not in payloads:
            raise DownloadError(f"GET {url} -> HTTP 404 Not Found")
        return Download(url=url, status=200, content=payloads[url], content_type="json")

    return _fetch


def _ingest(s3, **kwargs):
    kwargs.setdefault("fetcher", fake_fetcher())
    return run.ingest_sncf(s3, BUCKET, ingest_date=DAY, now=NOW, min_rows=1, **kwargs)


def _keys(s3) -> list[str]:
    return sorted(o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", []))


def test_lands_every_dataset_with_its_manifest(s3) -> None:
    results = _ingest(s3)
    assert [(r.dataset, r.ok, r.status) for r in results] == [
        ("punctuality", True, "written"),
        ("stations", True, "written"),
    ]
    assert _keys(s3) == [
        "bronze/sncf/punctuality/ingest_date=2026-11-05/_manifest.json",
        "bronze/sncf/punctuality/ingest_date=2026-11-05/punctuality.json",
        "bronze/sncf/stations/ingest_date=2026-11-05/_manifest.json",
        "bronze/sncf/stations/ingest_date=2026-11-05/stations.json",
    ]
    expected_rows = len(json.loads(PAYLOADS[STATIONS.export_url()]))
    assert results[1].rows == expected_rows


def test_rerun_same_day_is_a_noop(s3) -> None:
    _ingest(s3)
    again = _ingest(s3)
    assert {r.status for r in again} == {"skipped"}
    assert len(_keys(s3)) == 4


def test_only_selected_datasets_are_fetched(s3) -> None:
    calls: list[str] = []
    results = _ingest(s3, names=["stations"], fetcher=fake_fetcher(calls=calls))
    assert [r.dataset for r in results] == ["stations"]
    assert calls == [STATIONS.export_url()]


def test_unknown_dataset_is_rejected_before_any_download(s3) -> None:
    calls: list[str] = []
    with pytest.raises(ValueError, match="unknown SNCF dataset"):
        _ingest(s3, names=["stations", "trains"], fetcher=fake_fetcher(calls=calls))
    assert calls == []


def test_failed_download_does_not_block_other_datasets(s3) -> None:
    payloads = {STATIONS.export_url(): PAYLOADS[STATIONS.export_url()]}
    results = _ingest(s3, fetcher=fake_fetcher(payloads))
    punctuality, stations = results
    assert not punctuality.ok and "HTTP 404" in punctuality.error
    assert stations.ok
    assert not any("punctuality" in k for k in _keys(s3))


def test_invalid_export_never_reaches_bronze(s3) -> None:
    error_page = b"<html><body>Maintenance en cours</body></html>"
    payloads = {**PAYLOADS, PUNCTUALITY.export_url(): error_page}
    results = _ingest(s3, fetcher=fake_fetcher(payloads))
    assert results[0].status == "failed"
    assert "not valid JSON" in results[0].error
    assert not any("punctuality" in k for k in _keys(s3))


def test_real_row_floor_applies_by_default(s3) -> None:
    """Without the test override, a 5-row fixture is far below a real export."""
    results = run.ingest_sncf(
        s3, BUCKET, fetcher=fake_fetcher(), names=["stations"], ingest_date=DAY, now=NOW
    )
    assert not results[0].ok
    assert "expected at least 2000" in results[0].error


def test_cli_prints_summary_and_exit_code(s3, monkeypatch, capsys) -> None:
    monkeypatch.setattr(run, "lake_client", lambda settings: s3)
    monkeypatch.setattr(run, "fetch", lambda url, **_: fake_fetcher()(url))
    # The fixtures are tiny: relax the real-export row floor for this run only.
    monkeypatch.setattr(run, "ingest_sncf", partial(run.ingest_sncf, min_rows=1))

    code = run.main(["sncf", "--json", "--date", "2026-11-05"])
    out = json.loads(capsys.readouterr().out)
    assert code == 0
    assert [(r["dataset"], r["status"]) for r in out] == [
        ("punctuality", "written"),
        ("stations", "written"),
    ]
    assert all(r["key"].startswith("bronze/sncf/") for r in out)


def test_cli_exits_non_zero_when_a_dataset_fails(s3, monkeypatch, capsys) -> None:
    monkeypatch.setattr(run, "lake_client", lambda settings: s3)
    monkeypatch.setattr(run, "fetch", lambda url, **_: fake_fetcher({})(url))

    code = run.main(["sncf", "--dataset", "stations"])
    out = capsys.readouterr().out
    assert code == 1
    assert out.startswith("[FAIL] stations")
