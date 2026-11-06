"""Unit tests for `run meteo`: fake data.gouv.fr + file server, in-memory S3 (moto)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from functools import partial
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from railfall.ingest import meteo, run
from railfall.ingest.http import Download, DownloadError

BUCKET = "railfall-lake"
DAY = date(2026, 11, 6)
NOW = datetime(2026, 11, 6, 9, 30, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures" / "meteo"
BASE = "https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/"
PARIS_LATEST = BASE + "Q_75_latest-2025-2026_RR-T-Vent.csv.gz"
PARIS_PREVIOUS = BASE + "Q_75_previous-1950-2024_RR-T-Vent.csv.gz"
PAYLOADS = {
    meteo.dataset_url(): (FIXTURES / "catalogue_sample.json").read_bytes(),
    PARIS_LATEST: (FIXTURES / "Q_75_latest-2025-2026_RR-T-Vent.csv.gz").read_bytes(),
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
        return Download(url=url, status=200, content=payloads[url], content_type="")

    return _fetch


def _ingest(s3, **kwargs):
    kwargs.setdefault("fetcher", fake_fetcher())
    kwargs.setdefault("departments", ["75"])
    return run.ingest_meteo(s3, BUCKET, ingest_date=DAY, now=NOW, min_rows=1, **kwargs)


def _keys(s3) -> list[str]:
    return sorted(o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", []))


def test_lands_latest_file_as_gzip_with_manifest(s3) -> None:
    (result,) = _ingest(s3)
    assert (result.dataset, result.ok, result.status, result.rows) == (
        "daily_75_latest",
        True,
        "written",
        30,
    )
    prefix = "bronze/meteo/daily_75_latest/ingest_date=2026-11-06/"
    assert _keys(s3) == [prefix + "_manifest.json", prefix + "daily_75_latest.csv.gz"]
    head = s3.head_object(Bucket=BUCKET, Key=result.key)
    assert head["ContentType"] == "application/gzip"
    body = s3.get_object(Bucket=BUCKET, Key=result.key)["Body"].read()
    assert body == PAYLOADS[PARIS_LATEST]  # byte-for-byte, still compressed
    manifest = json.loads(
        s3.get_object(Bucket=BUCKET, Key=prefix + "_manifest.json")["Body"].read()
    )
    assert manifest["source_url"] == PARIS_LATEST
    assert manifest["rows"] == 30


def test_rerun_same_day_is_a_noop(s3) -> None:
    _ingest(s3)
    (again,) = _ingest(s3)
    assert again.status == "skipped"
    assert len(_keys(s3)) == 2


def test_catalogue_is_fetched_once_for_all_departments(s3) -> None:
    calls: list[str] = []
    _ingest(s3, departments=["75", "13"], fetcher=fake_fetcher(calls=calls))
    assert calls.count(meteo.dataset_url()) == 1
    # 13 is in the catalogue but its file is not served: only one more call each.
    assert calls[1:] == [PARIS_LATEST, BASE + "Q_13_latest-2025-2026_RR-T-Vent.csv.gz"]


def test_department_missing_from_catalogue_does_not_block_others(s3) -> None:
    results = _ingest(s3, departments=["33", "75"])
    gironde, paris = results
    assert (gironde.dataset, gironde.ok) == ("daily_33", False)
    assert "33/latest" in gironde.error
    assert paris.ok


def test_failed_download_never_reaches_bronze(s3) -> None:
    results = _ingest(s3, departments=["13", "75"])
    assert [(r.dataset, r.ok) for r in results] == [
        ("daily_13_latest", False),
        ("daily_75_latest", True),
    ]
    assert "HTTP 404" in results[0].error
    assert not any("daily_13" in k for k in _keys(s3))


def test_file_from_the_wrong_department_is_rejected(s3) -> None:
    payloads = {**PAYLOADS, BASE + "Q_13_latest-2025-2026_RR-T-Vent.csv.gz": PAYLOADS[PARIS_LATEST]}
    (result,) = _ingest(s3, departments=["13"], fetcher=fake_fetcher(payloads))
    assert not result.ok
    assert "not in 13" in result.error
    assert _keys(s3) == []


def test_previous_period_when_asked(s3) -> None:
    calls: list[str] = []
    results = _ingest(s3, periods=["latest", "previous"], fetcher=fake_fetcher(calls=calls))
    assert [r.dataset for r in results] == ["daily_75_latest", "daily_75_previous"]
    assert calls[-1] == PARIS_PREVIOUS


def test_catalogue_outage_is_reported_not_raised(s3) -> None:
    (result,) = _ingest(s3, fetcher=fake_fetcher({}))
    assert (result.dataset, result.ok) == ("catalogue", False)
    assert "HTTP 404" in result.error


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"departments": ["99"]}, "not a French department"),
        ({"departments": []}, "no Météo-France department"),
        ({"periods": ["recent"]}, "unknown period"),
    ],
)
def test_bad_arguments_fail_before_any_download(s3, kwargs, match) -> None:
    calls: list[str] = []
    with pytest.raises(ValueError, match=match):
        _ingest(s3, fetcher=fake_fetcher(calls=calls), **kwargs)
    assert calls == []


def test_real_row_floor_applies_by_default(s3) -> None:
    (result,) = run.ingest_meteo(
        s3, BUCKET, fetcher=fake_fetcher(), departments=["75"], ingest_date=DAY, now=NOW
    )
    assert not result.ok
    assert "expected at least 1000" in result.error


def test_cli_uses_configured_departments(s3, monkeypatch, capsys) -> None:
    monkeypatch.setenv("METEO_DEPARTMENTS", "75")
    monkeypatch.setattr(run, "lake_client", lambda settings: s3)
    monkeypatch.setattr(run, "fetch", lambda url, **_: fake_fetcher()(url))
    monkeypatch.setattr(run, "ingest_meteo", partial(run.ingest_meteo, min_rows=1))

    code = run.main(["meteo", "--json", "--date", "2026-11-06"])
    out = json.loads(capsys.readouterr().out)
    assert code == 0
    assert [(r["dataset"], r["status"]) for r in out] == [("daily_75_latest", "written")]


def test_cli_department_flag_overrides_config(s3, monkeypatch, capsys) -> None:
    monkeypatch.setenv("METEO_DEPARTMENTS", "75")
    monkeypatch.setattr(run, "lake_client", lambda settings: s3)
    monkeypatch.setattr(run, "fetch", lambda url, **_: fake_fetcher()(url))

    code = run.main(["meteo", "--department", "2a"])
    out = capsys.readouterr().out
    assert code == 1  # 2A is listed but its file is not served by the fake
    assert out.startswith("[FAIL] daily_2A_latest")


def test_cli_rejects_bad_department_with_usage_error(s3, monkeypatch, capsys) -> None:
    monkeypatch.setattr(run, "lake_client", lambda settings: s3)
    with pytest.raises(SystemExit) as exc:
        run.main(["meteo", "--department", "20"])
    assert exc.value.code == 2
    assert "not a French department" in capsys.readouterr().err


def test_dataset_flag_is_sncf_only() -> None:
    with pytest.raises(SystemExit):
        run.main(["meteo", "--dataset", "stations"])
