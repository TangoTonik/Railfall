"""Unit tests for the DAG's ingestion task callables: fake HTTP, in-memory S3 (moto)."""

from __future__ import annotations

import json
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from railfall.config import Settings
from railfall.ingest import meteo
from railfall.ingest.http import Download, DownloadError
from railfall.ingest.sncf import PUNCTUALITY, STATIONS
from railfall.orchestration import ingest as tasks

BUCKET = "railfall-lake"
DAY = "2026-11-12"
FIXTURES = Path(__file__).parent / "fixtures"
METEO_BASE = "https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/"
PAYLOADS = {
    PUNCTUALITY.export_url(): (FIXTURES / "sncf" / "punctuality_sample.json").read_bytes(),
    STATIONS.export_url(): (FIXTURES / "sncf" / "stations_sample.json").read_bytes(),
    meteo.dataset_url(): (FIXTURES / "meteo" / "catalogue_sample.json").read_bytes(),
    METEO_BASE + "Q_75_latest-2025-2026_RR-T-Vent.csv.gz": (
        FIXTURES / "meteo" / "Q_75_latest-2025-2026_RR-T-Vent.csv.gz"
    ).read_bytes(),
}
SETTINGS = Settings(lake_bucket=BUCKET, meteo_departments=("75",))


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def fetcher(payloads=PAYLOADS):
    def _fetch(url: str) -> Download:
        if url not in payloads:
            raise DownloadError(f"GET {url} -> HTTP 404 Not Found")
        return Download(url=url, status=200, content=payloads[url], content_type="")

    return _fetch


def _deps(s3, **kwargs):
    return {"settings": SETTINGS, "client": s3, "min_rows": 1, **kwargs}


def test_ingest_sncf_returns_a_json_serialisable_summary(s3) -> None:
    summary = tasks.ingest_sncf(DAY, fetcher=fetcher(), **_deps(s3))
    assert [(r["dataset"], r["status"]) for r in summary] == [
        ("punctuality", "written"),
        ("stations", "written"),
    ]
    assert all(f"ingest_date={DAY}/" in r["key"] for r in summary)
    json.dumps(summary)  # XCom-safe


def test_retry_after_success_is_idempotent(s3) -> None:
    tasks.ingest_sncf(DAY, fetcher=fetcher(), **_deps(s3))
    again = tasks.ingest_sncf(DAY, fetcher=fetcher(), **_deps(s3))
    assert {r["status"] for r in again} == {"skipped"}


def test_failed_dataset_fails_the_task_after_landing_the_others(s3) -> None:
    payloads = {k: v for k, v in PAYLOADS.items() if k != STATIONS.export_url()}
    with pytest.raises(tasks.TaskFailed, match=r"1 dataset\(s\) failed: stations: .*404"):
        tasks.ingest_sncf(DAY, fetcher=fetcher(payloads), **_deps(s3))
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET)["Contents"]]
    assert any(k.startswith("bronze/sncf/punctuality/") for k in keys)
    assert not any(k.startswith("bronze/sncf/stations/") for k in keys)


def test_ingest_meteo_handles_one_department(s3) -> None:
    summary = tasks.ingest_meteo(DAY, "75", fetcher=fetcher(), **_deps(s3))
    assert [(r["dataset"], r["ok"]) for r in summary] == [("daily_75_latest", True)]


def test_ingest_meteo_unknown_department_fails(s3) -> None:
    # 13 is in the catalogue sample but its file is not served.
    with pytest.raises(tasks.TaskFailed, match="ingest meteo 13"):
        tasks.ingest_meteo(DAY, "13", fetcher=fetcher(), **_deps(s3))


def test_verify_tasks_pass_on_what_ingest_landed(s3) -> None:
    tasks.ingest_sncf(DAY, fetcher=fetcher(), **_deps(s3))
    tasks.ingest_meteo(DAY, "75", fetcher=fetcher(), **_deps(s3))

    sncf = tasks.verify_sncf(DAY, **_deps(s3))
    weather = tasks.verify_meteo(DAY, **_deps(s3))
    assert [c["dataset"] for c in sncf] == ["punctuality", "stations"]
    assert [(c["dataset"], c["ok"]) for c in weather] == [("daily_75_latest", True)]


def test_verify_fails_on_an_empty_partition(s3) -> None:
    with pytest.raises(tasks.TaskFailed, match="verify sncf: 2 dataset"):
        tasks.verify_sncf(DAY, **_deps(s3))


def test_verify_meteo_defaults_to_configured_departments(s3) -> None:
    tasks.ingest_meteo(DAY, "75", fetcher=fetcher(), **_deps(s3))
    settings = Settings(lake_bucket=BUCKET, meteo_departments=("75", "69"))
    with pytest.raises(tasks.TaskFailed, match="daily_69_latest"):
        tasks.verify_meteo(DAY, settings=settings, client=s3, min_rows=1)


def test_accepts_a_date_or_an_iso_string(s3) -> None:
    from datetime import date

    a = tasks.ingest_sncf(date(2026, 11, 12), fetcher=fetcher(), **_deps(s3))
    b = tasks.ingest_sncf(DAY, fetcher=fetcher(), **_deps(s3))
    assert [r["key"] for r in a] == [r["key"] for r in b]


def test_bad_date_is_rejected_before_any_download(s3) -> None:
    calls: list[str] = []

    def _fetch(url: str) -> Download:
        calls.append(url)
        return fetcher()(url)

    with pytest.raises(ValueError):
        tasks.ingest_sncf("12/11/2026", fetcher=_fetch, **_deps(s3))
    assert calls == []
