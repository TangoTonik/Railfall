"""Unit tests for the silver job: bronze input resolution and a full local run."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pytest

from railfall.spark import job
from railfall.spark.write import PUNCTUALITY, STATIONS, WEATHER

FIXTURES = Path(__file__).parent / "fixtures"
RUN = date(2026, 11, 9)

KEYS = [
    "bronze/sncf/punctuality/ingest_date=2026-11-03/punctuality.json",
    "bronze/sncf/punctuality/ingest_date=2026-11-03/_manifest.json",
    "bronze/sncf/punctuality/ingest_date=2026-11-07/punctuality.json",
    "bronze/sncf/punctuality/ingest_date=2026-11-07/_manifest.json",
    "bronze/sncf/stations/ingest_date=2026-11-05/stations.json",
    "bronze/meteo/daily_75_latest/ingest_date=2026-11-05/daily_75_latest.csv.gz",
]


def test_latest_snapshot_picks_newest_day_and_ignores_manifests() -> None:
    assert job.latest_snapshot(KEYS, "sncf", "punctuality") == KEYS[2]
    assert job.latest_snapshot(KEYS, "sncf", "stations") == KEYS[4]


def test_latest_snapshot_does_not_match_a_dataset_prefix() -> None:
    keys = ["bronze/sncf/stations_old/ingest_date=2026-11-08/stations_old.json"]
    with pytest.raises(job.MissingBronze):
        job.latest_snapshot(keys, "sncf", "stations")


def test_bronze_input_paths() -> None:
    root = "s3a://lake/"
    assert job.bronze_input(PUNCTUALITY, KEYS, root) == f"s3a://lake/{KEYS[2]}"
    assert job.bronze_input(WEATHER, KEYS, root) == (
        "s3a://lake/bronze/meteo/daily_*/ingest_date=*/*.csv.gz"
    )
    with pytest.raises(job.MissingBronze):
        job.bronze_input(WEATHER, KEYS[:5], root)


def test_unknown_table_is_refused() -> None:
    with pytest.raises(ValueError, match="nope"):
        job.run(None, root="/x", keys=[], names=["nope"], run_date=RUN)


def _local_lake(root: Path, day: str = "2026-11-09") -> None:
    files = {
        f"bronze/sncf/punctuality/ingest_date={day}/punctuality.json": FIXTURES
        / "sncf"
        / "punctuality_sample.json",
        f"bronze/sncf/stations/ingest_date={day}/stations.json": FIXTURES
        / "sncf"
        / "stations_sample.json",
        f"bronze/meteo/daily_75_latest/ingest_date={day}/daily_75_latest.csv.gz": FIXTURES
        / "meteo"
        / "Q_75_latest-2025-2026_RR-T-Vent.csv.gz",
    }
    for key, src in files.items():
        dst = root / key
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dst)


def test_list_local_keys(tmp_path) -> None:
    _local_lake(tmp_path)
    keys = job.list_local_keys(str(tmp_path))
    assert len(keys) == 3
    assert all(k.startswith("bronze/") for k in keys)


@pytest.mark.spark
def test_full_run_is_idempotent(spark, tmp_path) -> None:
    _local_lake(tmp_path)
    root = str(tmp_path)
    keys = job.list_local_keys(root)

    first = job.run(spark, root=root, keys=keys, run_date=RUN)
    assert all(r.ok for r in first), [r.error for r in first]
    assert {r.table for r in first} == {"punctuality_monthly", "weather_daily", "stations"}
    assert all(r.rows > 0 for r in first)

    second = job.run(spark, root=root, keys=keys, run_date=RUN)
    assert [(r.table, r.rows) for r in second] == [(r.table, r.rows) for r in first]
    for r in second:
        assert spark.read.parquet(r.path).count() == r.rows


@pytest.mark.spark
def test_missing_input_fails_only_its_table(spark, tmp_path) -> None:
    _local_lake(tmp_path)
    shutil.rmtree(tmp_path / "bronze" / "meteo")
    root = str(tmp_path)

    results = {
        r.table: r for r in job.run(spark, root=root, keys=job.list_local_keys(root), run_date=RUN)
    }
    assert not results["weather_daily"].ok
    assert "MissingBronze" in results["weather_daily"].error
    assert results[STATIONS.name].ok and results[PUNCTUALITY.name].ok
