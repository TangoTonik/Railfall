"""Unit tests for the silver job: bronze input resolution and a full local run."""

from __future__ import annotations

import shutil
from datetime import date

import pytest
from lakes import derived_lake as _derived_lake
from lakes import local_lake as _local_lake

from railfall.spark import job
from railfall.spark.write import (
    BASE,
    DERIVED,
    PUNCTUALITY,
    ROUTE_DEPARTMENTS,
    ROUTE_WEATHER,
    STATIONS,
    TABLES,
    WEATHER,
    WEATHER_MONTHLY,
    read_silver,
)

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

    first = job.run(spark, root=root, keys=keys, names=[t.name for t in BASE], run_date=RUN)
    assert all(r.ok for r in first), [r.error for r in first]
    assert {r.table for r in first} == {"punctuality_monthly", "weather_daily", "stations"}
    assert all(r.rows > 0 for r in first)

    second = job.run(spark, root=root, keys=keys, names=[t.name for t in BASE], run_date=RUN)
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
    # Tables built on the failed one fail with the cause instead of reading stale silver;
    # the route mapping does not need weather and still builds.
    assert "UpstreamFailed" in results[WEATHER_MONTHLY.name].error
    assert "UpstreamFailed" in results[ROUTE_WEATHER.name].error
    assert results[ROUTE_DEPARTMENTS.name].ok


def test_tables_are_listed_in_dependency_order() -> None:
    order = list(TABLES.values())
    for table, inputs in job.DEPENDS_ON.items():
        assert all(order.index(i) < order.index(table) for i in inputs), table.name
    assert set(job.DEPENDS_ON) == set(DERIVED) == set(job.BUILDERS)


def test_derived_tables_run_after_their_inputs_whatever_the_selection_order(
    monkeypatch,
) -> None:
    calls: list[str] = []

    def fake_base(spark, table, **_):
        calls.append(table.name)
        return job.TableResult(table=table.name, ok=True)

    def fake_derived(spark, table, **_):
        calls.append(table.name)
        return job.TableResult(table=table.name, ok=True)

    monkeypatch.setattr(job, "run_table", fake_base)
    monkeypatch.setattr(job, "run_derived", fake_derived)
    job.run(None, root="/x", keys=[], names=[ROUTE_WEATHER.name, STATIONS.name], run_date=RUN)
    assert calls == [STATIONS.name, ROUTE_WEATHER.name]


@pytest.mark.spark
def test_derived_tables_from_a_full_lake(spark, tmp_path) -> None:
    _derived_lake(tmp_path)
    root = str(tmp_path)
    results = {
        r.table: r for r in job.run(spark, root=root, keys=job.list_local_keys(root), run_date=RUN)
    }
    assert all(r.ok for r in results.values()), {t: r.error for t, r in results.items()}
    assert results[ROUTE_WEATHER.name].source == (
        "silver/punctuality_monthly, silver/route_departments, silver/weather_monthly"
    )

    monthly = read_silver(spark, root, WEATHER_MONTHLY)
    assert {(r.department, r.month) for r in monthly.collect()} == {
        ("75", date(2024, 1, 1)),
        ("75", date(2024, 2, 1)),
        ("13", date(2024, 1, 1)),
    }

    rows = read_silver(spark, root, ROUTE_WEATHER).collect()
    # Same grain as punctuality: every route-month kept exactly once.
    assert len(rows) == results[PUNCTUALITY.name].rows
    by_route = {(r.route_id, r.month): r for r in rows}
    paris_marseille = next(
        r
        for (route, month), r in by_route.items()
        if month == date(2024, 1, 1) and "MARSEILLE" in r.arrival_station
    )
    assert paris_marseille.weather_endpoints == 2
    assert paris_marseille.dep_frost_days > 0
    # Paris -> Genève, February: one French endpoint with weather, one foreign.
    geneve = next(r for r in by_route.values() if r.arrival_station == "GENEVE")
    assert not geneve.is_domestic and geneve.weather_endpoints == 1

    # Re-running rewrites the derived tables instead of appending to them.
    again = {
        r.table: r.rows
        for r in job.run(
            spark,
            root=root,
            keys=job.list_local_keys(root),
            names=[d.name for d in DERIVED],
            run_date=RUN,
        )
    }
    assert again == {d.name: results[d.name].rows for d in DERIVED}
    # Read fresh: a frame planned before the re-run points at replaced files.
    assert read_silver(spark, root, ROUTE_WEATHER).count() == len(rows)
