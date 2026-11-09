"""Unit tests for writing silver Parquet tables (local filesystem as the lake)."""

from __future__ import annotations

from datetime import date

import pytest

from railfall.spark import write as w

pytestmark = pytest.mark.spark

TABLE = w.Table("demo", ("year",))
RUN = date(2026, 11, 9)


def _frame(spark, rows):
    return spark.createDataFrame(rows, "year int, k string, v double")


def _empty(spark):
    return spark.createDataFrame([], "k string, reject_reason string")


def test_paths() -> None:
    assert w.silver_path("s3a://lake/", w.WEATHER) == "s3a://lake/silver/weather_daily"
    assert (
        w.rejected_path("s3a://lake", w.STATIONS, RUN)
        == "s3a://lake/silver/_rejected/stations/run_date=2026-11-09"
    )
    assert set(w.TABLES) == {"punctuality_monthly", "weather_daily", "stations"}


def test_write_is_partitioned_and_idempotent(spark, tmp_path) -> None:
    root = str(tmp_path)
    rows = [(2024, "a", 1.0), (2025, "b", 2.0)]

    for _ in range(2):
        result = w.write_silver(
            _frame(spark, rows), _empty(spark), root=root, table=TABLE, run_date=RUN
        )

    assert result.rows == 2 and result.rejected == 0 and result.rejected_path is None
    assert sorted(p.name for p in (tmp_path / "silver" / "demo").glob("year=*")) == [
        "year=2024",
        "year=2025",
    ]
    assert spark.read.parquet(result.path).count() == 2


def test_rewrite_only_replaces_partitions_it_produces(spark, tmp_path) -> None:
    root = str(tmp_path)
    w.write_silver(
        _frame(spark, [(2024, "a", 1.0), (2025, "b", 2.0)]),
        _empty(spark),
        root=root,
        table=TABLE,
        run_date=RUN,
    )
    w.write_silver(
        _frame(spark, [(2025, "b", 3.0)]), _empty(spark), root=root, table=TABLE, run_date=RUN
    )

    back = {r.k: r.v for r in spark.read.parquet(w.silver_path(root, TABLE)).collect()}
    assert back == {"a": 1.0, "b": 3.0}


def test_rejected_rows_are_quarantined_per_run_date(spark, tmp_path) -> None:
    root = str(tmp_path)
    bad = spark.createDataFrame(
        [("x", "bad_day"), ("y", "bad_station")], "k string, reject_reason string"
    )

    for _ in range(2):
        result = w.write_silver(
            _frame(spark, [(2024, "a", 1.0)]), bad, root=root, table=TABLE, run_date=RUN
        )

    assert result.rejected == 2
    assert result.rejected_path.endswith("_rejected/demo/run_date=2026-11-09")
    assert spark.read.parquet(result.rejected_path).count() == 2
