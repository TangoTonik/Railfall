"""Unit tests for the silver contract checks."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pytest

from railfall.spark import job, verify
from railfall.spark.write import PUNCTUALITY, STATIONS, TABLES

FIXTURES = Path(__file__).parent / "fixtures"


def test_every_table_has_a_contract_covering_key_and_partitions() -> None:
    assert set(verify.CONTRACTS) == set(TABLES)
    for name, contract in verify.CONTRACTS.items():
        for column in (*contract.key, *TABLES[name].partition_by):
            assert column in contract.columns, (name, column)


def test_schema_errors_reports_missing_and_mistyped_columns() -> None:
    expected = {"a": "string", "b": "bigint", "c": "date"}
    actual = {"a": "string", "b": "int", "extra": "double"}
    assert verify.schema_errors(actual, expected) == [
        "b: expected bigint, got int",
        "missing column c",
    ]
    assert verify.schema_errors(expected, expected) == []


def _lake(root: Path) -> None:
    day = "2026-11-09"
    files = {
        f"bronze/sncf/punctuality/ingest_date={day}/punctuality.json": (
            "sncf/punctuality_sample.json"
        ),
        f"bronze/sncf/stations/ingest_date={day}/stations.json": "sncf/stations_sample.json",
        f"bronze/meteo/daily_75_latest/ingest_date={day}/daily_75_latest.csv.gz": (
            "meteo/Q_75_latest-2025-2026_RR-T-Vent.csv.gz"
        ),
    }
    for key, src in files.items():
        dst = root / key
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURES / src, dst)


@pytest.mark.spark
def test_job_output_passes_its_contract(spark, tmp_path) -> None:
    _lake(tmp_path)
    root = str(tmp_path)
    written = job.run(spark, root=root, keys=job.list_local_keys(root), run_date=date(2026, 11, 9))
    reports = verify.verify(spark, root=root)
    assert all(r.ok for r in reports), reports
    assert {r.table: r.rows for r in reports} == {r.table: r.rows for r in written}


@pytest.mark.spark
def test_duplicate_and_null_keys_fail(spark, tmp_path) -> None:
    _lake(tmp_path)
    root = str(tmp_path)
    job.run(spark, root=root, keys=job.list_local_keys(root), names=[STATIONS.name])
    df = spark.read.parquet(f"{root}/silver/stations")
    from pyspark.sql import functions as F

    first = df.limit(1)
    nulled = first.withColumn("uic_code", F.lit(None).cast("string"))
    report = verify.check_frame(df.unionByName(first).unionByName(nulled), STATIONS)
    assert not report.ok
    assert report.duplicate_keys == 1
    assert report.null_keys == 1


@pytest.mark.spark
def test_missing_table_and_wrong_type_fail(spark, tmp_path) -> None:
    missing = verify.verify_table(spark, PUNCTUALITY, str(tmp_path))
    assert not missing.ok and missing.error

    _lake(tmp_path)
    root = str(tmp_path)
    job.run(spark, root=root, keys=job.list_local_keys(root), names=[STATIONS.name])
    df = spark.read.parquet(f"{root}/silver/stations")
    report = verify.check_frame(df.withColumn("latitude", df["latitude"].cast("string")), STATIONS)
    assert not report.ok
    assert report.schema_errors == ["latitude: expected double, got string"]


@pytest.mark.spark
def test_empty_table_fails(spark, tmp_path) -> None:
    _lake(tmp_path)
    root = str(tmp_path)
    job.run(spark, root=root, keys=job.list_local_keys(root), names=[STATIONS.name])
    df = spark.read.parquet(f"{root}/silver/stations")
    report = verify.check_frame(df.limit(0), STATIONS)
    assert not report.ok and report.error == "table is empty"
