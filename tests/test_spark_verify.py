"""Unit tests for the silver contract checks."""

from __future__ import annotations

from datetime import date

import pytest
from lakes import derived_lake
from lakes import local_lake as _lake

from railfall.spark import job, verify
from railfall.spark.write import PUNCTUALITY, ROUTE_WEATHER, STATIONS, TABLES


def test_every_table_has_a_contract_covering_key_and_partitions() -> None:
    assert set(verify.CONTRACTS) == set(TABLES)
    for name, contract in verify.CONTRACTS.items():
        for column in (*contract.key, *TABLES[name].partition_by):
            assert column in contract.columns, (name, column)


def test_profiled_columns_are_contract_columns() -> None:
    for name, contract in verify.CONTRACTS.items():
        for column in contract.profile:
            assert column in contract.columns, (name, column)
    # The weather join is the one whose coverage e2e must be able to see.
    assert "weather_endpoints" in verify.CONTRACTS[ROUTE_WEATHER.name].profile


def test_schema_errors_reports_missing_and_mistyped_columns() -> None:
    expected = {"a": "string", "b": "bigint", "c": "date"}
    actual = {"a": "string", "b": "int", "extra": "double"}
    assert verify.schema_errors(actual, expected) == [
        "b: expected bigint, got int",
        "missing column c",
    ]
    assert verify.schema_errors(expected, expected) == []


@pytest.mark.spark
def test_job_output_passes_its_contract(spark, tmp_path) -> None:
    derived_lake(tmp_path)
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


@pytest.mark.spark
def test_profile_counts_values_including_nulls(spark) -> None:
    df = spark.createDataFrame(
        [("a", 2, True), ("b", 2, None), ("c", 0, False)],
        "k string, endpoints int, flag boolean",
    )
    assert verify.value_counts(df, ["endpoints", "flag"]) == {
        "endpoints": {"0": 1, "2": 2},
        "flag": {"false": 1, "null": 1, "true": 1},
    }


@pytest.mark.spark
def test_route_weather_report_profiles_endpoint_coverage(spark, tmp_path) -> None:
    derived_lake(tmp_path)
    root = str(tmp_path)
    job.run(spark, root=root, keys=job.list_local_keys(root), run_date=date(2026, 11, 9))
    (report,) = verify.verify(spark, root=root, names=[ROUTE_WEATHER.name])
    counts = report.profile["weather_endpoints"]
    assert sum(counts.values()) == report.rows
    # The lake has weather for 75 and 13, so some route-months must carry it.
    assert sum(n for v, n in counts.items() if v != "0") > 0
    assert "weather_endpoints:" in verify._format(report)
