"""Warehouse verification: pure comparisons, and checks against a real Postgres."""

from __future__ import annotations

from datetime import date

import pytest

from railfall.load import job
from railfall.load import verify as v
from railfall.spark import write as w
from railfall.spark.verify import CONTRACTS


@pytest.mark.parametrize("table", list(w.TABLES.values()), ids=lambda t: t.name)
def test_contract_target_follows_the_silver_contract(table):
    target = v.contract_target(table)
    assert (target.schema, target.name) == ("raw", table.name)
    assert target.key == CONTRACTS[table.name].key
    assert set(v.expected_columns(table)) == set(CONTRACTS[table.name].columns)


def test_expected_columns_use_format_type_spelling():
    cols = v.expected_columns(w.PUNCTUALITY)
    assert cols["month"] == "date"
    assert cols["year"] == "integer"
    assert cols["trains_planned"] == "bigint"
    assert cols["arrival_punctuality_rate"] == "double precision"


def test_column_errors_report_missing_and_mistyped_but_allow_extras():
    expected = {"a": "integer", "b": "text"}
    assert v.column_errors({"a": "integer", "b": "text", "extra": "date"}, expected) == []
    assert v.column_errors({"a": "bigint"}, expected) == [
        "a: expected integer, got bigint",
        "missing column b",
    ]


def test_count_errors_compare_every_partition_on_both_sides():
    assert v.count_errors({"2024": 3, "2025": 2}, {"2024": 3, "2025": 2}) == []
    assert v.count_errors({"2024": 3, "2023": 1}, {"2024": 2, "2025": 4}) == [
        "year=2023: 1 rows in warehouse, 0 in silver",
        "year=2024: 3 rows in warehouse, 2 in silver",
        "year=2025: 0 rows in warehouse, 4 in silver",
    ]


def test_key_stats_sql_groups_on_the_whole_key():
    from psycopg import sql

    text = v.key_stats_sql(v.contract_target(w.PUNCTUALITY)).as_string(None)
    assert '"raw"."punctuality_monthly"' in text
    for column in CONTRACTS[w.PUNCTUALITY.name].key:
        assert f'"{column}" is null' in text
    assert "group by" in text
    assert isinstance(v.key_stats_sql(v.contract_target(w.STATIONS)), sql.Composed)


def test_verify_rejects_unknown_tables():
    with pytest.raises(ValueError, match="unknown silver tables"):
        v.verify(None, None, root="/nowhere", names=["nope"])


def test_report_line_lists_every_error():
    bad = v.WarehouseReport("stations", ok=False, target="raw.stations", errors=["x", "y"])
    assert v._format(bad).endswith("x; y")
    good = v.WarehouseReport("stations", ok=True, target="raw.stations", rows=12)
    assert "12 rows = silver" in v._format(good)


# --- Postgres ----------------------------------------------------------------------


@pytest.fixture
def warehouse(pg_schema):
    import psycopg

    dsn, schema = pg_schema
    with psycopg.connect(dsn, autocommit=True) as conn:
        yield conn, schema


def _create_stations(conn, schema, *, with_pk=True):
    from railfall.load.postgres import ensure_table

    target = v.contract_target(w.STATIONS, schema=schema)
    if with_pk:
        ensure_table(conn, target)
    else:
        cols = ", ".join(f'"{c.name}" {c.pg_type}' for c in target.columns)
        conn.execute(f"create table {schema}.stations ({cols})")
    return target


def _insert(conn, target, n, *, start=0):
    from railfall.load.postgres import merge_rows

    rows = []
    for i in range(start, start + n):
        row = []
        for c in target.columns:
            if c.pg_type == "text":
                row.append(f"{c.name}-{i}")
            elif c.pg_type in ("double precision", "real"):
                row.append(float(i))
            elif c.pg_type == "boolean":
                row.append(i % 2 == 0)
            elif c.pg_type == "date":
                row.append(date(2025, 1, 1))
            else:
                row.append(i)
        rows.append(row)
    with conn.transaction():
        merge_rows(conn, target, rows)


def test_missing_table_fails(warehouse):
    conn, schema = warehouse
    report = v.check_table(conn, w.STATIONS, silver_rows=3, silver_by_year={}, schema=schema)
    assert not report.ok and report.errors == ["table missing"]


def test_loaded_table_matching_silver_passes(warehouse):
    conn, schema = warehouse
    _insert(conn, _create_stations(conn, schema), 3)
    report = v.check_table(conn, w.STATIONS, silver_rows=3, silver_by_year={}, schema=schema)
    assert report.ok, report
    assert report.primary_key == list(CONTRACTS["stations"].key)
    assert (report.rows, report.duplicate_keys, report.null_keys) == (3, 0, 0)


def test_row_count_drift_fails(warehouse):
    conn, schema = warehouse
    _insert(conn, _create_stations(conn, schema), 3)
    report = v.check_table(conn, w.STATIONS, silver_rows=4, silver_by_year={}, schema=schema)
    assert not report.ok
    assert "3 rows in warehouse, 4 in silver" in report.errors


def test_table_without_primary_key_fails(warehouse):
    conn, schema = warehouse
    _create_stations(conn, schema, with_pk=False)
    report = v.check_table(conn, w.STATIONS, silver_rows=0, silver_by_year={}, schema=schema)
    assert not report.ok
    assert any(e.startswith("primary key is missing") for e in report.errors)


# --- Spark + Postgres --------------------------------------------------------------

PUNCT_DDL = (
    "month date, year int, route_id string, service string, departure_station string, "
    "arrival_station string, trains_planned bigint, trains_cancelled bigint, "
    "trains_late_arrival bigint, trains_run bigint, arrival_punctuality_rate double, "
    "pct_cause_external double"
)


def _punct(month, route):
    dep, arr = route.split("|")
    return (month, month.year, route, "National", dep, arr, 100, 2, 10, 98, 90.0, 12.5)


@pytest.mark.spark
def test_load_then_verify_per_year(spark, tmp_path, warehouse):
    conn, schema = warehouse
    root = str(tmp_path)
    rows = [
        _punct(date(2024, 3, 1), "PARIS LYON|LYON PART DIEU"),
        _punct(date(2025, 3, 1), "PARIS LYON|LYON PART DIEU"),
        _punct(date(2025, 4, 1), "PARIS LYON|LYON PART DIEU"),
    ]
    df = spark.createDataFrame(rows, PUNCT_DDL)
    w.write_silver(df, None, root=root, table=w.PUNCTUALITY, run_date=date(2026, 11, 11))
    (loaded,) = job.run(spark, conn, root=root, names=["punctuality_monthly"], schema=schema)
    assert loaded.ok, loaded

    (report,) = v.verify(spark, conn, root=root, names=["punctuality_monthly"], schema=schema)
    assert report.ok, report
    assert report.by_year == {"2024": 1, "2025": 2}

    conn.execute(f"delete from {schema}.punctuality_monthly where year = 2024")
    (report,) = v.verify(spark, conn, root=root, names=["punctuality_monthly"], schema=schema)
    assert not report.ok
    assert "year=2024: 0 rows in warehouse, 1 in silver" in report.errors
