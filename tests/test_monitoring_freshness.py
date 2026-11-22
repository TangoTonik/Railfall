"""Freshness checks: verdict rules (pure) and queries on a small SQLite warehouse."""

import sqlite3
from datetime import UTC, date, datetime, timedelta

import pytest

from railfall.monitoring import freshness as fr

NOW = datetime(2026, 11, 22, 8, 0, tzinfo=UTC)
MONTHLY = fr.RULES["punctuality_monthly"]
STATIONS = fr.RULES["stations"]


# --------------------------------------------------------------------------- rules


def test_every_silver_table_has_a_rule():
    from railfall.spark.write import TABLES

    assert set(fr.RULES) == set(TABLES)


def test_rule_needs_both_column_and_lag():
    with pytest.raises(ValueError, match="go together"):
        fr.FreshnessRule("t", data_column="month")
    with pytest.raises(ValueError, match="positive"):
        fr.FreshnessRule("t", max_load_age=timedelta(0))


# --------------------------------------------------------------------------- evaluate


def test_recent_load_and_recent_month_is_ok():
    result = fr.evaluate(
        MONTHLY,
        rows=1200,
        last_loaded_at=NOW - timedelta(hours=3),
        latest_period=date(2026, 9, 1),
        now=NOW,
    )
    assert result.ok
    assert result.load_age_hours == 3
    assert result.data_lag_days == 82
    assert result.reasons == ()


def test_old_load_is_stale_even_with_fresh_data():
    result = fr.evaluate(
        MONTHLY,
        rows=10,
        last_loaded_at=NOW - timedelta(hours=50),
        latest_period=date(2026, 10, 1),
        now=NOW,
    )
    assert result.status == fr.STALE
    assert result.reasons == ("last successful load 50 h ago (limit 36 h)",)


def test_source_that_stopped_publishing_is_stale_even_if_loads_succeed():
    result = fr.evaluate(
        MONTHLY,
        rows=10,
        last_loaded_at=NOW - timedelta(hours=1),
        latest_period=date(2026, 6, 1),
        now=NOW,
    )
    assert result.status == fr.STALE
    assert result.data_lag_days == 174
    assert "newest month 2026-06-01 is 174 days old (limit 125)" in result.reasons


def test_never_loaded_is_stale_and_empty_wins():
    never = fr.evaluate(STATIONS, rows=5, last_loaded_at=None, latest_period=None, now=NOW)
    assert never.status == fr.STALE
    assert never.reasons == ("no successful load recorded",)

    empty = fr.evaluate(
        STATIONS, rows=0, last_loaded_at=NOW - timedelta(hours=1), latest_period=None, now=NOW
    )
    assert empty.status == fr.EMPTY
    assert empty.reasons == ("table is empty",)


def test_reference_tables_ignore_business_dates():
    result = fr.evaluate(
        STATIONS,
        rows=3,
        last_loaded_at=NOW - timedelta(hours=1),
        latest_period=date(2000, 1, 1),
        now=NOW,
    )
    assert result.ok
    assert result.data_lag_days is None


def test_as_dict_is_json_ready():
    result = fr.evaluate(
        MONTHLY, rows=1, last_loaded_at=NOW, latest_period=date(2026, 10, 1), now=NOW
    )
    assert result.as_dict() == {
        "table": "punctuality_monthly",
        "status": "ok",
        "rows": 1,
        "last_loaded_at": "2026-11-22T08:00:00+00:00",
        "load_age_hours": 0.0,
        "latest_period": "2026-10-01",
        "data_lag_days": 52,
        "reasons": [],
    }


# --------------------------------------------------------------------------- parsing


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, None),
        ("2026-11-22 07:00:00", datetime(2026, 11, 22, 7, tzinfo=UTC)),
        ("2026-11-22T07:00:00+01:00", datetime(2026, 11, 22, 6, tzinfo=UTC)),
        (datetime(2026, 11, 22, 7), datetime(2026, 11, 22, 7, tzinfo=UTC)),
    ],
)
def test_as_datetime_is_always_aware(raw, expected):
    assert fr.as_datetime(raw) == expected


def test_as_date_accepts_text_dates_and_datetimes():
    assert fr.as_date("2026-10-01") == date(2026, 10, 1)
    assert fr.as_date(datetime(2026, 10, 1, 12)) == date(2026, 10, 1)
    with pytest.raises(TypeError):
        fr.as_date(42)


# --------------------------------------------------------------------------- SQL


def test_sql_is_quoted_and_schema_qualified():
    assert fr.table_sql(MONTHLY, "raw") == (
        'select count(*), max("month") from "raw"."punctuality_monthly"'
    )
    assert fr.table_sql(STATIONS, None) == 'select count(*), null from "stations"'
    assert fr.last_load_sql("raw") == (
        'select max(finished_at) from "raw"."_load_runs" where table_name = %s and ok'
    )


# --------------------------------------------------------------------------- SQLite


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.execute("create table punctuality_monthly (month text, service text)")
    conn.executemany(
        "insert into punctuality_monthly values (?, ?)",
        [("2026-08-01", "TGV"), ("2026-09-01", "TGV"), ("2026-09-01", "TER")],
    )
    conn.execute("create table stations (uic_code text)")
    conn.execute("create table _load_runs (table_name text, ok boolean, finished_at timestamp)")
    conn.executemany(
        "insert into _load_runs values (?, ?, ?)",
        [
            ("punctuality_monthly", 1, "2026-11-22 05:40:00"),
            # A later failed run must not count as a load.
            ("punctuality_monthly", 0, "2026-11-22 07:00:00"),
            ("stations", 1, "2026-11-19 05:40:00"),
        ],
    )
    yield conn
    conn.close()


def run(conn, tables=None):
    return {r.table: r for r in fr.check(conn, tables, now=NOW, schema=None, placeholder="?")}


def test_check_reads_counts_dates_and_last_successful_load(db):
    results = run(db, ["punctuality_monthly", "stations", "weather_daily"])
    punct = results["punctuality_monthly"]
    assert punct.ok
    assert punct.rows == 3
    assert punct.latest_period == date(2026, 9, 1)
    assert punct.last_loaded_at == datetime(2026, 11, 22, 5, 40, tzinfo=UTC)

    assert results["stations"].status == fr.EMPTY
    assert results["weather_daily"].status == fr.MISSING


def test_check_without_audit_log_reports_no_load(db):
    db.execute("drop table _load_runs")
    result = run(db, ["punctuality_monthly"])["punctuality_monthly"]
    assert result.status == fr.STALE
    assert result.reasons == ("no successful load recorded",)


def test_check_refuses_tables_without_a_rule(db):
    with pytest.raises(ValueError, match="no freshness rule for: nope"):
        fr.check(db, ["nope"], now=NOW)
