"""Run metrics: volume verdicts (pure) and run summaries on a small SQLite audit log."""

import json
import sqlite3
from datetime import UTC, datetime

import pytest

from railfall.monitoring import runs as rm

# --------------------------------------------------------------------------- judge_volume


def test_short_history_is_not_judged():
    check = rm.judge_volume("stations", "full", 10, [3000, 3000])
    assert check.ok
    assert check.baseline is None
    assert check.history == 2


def test_volume_in_range_is_ok():
    check = rm.judge_volume("stations", "full", 3010, [2990, 3000, 3005, 2400])
    assert check.ok
    assert check.baseline == 2995
    assert check.ratio == 1.005


def test_collapse_and_jump_are_anomalies():
    drop = rm.judge_volume("weather_daily", "full", 400, [1000, 1000, 1100])
    assert not drop.ok
    assert drop.reasons == ("staged 400 rows, 40% of the usual 1000",)

    jump = rm.judge_volume("weather_daily", "full", 2500, [1000, 1000, 1100])
    assert jump.reasons == ("staged 2500 rows, 2.5x the usual 1000",)


def test_zero_baseline():
    assert rm.judge_volume("t", "full", 0, [0, 0, 0]).ok
    rows_from_nowhere = rm.judge_volume("t", "full", 5, [0, 0, 0])
    assert rows_from_nowhere.reasons == ("staged 5 rows where previous loads staged none",)
    assert rows_from_nowhere.ratio is None


def test_thresholds_must_bracket_one():
    with pytest.raises(ValueError):
        rm.judge_volume("t", "full", 1, [1, 1, 1], low=1.2)


# --------------------------------------------------------------------------- SQL


def test_sql_is_schema_qualified():
    assert '"raw"."_load_runs"' in rm.runs_sql("raw")
    assert rm.runs_sql(None, "?").endswith("limit ?")
    assert "where ok" in rm.history_sql("raw")


# --------------------------------------------------------------------------- SQLite

COLUMNS = "run_id, table_name, scope, ok, staged, inserted, updated, unchanged, deleted"


def entry(run, table, start, *, ok=1, staged=100, scope="full", inserted=0, minutes=2, hour=5):
    return (
        run,
        table,
        "raw." + table,
        scope,
        ok,
        staged,
        inserted,
        0,
        staged - inserted,
        0,
        None if ok else "boom",
        f"2026-11-{start} {hour:02d}:15:00",
        f"2026-11-{start} {hour:02d}:{15 + minutes:02d}:00",
    )


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "create table _load_runs (run_id text, table_name text, target text, scope text,"
        " ok boolean, staged int, inserted int, updated int, unchanged int, deleted int,"
        " error text, started_at timestamp, finished_at timestamp)"
    )
    rows = []
    for day in ("17", "18", "19", "20"):
        rows.append(entry(f"r{day}", "stations", day, staged=3000))
        rows.append(entry(f"r{day}", "weather_daily", day, staged=1000, minutes=4))
    # A partial reload: different scope, must not feed the full-scope baseline.
    rows.append(entry("r20b", "weather_daily", "20", staged=50, scope="2026", hour=9))
    # Latest run: weather collapsed, stations failed.
    rows.append(entry("r21", "weather_daily", "21", staged=300, inserted=10, minutes=6))
    rows.append(entry("r21", "stations", "21", ok=0, staged=0))
    conn.executemany("insert into _load_runs values (" + ", ".join("?" * 13) + ")", rows)
    yield conn
    conn.close()


def test_recent_runs_aggregates_per_run(db):
    summaries = rm.recent_runs(db, 3, schema=None, placeholder="?")
    assert [r.run_id for r in summaries] == ["r21", "r20b", "r20"]
    latest = summaries[0]
    assert not latest.ok
    assert latest.failed == ("stations",)
    assert latest.tables == 2
    assert latest.started_at == datetime(2026, 11, 21, 5, 15, tzinfo=UTC)
    assert latest.duration_s == 360
    assert latest.totals["staged"] == 300
    assert latest.totals["inserted"] == 10
    assert summaries[1].ok


def test_recent_runs_rejects_bad_limit(db):
    with pytest.raises(ValueError):
        rm.recent_runs(db, 0, schema=None, placeholder="?")


def test_volume_checks_compare_like_scopes_only(db):
    checks = {c.table: c for c in rm.volume_checks(db, schema=None)}
    weather = checks["weather_daily"]
    # The 2026-scope reload (50 rows) is skipped: the baseline is the four full loads.
    assert weather.scope == "full"
    assert weather.history == 4
    assert weather.baseline == 1000
    assert not weather.ok
    # Stations' latest *successful* load is the 20th: in line with its history.
    assert checks["stations"].latest == 3000
    assert checks["stations"].history == 3
    assert checks["stations"].ok


def test_missing_log_means_no_runs_and_unhealthy():
    conn = sqlite3.connect(":memory:")
    runs = rm.recent_runs(conn, schema=None, placeholder="?")
    volumes = rm.volume_checks(conn, schema=None)
    assert runs == [] and volumes == []
    assert not rm.healthy(runs, volumes)
    assert rm.format_report(runs, volumes) == "no load runs recorded"


def test_report_and_text(db):
    runs = rm.recent_runs(db, 2, schema=None, placeholder="?")
    volumes = rm.volume_checks(db, schema=None)
    payload = json.loads(json.dumps(rm.report(runs, volumes)))
    assert payload["healthy"] is False
    assert payload["runs"][0]["failed"] == ["stations"]
    assert payload["runs"][0]["duration_s"] == 360
    assert {v["table"] for v in payload["volumes"]} == {"stations", "weather_daily"}

    text = rm.format_report(runs, volumes)
    assert "FAILED: stations" in text
    assert "ANOMALY: staged 300 rows, 30% of the usual 1000" in text
    assert text.endswith("UNHEALTHY")


def test_healthy_when_last_run_ok_and_volumes_normal(db):
    db.execute("delete from _load_runs where run_id = 'r21'")
    runs = rm.recent_runs(db, schema=None, placeholder="?")
    # Latest run is now the partial 2026 reload, which has only itself as history.
    volumes = rm.volume_checks(db, schema=None)
    assert rm.healthy(runs, volumes)
    assert "n/a (only 0 earlier 2026 loads)" in rm.format_report(runs, volumes)
