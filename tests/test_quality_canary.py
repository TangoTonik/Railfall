"""The canary plants defects in copies and the real suites must catch them (on SQLite)."""

from __future__ import annotations

import sqlite3

import pytest

from railfall.quality import canary
from railfall.quality.checkpoint import Failure, TableReport
from railfall.quality.suites import SUITES

pytest.importorskip("great_expectations")

from quality_db import STATIONS, WEATHER_MONTHLY  # noqa: E402
from quality_db import sqlite_db as _db  # noqa: E402


def _sqlite(url: str):
    conn = sqlite3.connect(url.removeprefix("sqlite:///"), isolation_level=None)
    return conn


def _tables(conn) -> set[str]:
    return {r[0] for r in conn.execute("select name from sqlite_master where type = 'table'")}


def test_every_raw_suite_has_a_canary_aimed_at_one_of_its_expectations():
    assert set(canary.CORRUPTIONS) == set(SUITES)
    for table, spec in canary.CORRUPTIONS.items():
        targeted = {
            (_snake(e.kind), e.kwargs.get("column") or e.kwargs.get("column_A"))
            for e in SUITES[table]
        }
        assert (spec.expectation, spec.column) in targeted, table


def _snake(kind: str) -> str:
    return "".join(f"_{c.lower()}" if c.isupper() else c for c in kind).lstrip("_")


def test_the_gate_catches_each_planted_defect_and_the_copies_are_dropped(tmp_path):
    url = _db(tmp_path)
    conn = _sqlite(url)
    before = _tables(conn)

    results = canary.run(conn.execute, url, ["stations", "weather_monthly"], schema=None)

    assert [(r.table, r.caught) for r in results] == [
        ("stations", True),
        ("weather_monthly", True),
    ]
    assert _tables(conn) == before
    # The originals were never touched: same rows as before the canary.
    assert len(conn.execute("select * from stations").fetchall()) == len(STATIONS)
    assert conn.execute("select max(heavy_rain_days) from weather_monthly").fetchone() == (3.0,)


def test_only_one_row_is_corrupted(tmp_path):
    url = _db(tmp_path)
    conn = _sqlite(url)
    copies = canary.plant(conn.execute, ["stations", "weather_monthly"], None)
    assert conn.execute(
        f"select count(*) from \"{copies['stations']}\" where department = 'XX'"
    ).fetchone() == (1,)
    assert conn.execute(
        f'select count(*) from "{copies["weather_monthly"]}" where heavy_rain_days > rain_days'
    ).fetchone() == (1,)
    canary.remove(conn.execute, copies.values(), None)


def test_null_counters_are_skipped_when_picking_the_row(tmp_path):
    # The first department-month has no rain counter: corrupting it would be invisible.
    weather = [("01", "2023-01-01", 2023, 1, None, None, None, 2.0, -1.0, 3.0, 2.0, 9.0, 0.0)]
    url = _db(tmp_path, weather=[*weather, *WEATHER_MONTHLY])
    conn = _sqlite(url)
    (result,) = canary.run(conn.execute, url, ["weather_monthly"], schema=None)
    assert result.caught


def test_copies_are_dropped_even_when_validation_blows_up(tmp_path, monkeypatch):
    url = _db(tmp_path)
    conn = _sqlite(url)
    before = _tables(conn)

    def boom(*args, **kwargs):
        raise RuntimeError("datasource down")

    monkeypatch.setattr(canary.checkpoint, "run", boom)
    with pytest.raises(RuntimeError):
        canary.run(conn.execute, url, ["stations"], schema=None)
    assert _tables(conn) == before


def test_a_passing_copy_or_the_wrong_failure_is_a_miss():
    passing = TableReport("stations", ok=True, evaluated=8)
    elsewhere = TableReport(
        "stations",
        ok=False,
        evaluated=8,
        failures=[Failure("expect_column_values_to_be_unique", "uic_code", 1)],
    )
    results = canary.judge([passing, elsewhere])
    assert [r.caught for r in results] == [False, False]
    assert results[1].failures == ["expect_column_values_to_be_unique(uic_code)"]
    assert "MISSED" in canary._format(results)


def test_qualified_sql_for_postgres():
    sql = canary.corruption_sql("route_departments", '"raw"."_canary_route_departments"')
    assert sql.startswith('insert into "raw"."_canary_route_departments" select * from')
    assert "min(route_id)" in sql


def test_unknown_table_is_refused():
    with pytest.raises(ValueError, match="no canary for: nope"):
        canary.run(lambda s: None, "sqlite:///unused.db", ["nope"])
