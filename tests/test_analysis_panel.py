"""The route-month panel reader, run on a small SQLite copy of the marts."""

from __future__ import annotations

import sqlite3

import numpy as np
import pytest

from railfall.analysis.panel import FACTORS, PANEL_COLUMNS, build_panel, panel_sql, read_panel
from railfall.analysis.stats import pooled_gap

# route, service, month, coverage, run, on_time, wet, windy, frosty
ROUTE_MONTHS = [
    ("PARIS-LYON", "National", "2024-01-01", "both", 600, 540, 1, 0, 1),
    ("PARIS-LYON", "National", "2024-02-01", "both", 580, 551, 0, 0, 0),
    ("PARIS-LYON", "National", "2024-07-01", "departure_only", 620, 589, 0, 1, 0),
    ("LYON-MARSEILLE", "National", "2024-01-01", "arrival_only", 300, 270, 1, 1, 0),
    ("LYON-MARSEILLE", "National", "2024-07-01", "both", 310, 300, 0, 0, 0),
    # No weather at either end: out of the comparison, like the marts.
    ("PARIS-GENEVE", "International", "2024-01-01", "none", 200, 150, 0, 0, 0),
    # Fully cancelled month: no punctuality to compare.
    ("LYON-MARSEILLE", "National", "2024-02-01", "both", 0, None, 0, 0, 1),
]
MONTHS = [("2024-01-01", "winter"), ("2024-02-01", "winter"), ("2024-07-01", "summer")]


@pytest.fixture
def marts(tmp_path):
    conn = sqlite3.connect(tmp_path / "marts.db")
    conn.execute(
        "create table int_route_month_weather (route_id text, service text, month text,"
        " weather_coverage text, trains_run integer, trains_on_time integer,"
        " is_wet boolean, is_windy boolean, is_frosty boolean)"
    )
    conn.executemany("insert into int_route_month_weather values (?,?,?,?,?,?,?,?,?)", ROUTE_MONTHS)
    conn.execute("create table dim_month (month text, season text)")
    conn.executemany("insert into dim_month values (?,?)", MONTHS)
    yield conn
    conn.close()


def test_panel_keeps_covered_months_that_ran_trains(marts):
    panel = read_panel(marts, schema=None)

    assert len(panel) == 5
    assert panel.routes == 2
    assert panel.months == 3
    assert "PARIS-GENEVE" not in panel.route_id
    assert panel.trains_run.sum() == 600 + 580 + 620 + 300 + 310


def test_panel_flags_and_strata(marts):
    panel = read_panel(marts, schema=None)

    assert set(panel.adverse) == set(FACTORS)
    assert panel.adverse["frost"].tolist() == [False, False, True, False, False]
    assert panel.adverse["rain"].sum() == 2
    # route x service x season: Lyon-Marseille winter and summer are two strata.
    assert len(set(panel.strata.tolist())) == 4
    assert "PARIS-LYON|National|winter" in panel.strata
    assert panel.clusters.tolist() == panel.route_id.tolist()


def test_naive_gap_matches_the_mart_formula(marts):
    panel = read_panel(marts, schema=None)
    gap = pooled_gap(panel.trains_on_time, panel.trains_run, panel.adverse["rain"])

    adverse = (540 + 270) / (600 + 300)
    normal = (551 + 589 + 300) / (580 + 620 + 310)
    assert gap == pytest.approx(adverse - normal)


def test_panel_sql_qualifies_the_schema():
    sql = panel_sql("marts")
    assert "marts.int_route_month_weather" in sql
    assert "marts.dim_month" in sql
    assert "marts." not in panel_sql(None)


@pytest.mark.parametrize("schema", ["marts; drop table x", "Marts", "1marts", ""])
def test_panel_sql_rejects_odd_schema_names(schema):
    with pytest.raises(ValueError, match="invalid schema"):
        panel_sql(schema)


def test_build_panel_accepts_postgres_types():
    import datetime

    row = ("A-B", None, datetime.date(2024, 3, 1), "spring", 10, 9, True, False, "false")
    panel = build_panel([row])

    assert panel.month.tolist() == ["2024-03-01"]
    assert panel.service.tolist() == [""]
    assert panel.adverse["rain"].tolist() == [True]
    assert panel.adverse["frost"].tolist() == [False]


def test_build_panel_empty_and_invalid():
    empty = build_panel([])
    assert len(empty) == 0
    assert empty.routes == 0
    assert empty.adverse["wind"].size == 0

    with pytest.raises(ValueError, match="columns"):
        build_panel([("A-B", "National")])
    bad = ("A-B", "National", "2024-01-01", "winter", 10, 11, 0, 0, 0)
    assert len(bad) == len(PANEL_COLUMNS)
    with pytest.raises(ValueError, match="between 0 and trains_run"):
        build_panel([bad])


def test_strata_are_numpy_strings(marts):
    panel = read_panel(marts, schema=None)
    assert isinstance(panel.strata, np.ndarray)
