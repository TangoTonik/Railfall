"""Dose-response slopes: synthetic panels with a known slope, plus the SQLite reader."""

from __future__ import annotations

import math
import sqlite3

import numpy as np
import pytest

from railfall.analysis.dose import (
    DOSE_COLUMNS,
    analyse_dose,
    build_dose_panel,
    dose_response,
    dose_sql,
    read_dose_panel,
)


def _synthetic(slope: float, *, routes: int = 12, months: int = 10, seed: int = 3):
    """Route-months whose on-time share is route base + slope * dose + small noise."""
    rng = np.random.default_rng(seed)
    route, dose, run, on_time = [], [], [], []
    for r in range(routes):
        base = 0.80 + 0.1 * rng.random()
        for _ in range(months):
            x = rng.uniform(0.2, 2.5)
            n = 500.0
            share = np.clip(base + slope * x + rng.normal(0, 0.005), 0, 1)
            route.append(f"R{r:02d}")
            dose.append(x)
            run.append(n)
            on_time.append(round(share * n))
    route_a = np.asarray(route)
    return np.asarray(on_time, float), np.asarray(run), np.asarray(dose), route_a, route_a


def test_recovers_a_negative_slope():
    on_time, run, dose, strata, clusters = _synthetic(-0.04)
    test = dose_response(
        "rain", on_time, run, dose, strata, clusters, permutations=199, resamples=199
    )

    assert test.slope == pytest.approx(-0.04, abs=0.005)
    assert test.ci_low < test.slope < test.ci_high < 0
    assert test.p_value < 0.01
    assert test.strata_used == 12
    assert test.unit == "x normal rain"


def test_flat_relationship_is_not_significant():
    on_time, run, dose, strata, clusters = _synthetic(0.0)
    test = dose_response(
        "frost", on_time, run, dose, strata, clusters, permutations=199, resamples=199
    )

    assert abs(test.slope) < 0.005
    assert test.ci_low < 0 < test.ci_high
    assert test.p_value > 0.05


def test_between_route_differences_do_not_count():
    # Wetter routes are worse, but inside each route punctuality ignores rain.
    route = np.repeat(["DRY", "WET"], 4)
    dose = np.array([0.5, 0.6, 0.7, 0.8, 2.0, 2.1, 2.2, 2.3])
    share = np.array([0.95] * 4 + [0.75] * 4)
    run = np.full(8, 100.0)
    test = dose_response(
        "rain", share * run, run, dose, route, route, permutations=99, resamples=99
    )

    assert test.slope == pytest.approx(0.0, abs=1e-9)


def test_missing_intensity_rows_are_dropped_and_constant_dose_is_untestable():
    run = np.full(4, 100.0)
    on_time = np.array([90.0, 80.0, 85.0, 70.0])
    dose = np.array([np.nan, np.nan, 1.0, 1.0])
    strata = np.array(["a", "a", "b", "b"])
    test = dose_response("wind", on_time, run, dose, strata, strata, permutations=9, resamples=9)

    assert test.route_months == 2
    assert test.strata_used == 0
    assert math.isnan(test.slope)
    assert test.p_adjusted is None


def test_mismatched_inputs_are_rejected():
    with pytest.raises(ValueError):
        dose_response("rain", [1], [1, 2], [1], ["a"], ["a"])
    with pytest.raises(ValueError):
        dose_response("rain", [1], [1], [1], ["a"], ["a"], permutations=0)


def test_dose_sql_rejects_bad_schema():
    assert "marts.int_route_month_weather" in dose_sql()
    with pytest.raises(ValueError):
        dose_sql("marts; drop table x")


# route, service, month, coverage, run, on_time, rain ratio, wind days, frost days
ROWS = [
    ("PARIS-LYON", "National", "2024-01-01", "both", 600, 560, 0.8, 1, 2),
    ("PARIS-LYON", "National", "2024-02-01", "both", 580, 510, 1.9, 4, 9),
    ("PARIS-LYON", "National", "2024-12-01", "departure_only", 610, 530, 1.5, 3, None),
    ("LYON-MARSEILLE", "National", "2024-01-01", "both", 300, 280, 0.6, 2, 1),
    ("LYON-MARSEILLE", "National", "2024-02-01", "both", 310, 260, 2.2, 5, 6),
    ("LYON-MARSEILLE", "National", "2024-12-01", "both", 305, 270, 1.4, 2, 3),
    ("PARIS-GENEVE", "International", "2024-01-01", "none", 200, 150, None, None, None),
    ("PARIS-GENEVE", "International", "2024-02-01", "both", 0, None, 1.0, 1, 1),
]


@pytest.fixture
def marts(tmp_path):
    conn = sqlite3.connect(tmp_path / "marts.db")
    conn.execute(
        "create table int_route_month_weather (route_id text, service text, month text,"
        " weather_coverage text, trains_run integer, trains_on_time integer,"
        " max_precipitation_ratio real, max_strong_wind_days integer, max_frost_days integer)"
    )
    conn.executemany("insert into int_route_month_weather values (?,?,?,?,?,?,?,?,?)", ROWS)
    conn.execute("create table dim_month (month text, season text)")
    conn.executemany(
        "insert into dim_month values (?,?)",
        [("2024-01-01", "winter"), ("2024-02-01", "winter"), ("2024-12-01", "winter")],
    )
    yield conn
    conn.close()


def test_reader_keeps_the_weather_panel_population(marts):
    panel = read_dose_panel(marts, schema=None)

    assert len(panel) == 6
    assert set(panel.route_id.tolist()) == {"PARIS-LYON", "LYON-MARSEILLE"}
    assert np.isnan(panel.intensity["frost"]).sum() == 1
    # Ordered by route then month: Lyon-Marseille first.
    assert panel.intensity["rain"].tolist()[:3] == [0.6, 2.2, 1.4]


def test_analyse_dose_on_the_reader_output(marts):
    tests = analyse_dose(read_dose_panel(marts, schema=None), permutations=99, resamples=99)

    assert [t.factor for t in tests] == ["rain", "wind", "frost"]
    assert all(t.slope < 0 for t in tests)
    assert all(t.p_adjusted is not None for t in tests)
    assert tests[2].route_months == 5


def test_build_rejects_bad_rows():
    with pytest.raises(ValueError, match="columns"):
        build_dose_panel([("x",)])
    bad = ("R", "S", "winter", 10, 11, 1.0, 1, 1)
    assert len(bad) == len(DOSE_COLUMNS)
    with pytest.raises(ValueError, match="trains_on_time"):
        build_dose_panel([bad])
