"""Unit tests for the department-month weather roll-up (local Spark)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from railfall.spark import weather_monthly as wm

pytestmark = pytest.mark.spark

SCHEMA = (
    "department string, station_id string, day date, month date, "
    "precipitation_mm double, temp_min_c double, temp_max_c double, temp_mean_c double, "
    "wind_mean_ms double, wind_max_hourly_ms double"
)


def _days(
    station: str,
    first: date,
    n: int,
    *,
    department: str = "75",
    rain: float | None = 0.0,
    tmin: float | None = 5.0,
    tmax: float | None = 12.0,
    tmean: float | None = 8.0,
    wind: float | None = 3.0,
    wind_max: float | None = 6.0,
    overrides: dict[int, dict] | None = None,
) -> list[tuple]:
    """`n` consecutive station-days; `overrides={day_index: {field: value}}` patches some."""
    patches = overrides or {}
    rows = []
    for i in range(n):
        day = first + timedelta(days=i)
        values = {
            "rain": rain,
            "tmin": tmin,
            "tmax": tmax,
            "tmean": tmean,
            "wind": wind,
            "wind_max": wind_max,
            **patches.get(i, {}),
        }
        rows.append(
            (
                department,
                station,
                day,
                day.replace(day=1),
                values["rain"],
                values["tmin"],
                values["tmax"],
                values["tmean"],
                values["wind"],
                values["wind_max"],
            )
        )
    return rows


def _monthly(spark, rows: list[tuple]) -> dict[tuple[str, date], object]:
    frame = spark.createDataFrame(rows, SCHEMA)
    return {(r.department, r.month): r for r in wm.department_months(frame).collect()}


def test_counts_and_totals_for_a_fully_covered_month(spark) -> None:
    patches = {
        0: {"rain": 4.0, "tmin": -2.0},
        1: {"rain": 12.0, "tmin": -0.5},
        2: {"rain": 0.6, "wind_max": 11.0},
        3: {"rain": 1.0, "wind_max": 14.2},
    }
    rows = _days("75114001", date(2025, 1, 1), 31, overrides=patches)

    row = _monthly(spark, rows)[("75", date(2025, 1, 1))]

    assert row.year == 2025
    assert (row.stations, row.stations_rain, row.stations_temp, row.stations_wind) == (1, 1, 1, 1)
    assert row.precipitation_mm == 17.6
    # 0.6 mm is a trace, not a rain day; 1.0 mm exactly is.
    assert (row.rain_days, row.heavy_rain_days) == (3.0, 1.0)
    assert (row.frost_days, row.temp_min_c) == (2.0, -2.0)
    assert (row.strong_wind_days, row.wind_max_ms) == (2.0, 14.2)
    assert row.temp_mean_c == 8.0


def test_counts_are_scaled_to_the_month_from_observed_days(spark) -> None:
    # 28 of 31 days reported (90 %), 4 of them rainy -> 4 * 31 / 28.
    patches = {i: {"rain": 2.0} for i in range(4)}
    rows = _days("75114001", date(2025, 3, 1), 28, overrides=patches)

    row = _monthly(spark, rows)[("75", date(2025, 3, 1))]

    assert row.rain_days == round(4 * 31 / 28, 1)
    assert row.precipitation_mm == round(8 * 31 / 28, 1)


def test_poorly_covered_measures_drop_out_but_others_stay(spark) -> None:
    # Full temperature record, rain gauge out for half of April.
    patches = {i: {"rain": None} for i in range(15)}
    rows = _days("75114001", date(2025, 4, 1), 30, rain=3.0, overrides=patches)

    row = _monthly(spark, rows)[("75", date(2025, 4, 1))]

    assert (row.stations, row.stations_rain, row.stations_temp) == (1, 0, 1)
    assert row.precipitation_mm is None and row.rain_days is None
    assert row.temp_mean_c == 8.0


def test_month_in_progress_disappears(spark) -> None:
    rows = _days("75114001", date(2026, 11, 1), 9)

    assert _monthly(spark, rows) == {}


def test_stations_are_averaged_within_a_department_not_across(spark) -> None:
    rows = [
        *_days("75114001", date(2025, 2, 1), 28, rain=1.0, wind_max=12.0),
        *_days("75107005", date(2025, 2, 1), 28, rain=3.0, tmin=None, tmax=None, tmean=None),
        *_days("13055001", date(2025, 2, 1), 28, department="13", rain=0.0, tmin=-1.0),
    ]

    monthly = _monthly(spark, rows)

    paris = monthly[("75", date(2025, 2, 1))]
    assert (paris.stations, paris.stations_rain, paris.stations_temp) == (2, 2, 1)
    assert paris.precipitation_mm == (28 + 84) / 2
    assert paris.rain_days == 28.0
    # Max across stations, not an average of maxima.
    assert paris.wind_max_ms == 12.0
    assert paris.strong_wind_days == 14.0

    marseille = monthly[("13", date(2025, 2, 1))]
    assert (marseille.rain_days, marseille.frost_days) == (0.0, 28.0)


def test_tm_falls_back_to_mid_range_when_missing(spark) -> None:
    rows = _days("75114001", date(2025, 6, 1), 30, tmin=14.0, tmax=26.0, tmean=None)

    assert _monthly(spark, rows)[("75", date(2025, 6, 1))].temp_mean_c == 20.0


def test_one_row_per_department_month(spark) -> None:
    rows = [
        *_days("75114001", date(2025, 1, 1), 59),
        *_days("75107005", date(2025, 1, 1), 59),
    ]
    frame = spark.createDataFrame(rows, SCHEMA)
    result = wm.department_months(frame)

    assert result.count() == 2
    assert result.select(*wm.KEY).distinct().count() == 2
