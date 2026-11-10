"""Unit tests for the joined route x weather silver table (local Spark)."""

from __future__ import annotations

from datetime import date

import pytest

from railfall.spark import route_weather as rw

pytestmark = pytest.mark.spark

JAN, FEB = date(2025, 1, 1), date(2025, 2, 1)

PUNCT_SCHEMA = (
    "month date, year int, service string, route_id string, departure_station string, "
    "arrival_station string, trains_planned long, trains_cancelled long, trains_run long, "
    "trains_late_arrival long, trains_late_over_15 long, trains_late_over_30 long, "
    "avg_delay_all_arrival_min double, arrival_punctuality_rate double, "
    "pct_cause_external double, pct_cause_infrastructure double"
)
ROUTES_SCHEMA = (
    "route_id string, departure_station string, arrival_station string, "
    "departure_department string, arrival_department string, departure_match string, "
    "arrival_match string, is_domestic boolean"
)
WEATHER_SCHEMA = (
    "department string, month date, year int, stations long, precipitation_mm double, "
    "rain_days double, heavy_rain_days double, temp_mean_c double, temp_min_c double, "
    "frost_days double, wind_mean_ms double, wind_max_ms double, strong_wind_days double"
)


def _punct(month: date, route: str, dep: str, arr: str, late: int = 20) -> tuple:
    return (
        month,
        month.year,
        "National",
        route,
        dep,
        arr,
        300,
        10,
        290,
        late,
        8,
        3,
        4.2,
        round(1 - late / 290, 4),
        30.0,
        20.0,
    )


def _weather(dept: str, month: date, rain: float, tmin: float, wind_max: float) -> tuple:
    return (
        dept,
        month,
        month.year,
        3,
        rain,
        rain / 5,
        rain / 30,
        6.0,
        tmin,
        4.0,
        3.5,
        wind_max,
        1.0,
    )


@pytest.fixture
def frames(spark):
    punct = spark.createDataFrame(
        [
            _punct(JAN, "r_pm", "PARIS LYON", "MARSEILLE ST CHARLES"),
            _punct(FEB, "r_pm", "PARIS LYON", "MARSEILLE ST CHARLES", late=40),
            _punct(JAN, "r_pg", "PARIS LYON", "GENEVE"),
            _punct(JAN, "r_pb", "PARIS MONTPARNASSE", "BORDEAUX ST JEAN"),
        ],
        PUNCT_SCHEMA,
    )
    routes = spark.createDataFrame(
        [
            ("r_pm", "PARIS LYON", "MARSEILLE ST CHARLES", "75", "13", "exact", "exact", True),
            ("r_pg", "PARIS LYON", "GENEVE", "75", None, "exact", "foreign", False),
            ("r_pb", "PARIS MONTPARNASSE", "BORDEAUX ST JEAN", "75", "33", "exact", "exact", True),
        ],
        ROUTES_SCHEMA,
    )
    weather = spark.createDataFrame(
        [
            _weather("75", JAN, 40.0, -3.0, 12.0),
            _weather("13", JAN, 80.0, 1.0, 18.0),
            _weather("75", FEB, 20.0, -1.0, 9.0),
            # 13 has no February row and 33 none at all: partial / missing weather.
        ],
        WEATHER_SCHEMA,
    )
    return punct, routes, weather


def _by_key(df):
    return {(r.route_id, r.month): r for r in df.collect()}


def test_keeps_the_punctuality_grain(frames):
    punct, routes, weather = frames
    out = rw.route_weather(punct, routes, weather)
    assert out.count() == punct.count()
    assert out.select(*rw.KEY).distinct().count() == punct.count()


def test_route_values_combine_both_endpoints(frames):
    out = _by_key(rw.route_weather(*frames))
    row = out[("r_pm", JAN)]
    assert row.weather_endpoints == 2
    assert (row.dep_precipitation_mm, row.arr_precipitation_mm) == (40.0, 80.0)
    assert row.route_precipitation_mm == 60.0
    # Extremes stay extremes.
    assert row.route_wind_max_ms == 18.0
    assert row.route_temp_min_c == -3.0


def test_one_endpoint_falls_back_to_that_endpoint(frames):
    out = _by_key(rw.route_weather(*frames))
    feb = out[("r_pm", FEB)]
    assert feb.weather_endpoints == 1
    assert feb.arr_precipitation_mm is None
    assert feb.route_precipitation_mm == 20.0
    assert feb.route_wind_max_ms == 9.0

    geneva = out[("r_pg", JAN)]
    assert geneva.is_domestic is False
    assert geneva.arrival_department is None
    assert geneva.weather_endpoints == 1
    assert geneva.route_precipitation_mm == 40.0


def test_route_without_weather_is_kept_with_nulls(frames):
    out = _by_key(rw.route_weather(*frames))
    row = out[("r_pb", JAN)]
    # Departure (75) is covered in January, Bordeaux (33) is not.
    assert row.weather_endpoints == 1
    assert row.arr_weather_stations is None
    assert row.trains_planned == 300


def test_route_missing_from_mapping_keeps_its_row(spark, frames):
    punct, routes, weather = frames
    out = rw.route_weather(punct, routes.where("route_id != 'r_pb'"), weather)
    row = _by_key(out)[("r_pb", JAN)]
    assert row.departure_department is None
    assert row.weather_endpoints == 0
    assert row.route_precipitation_mm is None


def test_column_layout(frames):
    cols = rw.route_weather(*frames).columns
    assert cols[: len(rw.PUNCTUALITY_COLUMNS)] == list(rw.PUNCTUALITY_COLUMNS)
    for prefix in ("route", "dep", "arr"):
        assert all(f"{prefix}_{m}" in cols for m in rw.WEATHER_MEASURES)
    assert len(cols) == len(set(cols))
