"""Joined silver table: TGV punctuality per route-month with the weather at both ends.

Grain: one row per (month, service, route_id), exactly the grain of
`punctuality_monthly`. Every join is a left join on a key that is unique on the
right-hand side (`route_id` in the route mapping, `(department, month)` in
`weather_monthly`), so the table can neither lose nor duplicate a punctuality
row. Routes that cannot carry weather (a foreign or unmatched endpoint, or a
month Météo-France did not cover) keep null weather and say so through
`weather_endpoints`, instead of vanishing from the analysis.

Weather columns come twice, `dep_*` for the departure department and `arr_*` for
the arrival one. A route-level `route_*` value combines them: the mean of the
endpoints that have the measure, falling back to the one that does (extremes
such as the strongest wind stay extremes: max / min of the two ends). A TGV spends
most of its journey between its endpoints, so the two ends are a proxy for the
line, not a measurement of it; the per-endpoint columns stay so downstream
models can choose another combination.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

# Punctuality columns carried into the joined table.
PUNCTUALITY_COLUMNS: tuple[str, ...] = (
    "month",
    "year",
    "service",
    "route_id",
    "departure_station",
    "arrival_station",
    "trains_planned",
    "trains_cancelled",
    "trains_run",
    "trains_late_arrival",
    "trains_late_over_15",
    "trains_late_over_30",
    "avg_delay_all_arrival_min",
    "arrival_punctuality_rate",
    "pct_cause_external",
    "pct_cause_infrastructure",
)

ROUTE_COLUMNS: tuple[str, ...] = (
    "departure_department",
    "arrival_department",
    "is_domestic",
)

# weather_monthly measures copied once per endpoint.
WEATHER_MEASURES: tuple[str, ...] = (
    "precipitation_mm",
    "rain_days",
    "heavy_rain_days",
    "temp_mean_c",
    "temp_min_c",
    "frost_days",
    "wind_mean_ms",
    "wind_max_ms",
    "strong_wind_days",
)

ENDPOINTS: dict[str, str] = {"dep": "departure_department", "arr": "arrival_department"}

KEY = ("month", "service", "route_id")


def _endpoint_weather(weather: DataFrame, prefix: str) -> DataFrame:
    """weather_monthly renamed for one endpoint: `<prefix>_department`, `<prefix>_<measure>`."""
    from pyspark.sql import functions as F

    return weather.select(
        F.col("department").alias(f"{prefix}_department"),
        F.col("month").alias(f"{prefix}_month"),
        F.col("stations").alias(f"{prefix}_weather_stations"),
        *[F.col(m).alias(f"{prefix}_{m}") for m in WEATHER_MEASURES],
    )


# Extremes combine as extremes: the route saw the strongest wind and the coldest
# night of either end. Everything else is averaged.
EXTREMES: dict[str, str] = {"wind_max_ms": "max", "temp_min_c": "min"}


def _combine(measure: str, dep: Column, arr: Column) -> Column:
    """Route value of `measure` from its two endpoint values (nulls ignored)."""
    from pyspark.sql import functions as F

    how = EXTREMES.get(measure, "mean")
    if how == "max":
        value = F.greatest(dep, arr)
    elif how == "min":
        value = F.least(dep, arr)
    else:
        both = dep.isNotNull() & arr.isNotNull()
        value = F.when(both, (dep + arr) / 2).otherwise(F.coalesce(dep, arr))
    return F.round(value, 1)


def route_weather(punctuality: DataFrame, routes: DataFrame, weather: DataFrame) -> DataFrame:
    """Build silver `route_weather_monthly` from the three silver inputs (see module doc).

    `punctuality` is `punctuality_monthly`, `routes` the output of
    `route_departments.route_departments`, `weather` is `weather_monthly`.
    """
    from pyspark.sql import functions as F

    base = punctuality.select(*PUNCTUALITY_COLUMNS).join(
        routes.select("route_id", *ROUTE_COLUMNS), "route_id", "left"
    )
    out = base
    for prefix, dept_col in ENDPOINTS.items():
        side = _endpoint_weather(weather, prefix)
        cond = (out[dept_col] == side[f"{prefix}_department"]) & (
            out["month"] == side[f"{prefix}_month"]
        )
        out = out.join(side, cond, "left").drop(f"{prefix}_department", f"{prefix}_month")

    has_dep = F.col("dep_weather_stations").isNotNull()
    has_arr = F.col("arr_weather_stations").isNotNull()
    endpoints = F.when(has_dep, 1).otherwise(0) + F.when(has_arr, 1).otherwise(0)

    route_cols = [
        _combine(m, F.col(f"dep_{m}"), F.col(f"arr_{m}")).alias(f"route_{m}")
        for m in WEATHER_MEASURES
    ]
    dep_cols = ["dep_weather_stations", *[f"dep_{m}" for m in WEATHER_MEASURES]]
    arr_cols = ["arr_weather_stations", *[f"arr_{m}" for m in WEATHER_MEASURES]]
    return out.select(
        *PUNCTUALITY_COLUMNS,
        *ROUTE_COLUMNS,
        endpoints.cast("int").alias("weather_endpoints"),
        *route_cols,
        *dep_cols,
        *arr_cols,
    )
