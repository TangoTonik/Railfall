"""Expectation suites for the `raw` warehouse tables, declared as plain data.

dbt tests guard the models dbt builds (staging, marts); these suites guard the
layer *underneath*, right after the Spark loader merged silver into Postgres, so
the DAG can stop before dbt ever reads a broken load. They complement rather
than repeat the silver verifier: that one checks the Parquet contract (schema,
key uniqueness), these check the *values* once they are in the warehouse
(ranges, counters that must reconcile, codes that must look like codes).

Every suite is a tuple of `Expectation(kind, kwargs)`, where `kind` is the
class name in `great_expectations.expectations`. Keeping the spec free of any
GX import means it is tested (column names against the silver contracts, kinds
against GX) without building a context, and reads like a data contract.

Bounds are deliberately physical rather than statistical: a temperature of
-60 °C is a parsing bug, an unusually wet month is the point of the project.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from railfall.spark.route_departments import METHODS
from railfall.spark.stations import UIC_PATTERN
from railfall.spark.weather import BOUNDS

RAW_SCHEMA = "raw"

# INSEE department codes: metropolitan 01-95, Corsica 2A/2B, overseas 971-976.
DEPARTMENT_REGEX = r"^(\d{2}|2[AB]|97\d)$"
# Metropolitan France plus Corsica, with a margin; SNCF lines abroad stop at the border.
LATITUDE = (41.0, 51.5)
LONGITUDE = (-5.5, 10.0)
DAYS_IN_MONTH = (0, 31)


@dataclass(frozen=True)
class Expectation:
    """One GX expectation: its class name in `gx.expectations` and its arguments."""

    kind: str
    kwargs: Mapping[str, Any] = field(default_factory=dict)

    @property
    def columns(self) -> tuple[str, ...]:
        """Every column this expectation reads (for checking against the contract)."""
        out: list[str] = []
        for arg in ("column", "column_A", "column_B"):
            if arg in self.kwargs:
                out.append(self.kwargs[arg])
        for arg in ("column_list", "column_set"):
            out.extend(self.kwargs.get(arg, ()))
        return tuple(out)


def _e(kind: str, **kwargs: Any) -> Expectation:
    return Expectation(kind, kwargs)


def not_empty() -> Expectation:
    return _e("ExpectTableRowCountToBeBetween", min_value=1)


def not_null(column: str) -> Expectation:
    return _e("ExpectColumnValuesToNotBeNull", column=column)


def unique_key(*columns: str) -> Expectation:
    if len(columns) == 1:
        return _e("ExpectColumnValuesToBeUnique", column=columns[0])
    return _e("ExpectCompoundColumnsToBeUnique", column_list=list(columns))


def between(column: str, low: float, high: float, *, mostly: float = 1.0) -> Expectation:
    kwargs: dict[str, Any] = {"column": column, "min_value": low, "max_value": high}
    if mostly < 1.0:
        kwargs["mostly"] = mostly
    return _e("ExpectColumnValuesToBeBetween", **kwargs)


def at_most(column: str, ceiling: str) -> Expectation:
    """`column <= ceiling` on every row where both are set."""
    return _e(
        "ExpectColumnPairValuesAToBeGreaterThanB",
        column_A=ceiling,
        column_B=column,
        or_equal=True,
        ignore_row_if="either_value_is_missing",
    )


def department(column: str) -> Expectation:
    return _e("ExpectColumnValuesToMatchRegex", column=column, regex=DEPARTMENT_REGEX)


def has_columns(*columns: str) -> Expectation:
    """The table carries at least these columns (extra ones, e.g. audit, are fine)."""
    return _e("ExpectTableColumnsToMatchSet", column_set=list(columns), exact_match=False)


def _weather_bounds(*columns: str) -> tuple[Expectation, ...]:
    return tuple(between(c, *BOUNDS[c]) for c in columns)


SUITES: dict[str, tuple[Expectation, ...]] = {
    "punctuality_monthly": (
        not_empty(),
        has_columns("month", "route_id", "service", "trains_planned", "trains_run"),
        *(not_null(c) for c in ("month", "route_id", "service")),
        unique_key("month", "service", "departure_station", "arrival_station"),
        between("arrival_punctuality_rate", 0.0, 1.0),
        between("pct_cause_external", 0.0, 100.0),
        between("trains_planned", 0, 100_000),
        at_most("trains_cancelled", "trains_planned"),
        at_most("trains_run", "trains_planned"),
        at_most("trains_late_arrival", "trains_run"),
        between("year", 2010, 2100),
    ),
    "weather_daily": (
        not_empty(),
        *(not_null(c) for c in ("department", "station_id", "day")),
        unique_key("station_id", "day"),
        department("department"),
        *_weather_bounds("precipitation_mm", "temp_min_c", "temp_max_c", "wind_mean_ms"),
        at_most("temp_min_c", "temp_max_c"),
        between("latitude", -22.0, 52.0),
    ),
    "stations": (
        not_empty(),
        not_null("uic_code"),
        unique_key("uic_code"),
        _e("ExpectColumnValuesToMatchRegex", column="uic_code", regex=UIC_PATTERN),
        department("department"),
        # Silver rejects foreign UIC codes, but a few French-run stations sit
        # just across a border (Geneva, Basel), so the box is "mostly".
        between("latitude", *LATITUDE, mostly=0.99),
        between("longitude", *LONGITUDE, mostly=0.99),
    ),
    "weather_monthly": (
        not_empty(),
        *(not_null(c) for c in ("department", "month")),
        unique_key("department", "month"),
        department("department"),
        between("stations", 1, 1_000),
        *(between(c, *DAYS_IN_MONTH) for c in ("rain_days", "heavy_rain_days", "frost_days")),
        at_most("heavy_rain_days", "rain_days"),
        _e("ExpectColumnValuesToBeBetween", column="precipitation_mm", min_value=0.0),
        *_weather_bounds("temp_mean_c", "temp_min_c", "wind_mean_ms"),
        at_most("temp_min_c", "temp_mean_c"),
        at_most("wind_mean_ms", "wind_max_ms"),
    ),
    "route_departments": (
        not_empty(),
        not_null("route_id"),
        unique_key("route_id"),
        department("departure_department"),
        department("arrival_department"),
        *(
            _e("ExpectColumnValuesToBeInSet", column=c, value_set=list(METHODS))
            for c in ("departure_match", "arrival_match")
        ),
    ),
}


def suite_name(table: str, schema: str = RAW_SCHEMA) -> str:
    return f"{schema}.{table}"
