"""Monthly weather by department, built from the silver `weather_daily` table.

Punctuality is published per route and month, so weather has to meet it at the
same grain. The roll-up is done in two steps:

1. **station-month**: each station's month is summarised on its own (total
   rain, rain / heavy-rain / frost / strong-wind days, mean temperature and
   wind). Counts are scaled to the length of the month from the days the
   station actually reported, so a station that missed three days is not
   "drier" than its neighbours. A station-month only counts for a measure when
   it covers at least `MIN_COVERAGE` of the month: below that, scaling would
   amplify noise, and the month still in progress drops out by itself.
2. **department-month**: the covered station-months are averaged (maximum for
   the strongest wind). Each measure family carries how many stations it was
   computed from, so a department-month resting on one rain gauge is visible
   downstream instead of looking as solid as Paris.

Thresholds follow Météo-France climatology conventions: a rain day has at
least 1 mm, a heavy-rain day at least 10 mm, a frost day TN below 0 °C. Strong
wind uses FXY (max hourly mean wind) at 10.8 m/s, the lower bound of Beaufort 6
"strong breeze", because gusts are not in the files silver keeps.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

RAIN_DAY_MM = 1.0
HEAVY_RAIN_DAY_MM = 10.0
FROST_C = 0.0
STRONG_WIND_MS = 10.8
MIN_COVERAGE = 0.8

KEY = ("department", "month")


def _days_in_month(month: Column) -> Column:
    from pyspark.sql import functions as F

    return F.dayofmonth(F.last_day(month))


def _count_if(condition: Column) -> Column:
    from pyspark.sql import functions as F

    return F.sum(F.when(condition, 1).otherwise(0))


def station_months(daily: DataFrame) -> DataFrame:
    """One row per (department, station, month) with coverage-aware measures."""
    from pyspark.sql import functions as F

    rain = F.col("precipitation_mm")
    tmin, tmax = F.col("temp_min_c"), F.col("temp_max_c")
    # TM is missing at many automatic stations; (TN + TX) / 2 is the usual stand-in.
    tmean = F.coalesce(F.col("temp_mean_c"), (tmin + tmax) / 2)
    wind_max = F.col("wind_max_hourly_ms")

    grouped = daily.groupBy("department", "station_id", "month").agg(
        F.count(rain).alias("rain_obs"),
        F.sum(rain).alias("rain_sum"),
        _count_if(rain >= RAIN_DAY_MM).alias("rain_days_raw"),
        _count_if(rain >= HEAVY_RAIN_DAY_MM).alias("heavy_rain_days_raw"),
        F.count(tmean).alias("temp_obs"),
        F.avg(tmean).alias("temp_mean_c"),
        F.count(tmin).alias("frost_obs"),
        _count_if(tmin < FROST_C).alias("frost_days_raw"),
        F.min(tmin).alias("temp_min_c"),
        F.count(F.col("wind_mean_ms")).alias("wind_obs"),
        F.avg("wind_mean_ms").alias("wind_mean_ms"),
        F.count(wind_max).alias("wind_max_obs"),
        F.max(wind_max).alias("wind_max_ms"),
        _count_if(wind_max >= STRONG_WIND_MS).alias("strong_wind_days_raw"),
    )

    days = _days_in_month(F.col("month"))

    def covered(obs: str) -> Column:
        return F.col(obs) >= F.ceil(days * F.lit(MIN_COVERAGE))

    def scaled(raw: str, obs: str) -> Column:
        """Count over the observed days, extrapolated to the whole month."""
        return F.when(covered(obs), F.col(raw) * days / F.col(obs))

    return grouped.select(
        "department",
        "station_id",
        "month",
        days.alias("days_in_month"),
        scaled("rain_sum", "rain_obs").alias("precipitation_mm"),
        scaled("rain_days_raw", "rain_obs").alias("rain_days"),
        scaled("heavy_rain_days_raw", "rain_obs").alias("heavy_rain_days"),
        F.when(covered("temp_obs"), F.col("temp_mean_c")).alias("temp_mean_c"),
        F.when(covered("frost_obs"), F.col("temp_min_c")).alias("temp_min_c"),
        scaled("frost_days_raw", "frost_obs").alias("frost_days"),
        F.when(covered("wind_obs"), F.col("wind_mean_ms")).alias("wind_mean_ms"),
        F.when(covered("wind_max_obs"), F.col("wind_max_ms")).alias("wind_max_ms"),
        scaled("strong_wind_days_raw", "wind_max_obs").alias("strong_wind_days"),
    )


def department_months(daily: DataFrame) -> DataFrame:
    """Silver `weather_monthly`: one row per (department, month), see module doc."""
    from pyspark.sql import functions as F

    stations = station_months(daily)

    def mean(col: str, digits: int = 1) -> Column:
        return F.round(F.avg(col), digits).alias(col)

    monthly = stations.groupBy(*KEY).agg(
        F.countDistinct("station_id").alias("stations"),
        F.count("precipitation_mm").alias("stations_rain"),
        mean("precipitation_mm"),
        mean("rain_days"),
        mean("heavy_rain_days"),
        F.count("temp_mean_c").alias("stations_temp"),
        mean("temp_mean_c"),
        F.round(F.min("temp_min_c"), 1).alias("temp_min_c"),
        mean("frost_days"),
        F.count("wind_max_ms").alias("stations_wind"),
        mean("wind_mean_ms"),
        F.round(F.max("wind_max_ms"), 1).alias("wind_max_ms"),
        mean("strong_wind_days"),
    )
    # A department-month where no station covered any measure carries no information.
    informative = (
        (F.col("stations_rain") > 0) | (F.col("stations_temp") > 0) | (F.col("stations_wind") > 0)
    )
    return (
        monthly.where(informative)
        .withColumn("year", F.year("month"))
        .select("department", "month", "year", *[c for c in monthly.columns if c not in KEY])
    )
