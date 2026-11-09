"""Silver transform for the SNCF monthly TGV punctuality export (AQST).

Bronze holds the Opendatasoft JSON export as one array. Silver gives it:

* an explicit schema (no inference: a column that turns into a string in a future
  export must fail loudly, not silently change type),
* English snake_case names and a real `month` date (first day of the month),
* normalised station names (trimmed, upper-case, single spaces) and a stable
  `route_id` hashed from them, so Day 7 can join routes to departments,
* one row per (month, service, departure, arrival): the export occasionally
  repeats a route-month, and silver must stay idempotent under re-runs,
* derived `trains_run` and `arrival_punctuality_rate`.

Rows that cannot be true (more cancellations than planned trains, more late
arrivals than trains run, missing keys) are not dropped silently: they come back
in a separate `rejected` frame with a `reject_reason`, which the job writes to a
quarantine path and counts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

# bronze field -> (silver column, Spark type)
COLUMNS: dict[str, tuple[str, str]] = {
    "date": ("month_raw", "string"),
    "service": ("service", "string"),
    "gare_depart": ("departure_station", "string"),
    "gare_arrivee": ("arrival_station", "string"),
    "duree_moyenne": ("avg_journey_min", "double"),
    "nb_train_prevu": ("trains_planned", "long"),
    "nb_annulation": ("trains_cancelled", "long"),
    "nb_train_depart_retard": ("trains_late_departure", "long"),
    "retard_moyen_depart": ("avg_delay_late_departure_min", "double"),
    "retard_moyen_tous_trains_depart": ("avg_delay_all_departure_min", "double"),
    "nb_train_retard_arrivee": ("trains_late_arrival", "long"),
    "retard_moyen_arrivee": ("avg_delay_late_arrival_min", "double"),
    "retard_moyen_tous_trains_arrivee": ("avg_delay_all_arrival_min", "double"),
    "nb_train_retard_sup_15": ("trains_late_over_15", "long"),
    "retard_moyen_trains_retard_sup15": ("avg_delay_over_15_min", "double"),
    "nb_train_retard_sup_30": ("trains_late_over_30", "long"),
    "nb_train_retard_sup_60": ("trains_late_over_60", "long"),
    "prct_cause_externe": ("pct_cause_external", "double"),
    "prct_cause_infra": ("pct_cause_infrastructure", "double"),
    "prct_cause_gestion_trafic": ("pct_cause_traffic_management", "double"),
    "prct_cause_materiel_roulant": ("pct_cause_rolling_stock", "double"),
    "prct_cause_gestion_gare": ("pct_cause_station_management", "double"),
    "prct_cause_prise_en_charge_voyageurs": ("pct_cause_passenger_handling", "double"),
}

MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"

KEY = ("month", "service", "departure_station", "arrival_station")


class Split(NamedTuple):
    valid: DataFrame
    rejected: DataFrame


def bronze_schema():
    """Schema of the bronze JSON records (only the fields silver keeps)."""
    from pyspark.sql.types import StructField, StructType, _parse_datatype_string

    return StructType(
        [StructField(src, _parse_datatype_string(t), True) for src, (_, t) in COLUMNS.items()]
    )


def read_bronze(spark: SparkSession, path: str) -> DataFrame:
    """Read one bronze punctuality export (a JSON array) with the explicit schema."""
    return spark.read.schema(bronze_schema()).option("multiLine", "true").json(path)


def normalise_station(col: Column) -> Column:
    """`  paris  lyon ` -> `PARIS LYON`; empty strings become null."""
    from pyspark.sql import functions as F

    cleaned = F.upper(F.trim(F.regexp_replace(col, r"\s+", " ")))
    return F.when(cleaned == "", None).otherwise(cleaned)


def route_id(departure: Column, arrival: Column) -> Column:
    """Stable 16-hex-char id of a directed route, from normalised station names."""
    from pyspark.sql import functions as F

    return F.substring(F.sha2(F.concat_ws("|", departure, arrival), 256), 1, 16)


def _reject_reason() -> Column:
    from pyspark.sql import functions as F

    planned = F.col("trains_planned")
    cancelled = F.coalesce(F.col("trains_cancelled"), F.lit(0))
    return (
        F.when(F.col("month").isNull(), "bad_month")
        .when(F.col("service").isNull(), "missing_service")
        .when(
            F.col("departure_station").isNull() | F.col("arrival_station").isNull(),
            "missing_station",
        )
        .when(planned.isNull() | (planned <= 0), "no_planned_trains")
        .when(
            (cancelled < 0)
            | (F.col("trains_late_departure") < 0)
            | (F.col("trains_late_arrival") < 0),
            "negative_count",
        )
        .when(cancelled > planned, "cancelled_over_planned")
        .when(F.col("trains_late_arrival") > planned - cancelled, "late_over_run")
    )


def to_silver(bronze: DataFrame) -> Split:
    """Type, rename, clean and de-duplicate a bronze punctuality frame."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    renamed = bronze.select(*[F.col(src).alias(dst) for src, (dst, _) in COLUMNS.items()])
    typed = (
        renamed.withColumn(
            # Spark 4 runs in ANSI mode, where to_date raises on "2024-13": only
            # parse values shaped like a month, the rest become null (bad_month).
            "month",
            F.when(
                F.trim("month_raw").rlike(MONTH_PATTERN),
                F.to_date(F.concat(F.trim("month_raw"), F.lit("-01")), "yyyy-MM-dd"),
            ),
        )
        .withColumn("service", F.initcap(F.trim("service")))
        .withColumn("departure_station", normalise_station(F.col("departure_station")))
        .withColumn("arrival_station", normalise_station(F.col("arrival_station")))
        .withColumn("reject_reason", _reject_reason())
    )

    rejected = typed.where(F.col("reject_reason").isNotNull())
    good = typed.where(F.col("reject_reason").isNull()).drop("reject_reason", "month_raw")

    # Keep one row per key. Exact repeats are the common case; when two versions
    # differ, keep the one with the most planned trains (the more complete count).
    order = Window.partitionBy(*KEY).orderBy(
        F.col("trains_planned").desc(), F.col("trains_cancelled").asc_nulls_last()
    )
    deduped = (
        good.withColumn("_rn", F.row_number().over(order)).where(F.col("_rn") == 1).drop("_rn")
    )

    trains_run = F.col("trains_planned") - F.coalesce(F.col("trains_cancelled"), F.lit(0))
    valid = (
        deduped.withColumn(
            "route_id", route_id(F.col("departure_station"), F.col("arrival_station"))
        )
        .withColumn("trains_run", trains_run)
        .withColumn(
            "arrival_punctuality_rate",
            F.when(
                F.col("trains_run") > 0,
                F.round(
                    1 - F.coalesce(F.col("trains_late_arrival"), F.lit(0)) / F.col("trains_run"), 4
                ),
            ),
        )
        .withColumn("year", F.year("month"))
    )
    ordered = [
        "month",
        "year",
        "route_id",
        *[c for c in valid.columns if c not in {"month", "year", "route_id"}],
    ]
    return Split(valid=valid.select(*ordered), rejected=rejected)
