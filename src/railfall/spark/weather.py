"""Silver transform for the Météo-France daily climatological files (BASE/QUOT).

Bronze holds one `csv.gz` per department and period, byte-for-byte, under
`bronze/meteo/daily_<dept>_<period>/ingest_date=<day>/daily_<dept>_<period>.csv.gz`.
Silver turns them into one typed table of station-days:

* every column is read as a string first and cast explicitly: the files carry
  ~58 columns and Spark's CSV inference would guess `ALTI` or `HTN` differently
  from one department to the next,
* department, period and ingest date come from the bronze path, not from
  `NUM_POSTE` (Corsican stations still start with `20`),
* each measure has a Météo-France quality flag `Q<measure>`; a value flagged
  `2` ("doubtful, being checked") is nulled rather than trusted,
* physically impossible values (negative rain, TN > TX, wind outside 0-100 m/s)
  are nulled too, measure by measure, so one bad sensor does not cost the day,
* the `previous` and `latest` files overlap around January, and a department
  may have been ingested on several days: one row per (station, day) is kept,
  preferring `latest`, then the most recent ingest.

Rows whose key cannot be trusted (bad date, station id outside its department)
are returned in a separate `rejected` frame with a `reject_reason`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

# bronze column -> (silver column, Spark type)
COLUMNS: dict[str, tuple[str, str]] = {
    "NUM_POSTE": ("station_id", "string"),
    "NOM_USUEL": ("station_name", "string"),
    "LAT": ("latitude", "double"),
    "LON": ("longitude", "double"),
    "ALTI": ("altitude_m", "double"),
    "AAAAMMJJ": ("day_raw", "string"),
    "RR": ("precipitation_mm", "double"),
    "TN": ("temp_min_c", "double"),
    "TX": ("temp_max_c", "double"),
    "TM": ("temp_mean_c", "double"),
    "FFM": ("wind_mean_ms", "double"),
    "FXY": ("wind_max_hourly_ms", "double"),
}
MEASURES = ("RR", "TN", "TX", "TM", "FFM", "FXY")

# Quality codes (Météo-France): 0 protected, 1 validated, 2 doubtful being
# checked, 9 filtered (passed first-level checks, not yet validated).
DOUBTFUL_QUALITY = "2"

# Physical bounds, inclusive, applied to each measure on its own.
BOUNDS: dict[str, tuple[float, float]] = {
    "precipitation_mm": (0.0, 500.0),
    "temp_min_c": (-50.0, 50.0),
    "temp_max_c": (-50.0, 50.0),
    "temp_mean_c": (-50.0, 50.0),
    "wind_mean_ms": (0.0, 100.0),
    "wind_max_hourly_ms": (0.0, 100.0),
}

PATH_PATTERN = r"daily_(\d{2}|2[AB]|97\d)_(previous|latest)/ingest_date=(\d{4}-\d{2}-\d{2})/"
DAY_PATTERN = r"^(19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])$"

KEY = ("station_id", "day")


class Split(NamedTuple):
    valid: DataFrame
    rejected: DataFrame


def bronze_glob(root: str) -> str:
    """Every department/period/ingest day under a lake root (`s3a://bucket` or a dir)."""
    return f"{root.rstrip('/')}/bronze/meteo/daily_*/ingest_date=*/*.csv.gz"


def read_bronze(spark: SparkSession, path: str) -> DataFrame:
    """Read bronze department files as all-string columns plus their source path."""
    from pyspark.sql import functions as F

    frame = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .csv(path)
    )
    missing = [c for c in [*COLUMNS, *(f"Q{m}" for m in MEASURES)] if c not in frame.columns]
    if missing:
        raise ValueError(f"bronze weather files lack columns {missing}")
    return frame.withColumn("source_file", F.col("_metadata.file_path"))


def _blank_to_null(col: Column) -> Column:
    from pyspark.sql import functions as F

    trimmed = F.trim(col)
    return F.when(trimmed == "", None).otherwise(trimmed)


def _measure(src: str, dst: str) -> Column:
    """Cast one measure, dropping values flagged doubtful or outside physical bounds."""
    from pyspark.sql import functions as F

    value = _blank_to_null(F.col(src)).try_cast("double")
    low, high = BOUNDS[dst]
    flagged = F.coalesce(F.trim(F.col(f"Q{src}")) == DOUBTFUL_QUALITY, F.lit(False))
    return F.when(~flagged & value.between(low, high), value).alias(dst)


def _station_prefix(department: Column) -> Column:
    from pyspark.sql import functions as F

    return F.when(department.isin("2A", "2B"), F.lit("20")).otherwise(department)


def to_silver(bronze: DataFrame) -> Split:
    """Type, clean and de-duplicate a bronze weather frame (see module doc)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    measures = {dst for src, (dst, _) in COLUMNS.items() if src in MEASURES}
    selected = bronze.select(
        *[
            _measure(src, dst)
            if dst in measures
            else _blank_to_null(F.col(src)).try_cast(kind).alias(dst)
            for src, (dst, kind) in COLUMNS.items()
        ],
        F.regexp_extract("source_file", PATH_PATTERN, 1).alias("department"),
        F.regexp_extract("source_file", PATH_PATTERN, 2).alias("period"),
        F.to_date(F.regexp_extract("source_file", PATH_PATTERN, 3)).alias("ingest_date"),
    )

    # TN > TX means one of the two is wrong and we cannot tell which: drop both.
    inverted = F.col("temp_min_c") > F.col("temp_max_c")
    keep = F.coalesce(~inverted, F.lit(True))
    cleaned = selected.withColumns(
        {
            "temp_min_c": F.when(keep, F.col("temp_min_c")),
            "temp_max_c": F.when(keep, F.col("temp_max_c")),
        }
    ).withColumn(
        "day",
        F.when(F.col("day_raw").rlike(DAY_PATTERN), F.to_date("day_raw", "yyyyMMdd")),
    )

    prefix = _station_prefix(F.col("department"))
    reason = (
        F.when(F.col("department") == "", "unknown_source_path")
        .when(F.col("station_id").isNull() | ~F.col("station_id").rlike(r"^\d{8}$"), "bad_station")
        .when(~F.col("station_id").startswith(prefix), "station_outside_department")
        .when(F.col("day").isNull(), "bad_day")
    )
    flagged = cleaned.withColumn("reject_reason", reason)
    rejected = flagged.where(F.col("reject_reason").isNotNull())
    good = flagged.where(F.col("reject_reason").isNull()).drop("reject_reason", "day_raw")

    order = Window.partitionBy(*KEY).orderBy(
        F.when(F.col("period") == "latest", 0).otherwise(1), F.col("ingest_date").desc()
    )
    deduped = (
        good.withColumn("_rn", F.row_number().over(order)).where(F.col("_rn") == 1).drop("_rn")
    )
    valid = deduped.withColumn("year", F.year("day")).withColumn("month", F.trunc("day", "month"))
    ordered = ["department", "station_id", "day", "year", "month"]
    return Split(
        valid=valid.select(*ordered, *[c for c in valid.columns if c not in ordered]),
        rejected=rejected,
    )
