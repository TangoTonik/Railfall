"""Silver transform for the SNCF passenger stations export (`gares-de-voyageurs`).

Stations are the bridge between train routes (named by station) and weather
(keyed by department). Silver gives them:

* an explicit schema, `position_geographique` flattened into `latitude`/`longitude`,
* one row per UIC code: `codes_uic` may hold several codes separated by `;`
  (stations shared by two networks), each becomes its own row,
* the department derived from the INSEE commune code (`2A`/`2B` for Corsica,
  three digits for overseas departments), the key Day 7 joins weather on,
* `name_key`, an upper-case, accent-free, punctuation-free name used to match
  the station names of the punctuality export.

Rows without a usable UIC code or commune code are returned in `rejected`.
Stations without coordinates are kept: the department is enough for the join.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

UIC_PATTERN = r"^87\d{6}$"
INSEE_PATTERN = r"^(\d{5}|2[AB]\d{3})$"

ACCENTED = "ÀÂÄÇÉÈÊËÎÏÔÖÙÛÜŸ"
PLAIN = "AAACEEEEIIOOUUUY"


class Split(NamedTuple):
    valid: DataFrame
    rejected: DataFrame


def bronze_schema():
    """Schema of the bronze station records (only the fields silver keeps)."""
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    point = StructType([StructField("lon", DoubleType()), StructField("lat", DoubleType())])
    return StructType(
        [
            StructField("nom", StringType()),
            StructField("libellecourt", StringType()),
            StructField("segment_drg", StringType()),
            StructField("position_geographique", point),
            StructField("codeinsee", StringType()),
            StructField("codes_uic", StringType()),
        ]
    )


def read_bronze(spark: SparkSession, path: str) -> DataFrame:
    """Read one bronze stations export (a JSON array) with the explicit schema."""
    return spark.read.schema(bronze_schema()).option("multiLine", "true").json(path)


def name_key(col: Column) -> Column:
    """`Saint-Étienne Châteaucreux` -> `SAINT ETIENNE CHATEAUCREUX`; blank -> null."""
    from pyspark.sql import functions as F

    folded = F.translate(F.upper(col), ACCENTED, PLAIN)
    spaced = F.regexp_replace(folded, r"[^A-Z0-9]+", " ")
    cleaned = F.trim(spaced)
    return F.when(cleaned == "", None).otherwise(cleaned)


def department_from_insee(col: Column) -> Column:
    """Department code of an INSEE commune code: `75112`->`75`, `2A004`->`2A`, `97411`->`974`."""
    from pyspark.sql import functions as F

    return F.when(col.startswith("97"), F.substring(col, 1, 3)).otherwise(F.substring(col, 1, 2))


def to_silver(bronze: DataFrame) -> Split:
    """Type, flatten, explode and de-duplicate a bronze stations frame."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    exploded = bronze.select(
        F.trim("nom").alias("station_name"),
        F.upper(F.trim("libellecourt")).alias("short_code"),
        F.upper(F.trim("segment_drg")).alias("segment"),
        F.col("position_geographique.lat").alias("latitude"),
        F.col("position_geographique.lon").alias("longitude"),
        F.upper(F.trim("codeinsee")).alias("insee_code"),
        F.explode_outer(F.split(F.coalesce(F.col("codes_uic"), F.lit("")), ";")).alias("uic"),
    ).withColumn("uic_code", F.trim("uic"))

    reason = (
        F.when(F.col("station_name").isNull() | (F.col("station_name") == ""), "missing_name")
        .when(~F.col("uic_code").rlike(UIC_PATTERN), "bad_uic_code")
        .when(
            F.col("insee_code").isNull() | ~F.col("insee_code").rlike(INSEE_PATTERN),
            "bad_insee_code",
        )
    )
    flagged = exploded.drop("uic").withColumn("reject_reason", reason)
    rejected = flagged.where(F.col("reject_reason").isNotNull())
    good = flagged.where(F.col("reject_reason").isNull()).drop("reject_reason")

    # A UIC code listed twice: keep the record that has coordinates, then by name
    # so the choice does not depend on file order.
    order = Window.partitionBy("uic_code").orderBy(
        F.col("latitude").isNull().asc(), F.col("station_name").asc()
    )
    deduped = (
        good.withColumn("_rn", F.row_number().over(order)).where(F.col("_rn") == 1).drop("_rn")
    )

    lat_ok = F.col("latitude").between(-90.0, 90.0) & F.col("longitude").between(-180.0, 180.0)
    valid = deduped.select(
        "uic_code",
        "station_name",
        name_key(F.col("station_name")).alias("name_key"),
        "short_code",
        "segment",
        "insee_code",
        department_from_insee(F.col("insee_code")).alias("department"),
        F.when(lat_ok, F.col("latitude")).alias("latitude"),
        F.when(lat_ok, F.col("longitude")).alias("longitude"),
    )
    return Split(valid=valid, rejected=rejected)
