"""Write silver tables and their quarantine to the lake as Parquet.

Layout under a lake root (`s3a://<bucket>` in the stack, a local dir in tests):

    silver/<table>/<partition>=<value>/part-*.parquet
    silver/_rejected/<table>/run_date=YYYY-MM-DD/part-*.parquet

Idempotency comes from overwrite semantics, not from bookkeeping: the session
runs with `partitionOverwriteMode=dynamic`, so a run replaces exactly the
partitions it produces and leaves the others alone. Re-running a day therefore
rewrites the same files instead of appending a second copy. Rejected rows are
overwritten per run date for the same reason.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# Spark type of every partition column used by a silver table.
PARTITION_TYPES: dict[str, str] = {"year": "int", "department": "string"}


@dataclass(frozen=True)
class Table:
    name: str
    partition_by: tuple[str, ...] = ()


PUNCTUALITY = Table("punctuality_monthly", ("year",))
WEATHER = Table("weather_daily", ("department", "year"))
STATIONS = Table("stations")

# Derived tables: built from other silver tables, not from bronze.
WEATHER_MONTHLY = Table("weather_monthly", ("year",))
ROUTE_DEPARTMENTS = Table("route_departments")
ROUTE_WEATHER = Table("route_weather_monthly", ("year",))

BASE: tuple[Table, ...] = (PUNCTUALITY, WEATHER, STATIONS)
DERIVED: tuple[Table, ...] = (WEATHER_MONTHLY, ROUTE_DEPARTMENTS, ROUTE_WEATHER)

# Dependency order: a table only reads tables listed before it.
TABLES: dict[str, Table] = {t.name: t for t in (*BASE, *DERIVED)}


@dataclass(frozen=True)
class WriteResult:
    table: str
    path: str
    rows: int
    rejected: int
    rejected_path: str | None


def silver_path(root: str, table: Table) -> str:
    return f"{root.rstrip('/')}/silver/{table.name}"


def rejected_path(root: str, table: Table, run_date: date) -> str:
    return f"{root.rstrip('/')}/silver/_rejected/{table.name}/run_date={run_date.isoformat()}"


def read_silver(spark: SparkSession, root: str, table: Table) -> DataFrame:
    """Read a silver table back with its partition columns properly typed.

    Spark's partition discovery guesses types from directory names, which turns
    `department=01` into the integer 1 and `department=2A` into a string. The
    table is read with inference off (every partition value stays a string) and
    each partition column is then cast to its declared type.
    """
    key = "spark.sql.sources.partitionColumnTypeInference.enabled"
    previous = spark.conf.get(key, "true")
    spark.conf.set(key, "false")
    try:
        df = spark.read.parquet(silver_path(root, table))
        # Bind the schema now, while inference is still off.
        df.schema  # noqa: B018
    finally:
        spark.conf.set(key, previous)
    for column in table.partition_by:
        df = df.withColumn(column, df[column].cast(PARTITION_TYPES[column]))
    return df


def write_silver(
    valid: DataFrame,
    rejected: DataFrame | None,
    *,
    root: str,
    table: Table,
    run_date: date,
) -> WriteResult:
    """Overwrite the table's partitions with `valid` and quarantine `rejected`.

    Derived tables have no quarantine and pass `rejected=None`.

    Both frames are cached first: each is counted and written, and recomputing
    the bronze read twice would double the S3 traffic.
    """
    valid = valid.cache()
    rejected = rejected.cache() if rejected is not None else None
    try:
        rows = valid.count()
        n_rejected = rejected.count() if rejected is not None else 0

        path = silver_path(root, table)
        writer = valid.write.mode("overwrite")
        if table.partition_by:
            writer = writer.partitionBy(*table.partition_by)
        writer.parquet(path)

        bad_path = None
        if n_rejected:
            bad_path = rejected_path(root, table, run_date)
            rejected.coalesce(1).write.mode("overwrite").parquet(bad_path)
        return WriteResult(table.name, path, rows, n_rejected, bad_path)
    finally:
        valid.unpersist()
        if rejected is not None:
            rejected.unpersist()
