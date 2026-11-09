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
    from pyspark.sql import DataFrame


@dataclass(frozen=True)
class Table:
    name: str
    partition_by: tuple[str, ...] = ()


PUNCTUALITY = Table("punctuality_monthly", ("year",))
WEATHER = Table("weather_daily", ("department", "year"))
STATIONS = Table("stations")

TABLES: dict[str, Table] = {t.name: t for t in (PUNCTUALITY, WEATHER, STATIONS)}


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


def write_silver(
    valid: DataFrame,
    rejected: DataFrame,
    *,
    root: str,
    table: Table,
    run_date: date,
) -> WriteResult:
    """Overwrite the table's partitions with `valid` and quarantine `rejected`.

    Both frames are cached first: each is counted and written, and recomputing
    the bronze read twice would double the S3 traffic.
    """
    valid = valid.cache()
    rejected = rejected.cache()
    try:
        rows = valid.count()
        n_rejected = rejected.count()

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
        rejected.unpersist()
