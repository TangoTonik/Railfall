"""Load job: read silver Parquet with Spark and merge it into the warehouse `raw` schema.

    python -m railfall.load.job                         # every silver table, full refresh
    python -m railfall.load.job --year 2025             # only the 2025 partitions
    python -m railfall.load.job --table stations        # one table (repeatable)
    python -m railfall.load.job --json --report out.json
    python -m railfall.load.job --root /tmp/lake        # local lake (no S3A), for dev

Each silver table becomes `raw.<table>`, with the same columns (see
`railfall.load.schema`) and the silver contract's business key as primary key.

Scope of a load:

* without `--year`, the whole table is replaced: rows silver no longer has are
  deleted from the warehouse;
* with `--year`, only year-partitioned tables are filtered, and only the
  partitions silver has for those years are replaced. Unpartitioned tables
  (`stations`, `route_departments`) are small reference data and always load
  in full, so a route-month never points at a route the warehouse lacks.

Every table is merged in its own transaction (`railfall.load.postgres.merge_rows`),
so a failing table rolls back alone and is reported; the command exits non-zero
if any table failed. Re-running the job with the same silver changes nothing.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from railfall.config import Settings
from railfall.load.postgres import merge_rows
from railfall.load.schema import TargetTable, target_table
from railfall.spark.verify import CONTRACTS
from railfall.spark.write import TABLES, Table, read_silver

if TYPE_CHECKING:
    import psycopg
    from pyspark.sql import DataFrame, SparkSession

YEAR = "year"


@dataclass(frozen=True)
class LoadResult:
    table: str
    ok: bool
    target: str = ""
    scope: str = "full"
    staged: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    deleted: int = 0
    error: str = ""


def target_for(
    table: Table, spark_schema: Iterable[tuple[str, str]], *, schema: str = "raw"
) -> TargetTable:
    """Warehouse table for a silver table: silver columns, contract key, silver partitions."""
    return target_table(
        table.name,
        spark_schema,
        key=CONTRACTS[table.name].key,
        partition_by=table.partition_by,
        schema=schema,
    )


def is_incremental(table: Table, years: Sequence[int] | None) -> bool:
    """Whether `years` restricts this table (only year-partitioned tables are restricted)."""
    return bool(years) and YEAR in table.partition_by


def scope_label(table: Table, years: Sequence[int] | None) -> str:
    if not is_incremental(table, years):
        return "full"
    return "year=" + ",".join(str(y) for y in sorted(set(years or ())))


def spark_schema(df: DataFrame) -> list[tuple[str, str]]:
    return [(f.name, f.dataType.simpleString()) for f in df.schema]


def select_scope(df: DataFrame, table: Table, years: Sequence[int] | None) -> DataFrame:
    if not is_incremental(table, years):
        return df
    from pyspark.sql import functions as F

    return df.where(F.col(YEAR).isin(sorted(set(years or ()))))


def partitions_in(df: DataFrame, target: TargetTable) -> list[tuple[Any, ...]]:
    """Distinct partition tuples present in `df`, sorted (the incremental delete scope)."""
    rows = df.select(*target.partition_by).distinct().collect()
    return sorted(tuple(r) for r in rows)


def iter_rows(df: DataFrame, columns: Sequence[str]) -> Iterator[tuple[Any, ...]]:
    """Stream rows to the driver one partition at a time, in `columns` order."""
    for row in df.select(*columns).toLocalIterator():
        yield tuple(row)


def load_table(
    spark: SparkSession,
    conn: psycopg.Connection,
    table: Table,
    *,
    root: str,
    years: Sequence[int] | None = None,
    schema: str = "raw",
) -> LoadResult:
    """Merge one silver table (or its `years`) into the warehouse, in one transaction."""
    scope = scope_label(table, years)
    try:
        df = read_silver(spark, root, table)
        target = target_for(table, spark_schema(df), schema=schema)
        df = select_scope(df, table, years)
        partitions = partitions_in(df, target) if is_incremental(table, years) else None
        with conn.transaction():
            merged = merge_rows(
                conn, target, iter_rows(df, target.column_names), partitions=partitions
            )
    except Exception as exc:  # one table failing must not stop the others
        return LoadResult(
            table.name,
            ok=False,
            target=f"{schema}.{table.name}",
            scope=scope,
            error=f"{type(exc).__name__}: {exc}",
        )
    return LoadResult(
        table.name,
        ok=True,
        target=f"{target.schema}.{target.name}",
        scope=scope,
        staged=merged.staged,
        inserted=merged.inserted,
        updated=merged.updated,
        unchanged=merged.unchanged,
        deleted=merged.deleted,
    )


def run(
    spark: SparkSession,
    conn: psycopg.Connection,
    *,
    root: str,
    names: Iterable[str] | None = None,
    years: Sequence[int] | None = None,
    schema: str = "raw",
) -> list[LoadResult]:
    """Load the selected tables (default: all) in silver dependency order."""
    selected = set(names) if names else set(TABLES)
    unknown = sorted(selected - set(TABLES))
    if unknown:
        raise ValueError(f"unknown silver tables: {unknown}")
    return [
        load_table(spark, conn, table, root=root, years=years, schema=schema)
        for name, table in TABLES.items()
        if name in selected
    ]


def _format(r: LoadResult) -> str:
    if not r.ok:
        return f"[FAIL] {r.table:<22} {r.error}"
    return (
        f"[  ok] {r.target:<26} {r.scope:<12} {r.staged:>8} staged: "
        f"+{r.inserted} ~{r.updated} ={r.unchanged} -{r.deleted}"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load silver tables into the warehouse.")
    parser.add_argument("--table", action="append", choices=sorted(TABLES))
    parser.add_argument(
        "--year",
        action="append",
        type=int,
        help="only replace these years of year-partitioned tables (repeatable)",
    )
    parser.add_argument("--root", default=None, help="local lake directory instead of MinIO")
    parser.add_argument("--json", action="store_true", help="print a JSON summary")
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="also write the JSON summary to this file (JVM output can share stdout)",
    )
    args = parser.parse_args(argv)

    import psycopg

    from railfall.spark.session import build_session, lake_uri

    settings = Settings.from_env()
    if args.root:
        root = args.root
        spark = build_session(app_name="railfall-load", with_lake=False)
    else:
        root = lake_uri(settings).rstrip("/")
        spark = build_session(settings, app_name="railfall-load")
    try:
        # Autocommit: each table opens (and commits or rolls back) its own transaction.
        with psycopg.connect(settings.warehouse_dsn, autocommit=True) as conn:
            results = run(spark, conn, root=root, names=args.table, years=args.year)
    finally:
        spark.stop()

    payload = json.dumps([asdict(r) for r in results], indent=2)
    if args.report:
        args.report.write_text(payload + "\n")
    if args.json:
        print(payload)
    else:
        for r in results:
            print(_format(r))
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
