"""Check the warehouse `raw` tables against silver, independently of the loader.

    python -m railfall.load.verify                       # every silver table
    python -m railfall.load.verify --table stations      # one table (repeatable)
    python -m railfall.load.verify --json --report out.json
    python -m railfall.load.verify --root /tmp/lake      # local lake (no S3A)

For each silver table, `raw.<table>` must:

* exist, with every silver contract column at the Postgres type it maps to,
* have the contract's business key as its primary key,
* hold no null and no repeated key (re-running the load must not duplicate),
* hold exactly as many rows as silver, and, for year-partitioned tables, as
  many rows per year (a full load leaves no stale year behind).

Expected columns and keys come from the silver contract, not from the loader's
own derivation, so a bug in `railfall.load.schema` cannot hide itself here.

Exit code 0 only when every selected table passes.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from psycopg import sql

from railfall.config import Settings
from railfall.load.postgres import existing_columns
from railfall.load.schema import TargetTable, canonical_pg_type, pg_type, target_table
from railfall.spark.verify import CONTRACTS
from railfall.spark.write import TABLES, Table, read_silver

if TYPE_CHECKING:
    import psycopg
    from pyspark.sql import SparkSession

YEAR = "year"


@dataclass(frozen=True)
class WarehouseReport:
    table: str
    ok: bool
    target: str = ""
    rows: int = 0
    silver_rows: int = 0
    duplicate_keys: int = 0
    null_keys: int = 0
    primary_key: list[str] = field(default_factory=list)
    # year -> warehouse rows, for year-partitioned tables (JSON keys are strings).
    by_year: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


def contract_target(table: Table, *, schema: str = "raw") -> TargetTable:
    """The warehouse table the silver contract implies (columns, key, partitions)."""
    contract = CONTRACTS[table.name]
    return target_table(
        table.name,
        contract.columns,
        key=contract.key,
        partition_by=table.partition_by,
        schema=schema,
    )


def expected_columns(table: Table) -> dict[str, str]:
    """`{column: format_type spelling}` every warehouse copy of `table` must have."""
    columns = CONTRACTS[table.name].columns
    return {name: canonical_pg_type(pg_type(kind)) for name, kind in columns.items()}


def column_errors(actual: Mapping[str, str], expected: Mapping[str, str]) -> list[str]:
    """Missing or mistyped columns; extra warehouse columns are allowed."""
    errors = []
    for column, kind in expected.items():
        if column not in actual:
            errors.append(f"missing column {column}")
        elif actual[column] != kind:
            errors.append(f"{column}: expected {kind}, got {actual[column]}")
    return errors


def count_errors(
    warehouse: Mapping[str, int], silver: Mapping[str, int], *, label: str = "year"
) -> list[str]:
    """One message per partition whose row count differs between warehouse and silver."""
    errors = []
    for value in sorted(set(warehouse) | set(silver)):
        got, want = warehouse.get(value, 0), silver.get(value, 0)
        if got != want:
            errors.append(f"{label}={value}: {got} rows in warehouse, {want} in silver")
    return errors


def primary_key(conn: psycopg.Connection, schema: str, name: str) -> list[str]:
    """Primary key columns of `schema.name`, in key order; empty if none."""
    rows = conn.execute(
        """
        select a.attname
        from pg_index i
        join pg_class c on c.oid = i.indrelid
        join pg_namespace n on n.oid = c.relnamespace
        cross join lateral unnest(i.indkey) with ordinality as k(attnum, pos)
        join pg_attribute a on a.attrelid = c.oid and a.attnum = k.attnum
        where i.indisprimary and n.nspname = %s and c.relname = %s
        order by k.pos
        """,
        (schema, name),
    ).fetchall()
    return [r[0] for r in rows]


def key_stats_sql(target: TargetTable) -> sql.Composed:
    """`(rows, null_keys, duplicate_keys)` of the warehouse table."""
    keys = sql.SQL(", ").join(sql.Identifier(k) for k in target.key)
    any_null = sql.SQL(" or ").join(
        sql.SQL("{} is null").format(sql.Identifier(k)) for k in target.key
    )
    return sql.SQL(
        "select count(*), count(*) filter (where {any_null}), "
        "(select coalesce(sum(n - 1), 0) from "
        "(select count(*) as n from {t} group by {keys} having count(*) > 1) d) "
        "from {t}"
    ).format(any_null=any_null, keys=keys, t=sql.Identifier(target.schema, target.name))


def warehouse_by_year(conn: psycopg.Connection, target: TargetTable) -> dict[str, int]:
    rows = conn.execute(
        sql.SQL("select {y}, count(*) from {t} group by {y}").format(
            y=sql.Identifier(YEAR), t=sql.Identifier(target.schema, target.name)
        )
    ).fetchall()
    return {str(year): int(n) for year, n in rows}


def silver_counts(spark: SparkSession, root: str, table: Table) -> tuple[int, dict[str, int]]:
    """Silver row count, and per year for year-partitioned tables."""
    df = read_silver(spark, root, table)
    if YEAR not in table.partition_by:
        return df.count(), {}
    by_year = {str(r[0]): int(r[1]) for r in df.groupBy(YEAR).count().collect()}
    return sum(by_year.values()), by_year


def check_table(
    conn: psycopg.Connection,
    table: Table,
    *,
    silver_rows: int,
    silver_by_year: Mapping[str, int],
    schema: str = "raw",
) -> WarehouseReport:
    """Compare `schema.<table>` with silver's counts; pure SQL, no Spark."""
    target = contract_target(table, schema=schema)
    name = f"{schema}.{table.name}"
    columns = existing_columns(conn, target)
    if not columns:
        return WarehouseReport(
            table.name, ok=False, target=name, silver_rows=silver_rows, errors=["table missing"]
        )
    errors = column_errors(columns, expected_columns(table))
    pk = primary_key(conn, schema, table.name)
    if pk != list(target.key):
        errors.append(f"primary key is {pk or 'missing'}, contract key is {list(target.key)}")
    if errors:  # stats on a wrongly shaped table would be misleading
        return WarehouseReport(
            table.name,
            ok=False,
            target=name,
            silver_rows=silver_rows,
            primary_key=pk,
            errors=errors,
        )

    rows, nulls, dupes = (int(v) for v in conn.execute(key_stats_sql(target)).fetchone())
    if nulls:
        errors.append(f"{nulls} rows with a null key")
    if dupes:
        errors.append(f"{dupes} duplicate key rows")
    if rows != silver_rows:
        errors.append(f"{rows} rows in warehouse, {silver_rows} in silver")
    if rows == 0:
        errors.append("table is empty")
    by_year: dict[str, int] = {}
    if YEAR in table.partition_by:
        by_year = dict(sorted(warehouse_by_year(conn, target).items()))
        errors += count_errors(by_year, silver_by_year)
    return WarehouseReport(
        table.name,
        ok=not errors,
        target=name,
        rows=rows,
        silver_rows=silver_rows,
        duplicate_keys=dupes,
        null_keys=nulls,
        primary_key=pk,
        by_year=by_year,
        errors=errors,
    )


def verify_table(
    spark: SparkSession, conn: psycopg.Connection, table: Table, *, root: str, schema: str = "raw"
) -> WarehouseReport:
    try:
        silver_rows, silver_by_year = silver_counts(spark, root, table)
        return check_table(
            conn, table, silver_rows=silver_rows, silver_by_year=silver_by_year, schema=schema
        )
    except Exception as exc:  # unreadable silver or a broken table is a failed check
        return WarehouseReport(
            table.name,
            ok=False,
            target=f"{schema}.{table.name}",
            errors=[f"{type(exc).__name__}: {exc}"],
        )


def verify(
    spark: SparkSession,
    conn: psycopg.Connection,
    *,
    root: str,
    names: Iterable[str] | None = None,
    schema: str = "raw",
) -> list[WarehouseReport]:
    selected = set(names) if names else set(TABLES)
    unknown = sorted(selected - set(TABLES))
    if unknown:
        raise ValueError(f"unknown silver tables: {unknown}")
    return [
        verify_table(spark, conn, table, root=root, schema=schema)
        for name, table in TABLES.items()
        if name in selected
    ]


def _format(r: WarehouseReport) -> str:
    if r.ok:
        return f"[  ok] {r.target:<26} {r.rows:>8} rows = silver, key unique"
    return f"[FAIL] {r.target:<26} " + "; ".join(r.errors)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check warehouse raw tables against silver.")
    parser.add_argument("--table", action="append", choices=sorted(TABLES))
    parser.add_argument("--root", default=None, help="local lake directory instead of MinIO")
    parser.add_argument("--json", action="store_true", help="print a JSON report")
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="also write the JSON report to this file (JVM output can share stdout)",
    )
    args = parser.parse_args(argv)

    import psycopg

    from railfall.spark.session import build_session, lake_uri

    settings = Settings.from_env()
    if args.root:
        root = args.root
        spark = build_session(app_name="railfall-verify-load", with_lake=False)
    else:
        root = lake_uri(settings).rstrip("/")
        spark = build_session(settings, app_name="railfall-verify-load")
    try:
        with psycopg.connect(settings.warehouse_dsn, autocommit=True) as conn:
            reports = verify(spark, conn, root=root, names=args.table)
    finally:
        spark.stop()

    payload = json.dumps([asdict(r) for r in reports], indent=2)
    if args.report:
        args.report.write_text(payload + "\n")
    if args.json:
        print(payload)
    else:
        for r in reports:
            print(_format(r))
    return 0 if all(r.ok for r in reports) else 1


if __name__ == "__main__":
    sys.exit(main())
