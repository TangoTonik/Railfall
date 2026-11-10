"""Silver job: read bronze from the lake, clean it in Spark, write silver Parquet.

    python -m railfall.spark.job                       # every silver table
    python -m railfall.spark.job --table stations      # one table (repeatable)
    python -m railfall.spark.job --json                # machine-readable summary
    python -m railfall.spark.job --root /tmp/lake      # local lake (no S3A), for dev

Which bronze feeds which table:

* SNCF exports are full snapshots, so only the most recent `ingest_date` of each
  dataset is read; older days are history, not extra rows.
* Météo-France files overlap from one day to the next ("latest" covers the
  current and previous year), so every bronze day is read and the weather
  transform keeps one row per station-day, preferring the newest file.

Derived tables (`weather_monthly`, `route_departments`, `route_weather_monthly`)
are built afterwards from silver itself, read back with typed partitions. Tables
run in dependency order; when an input failed in the same run, the tables built
on it are reported as failed instead of being rebuilt from a stale copy.

Each table is handled on its own: a missing bronze input or a failing transform
is reported and makes the command exit non-zero, without stopping the others.
Writes are partition overwrites (see `railfall.spark.write`), so re-running the
job rewrites silver instead of appending to it.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from railfall.config import Settings
from railfall.spark import (
    punctuality,
    route_departments,
    route_weather,
    stations,
    weather,
    weather_monthly,
)
from railfall.spark.write import (
    PUNCTUALITY,
    ROUTE_DEPARTMENTS,
    ROUTE_WEATHER,
    STATIONS,
    TABLES,
    WEATHER,
    WEATHER_MONTHLY,
    Table,
    read_silver,
    write_silver,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_PARTITION = re.compile(r"/ingest_date=(\d{4}-\d{2}-\d{2})/")


class MissingBronze(LookupError):
    """No bronze object exists yet for a silver table's input."""


class EmptyTable(RuntimeError):
    """A derived table came out empty: its inputs do not overlap."""


class UpstreamFailed(RuntimeError):
    """A silver table this derived table reads failed earlier in the same run."""


# Silver inputs of each derived table, in the order its builder takes them.
DEPENDS_ON: dict[Table, tuple[Table, ...]] = {
    WEATHER_MONTHLY: (WEATHER,),
    ROUTE_DEPARTMENTS: (PUNCTUALITY, STATIONS),
    ROUTE_WEATHER: (PUNCTUALITY, ROUTE_DEPARTMENTS, WEATHER_MONTHLY),
}

BUILDERS: dict[Table, Callable[..., DataFrame]] = {
    WEATHER_MONTHLY: weather_monthly.department_months,
    ROUTE_DEPARTMENTS: route_departments.route_departments,
    ROUTE_WEATHER: route_weather.route_weather,
}


@dataclass(frozen=True)
class TableResult:
    table: str
    ok: bool
    rows: int = 0
    rejected: int = 0
    source: str = ""
    path: str = ""
    error: str = ""


def latest_snapshot(keys: Iterable[str], source: str, dataset: str, fmt: str = "json") -> str:
    """Key of the newest bronze payload of `source/dataset` among `keys`.

    Manifests and other files in the partition are ignored. Raises MissingBronze
    when the dataset was never ingested.
    """
    prefix = f"bronze/{source}/{dataset}/"
    suffix = f"/{dataset}.{fmt}"
    best: tuple[date, str] | None = None
    for key in keys:
        if not (key.startswith(prefix) and key.endswith(suffix)):
            continue
        match = _PARTITION.search(key)
        if match is None:
            continue
        day = date.fromisoformat(match.group(1))
        if best is None or day > best[0]:
            best = (day, key)
    if best is None:
        raise MissingBronze(f"no bronze snapshot for {source}/{dataset}")
    return best[1]


def list_bronze_keys(client, bucket: str) -> list[str]:
    keys: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="bronze/"):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def list_local_keys(root: str) -> list[str]:
    base = Path(root)
    files = (p for p in (base / "bronze").rglob("*") if p.is_file())
    return sorted(p.relative_to(base).as_posix() for p in files)


def _transform(table: Table) -> tuple[Callable, Callable]:
    """(read_bronze, to_silver) of a silver table."""
    module = {PUNCTUALITY: punctuality, WEATHER: weather, STATIONS: stations}[table]
    return module.read_bronze, module.to_silver


def bronze_input(table: Table, keys: Sequence[str], root: str) -> str:
    """Path (under `root`) Spark should read to build `table`."""
    root = root.rstrip("/")
    if table == WEATHER:
        if not any(k.startswith("bronze/meteo/") and k.endswith(".csv.gz") for k in keys):
            raise MissingBronze("no bronze Météo-France files")
        return weather.bronze_glob(root)
    dataset = {PUNCTUALITY: "punctuality", STATIONS: "stations"}[table]
    return f"{root}/{latest_snapshot(keys, 'sncf', dataset)}"


def run_derived(
    spark: SparkSession, table: Table, *, root: str, run_date: date, failed: Iterable[str] = ()
) -> TableResult:
    """Build a derived table from the silver tables it depends on."""
    inputs = DEPENDS_ON[table]
    source = ", ".join(f"silver/{t.name}" for t in inputs)
    try:
        broken = [t.name for t in inputs if t.name in set(failed)]
        if broken:
            raise UpstreamFailed(f"input(s) failed in this run: {', '.join(broken)}")
        frames = [read_silver(spark, root, t) for t in inputs]
        written = write_silver(
            BUILDERS[table](*frames), None, root=root, table=table, run_date=run_date
        )
        if written.rows == 0:
            # Spark writes no Parquet file for an empty partitioned frame, so the
            # table would be unreadable downstream; fail here with the real cause.
            raise EmptyTable(f"{table.name} has no rows (inputs: {source})")
    except Exception as exc:  # one table failing must not hide the others
        return TableResult(table=table.name, ok=False, error=f"{type(exc).__name__}: {exc}")
    return TableResult(
        table=table.name, ok=True, rows=written.rows, source=source, path=written.path
    )


def run_table(
    spark: SparkSession, table: Table, *, root: str, keys: Sequence[str], run_date: date
) -> TableResult:
    try:
        source = bronze_input(table, keys, root)
        read, transform = _transform(table)
        split = transform(read(spark, source))
        written = write_silver(
            split.valid, split.rejected, root=root, table=table, run_date=run_date
        )
    except Exception as exc:  # one table failing must not hide the others
        return TableResult(table=table.name, ok=False, error=f"{type(exc).__name__}: {exc}")
    return TableResult(
        table=table.name,
        ok=True,
        rows=written.rows,
        rejected=written.rejected,
        source=source,
        path=written.path,
    )


def run(
    spark: SparkSession,
    *,
    root: str,
    keys: Sequence[str],
    names: Iterable[str] | None = None,
    run_date: date | None = None,
) -> list[TableResult]:
    selected = list(names) if names else list(TABLES)
    unknown = sorted(set(selected) - set(TABLES))
    if unknown:
        raise ValueError(f"unknown silver table(s): {', '.join(unknown)}")
    run_date = run_date or datetime.now(UTC).date()
    results: list[TableResult] = []
    failed: set[str] = set()
    for name, table in TABLES.items():  # dependency order, whatever the selection order
        if name not in selected:
            continue
        if table in DEPENDS_ON:
            result = run_derived(spark, table, root=root, run_date=run_date, failed=failed)
        else:
            result = run_table(spark, table, root=root, keys=keys, run_date=run_date)
        if not result.ok:
            failed.add(name)
        results.append(result)
    return results


def _format(result: TableResult) -> str:
    if not result.ok:
        return f"[FAIL] {result.table:<20} {result.error}"
    return (
        f"[  ok] {result.table:<20} {result.rows:>8} rows {result.rejected:>6} rejected"
        f" -> {result.path}"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the silver layer from bronze.")
    parser.add_argument(
        "--table",
        action="append",
        choices=sorted(TABLES),
        help="only this table (repeatable); default: all",
    )
    parser.add_argument(
        "--root",
        default=None,
        help="local lake directory instead of the MinIO bucket (no S3A)",
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="run date used for the quarantine partition; default: today (UTC)",
    )
    parser.add_argument("--json", action="store_true", help="print a JSON summary")
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="also write the JSON summary to this file (JVM output can share stdout)",
    )
    args = parser.parse_args(argv)

    from railfall.spark.session import build_session, lake_uri

    if args.root:
        root, keys = args.root, list_local_keys(args.root)
        spark = build_session(app_name="railfall-silver", with_lake=False)
    else:
        from railfall.ingest.run import lake_client

        settings = Settings.from_env()
        root = lake_uri(settings).rstrip("/")
        keys = list_bronze_keys(lake_client(settings), settings.lake_bucket)
        spark = build_session(settings, app_name="railfall-silver")
    try:
        results = run(spark, root=root, keys=keys, names=args.table, run_date=args.date)
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
