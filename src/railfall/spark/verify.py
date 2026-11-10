"""Read silver back from the lake and check it against its contract.

    python -m railfall.spark.verify                    # every silver table
    python -m railfall.spark.verify --table stations   # one table (repeatable)
    python -m railfall.spark.verify --json             # machine-readable report
    python -m railfall.spark.verify --root /tmp/lake   # local lake (no S3A)

For each table the check is independent of the job that wrote it:

* the Parquet files exist and Spark can read them,
* every contract column is present with its expected Spark type,
* the business key is never null and never repeated (re-runs must not
  duplicate rows),
* the table is not empty.

Tables whose contract names `profile` columns also report the row count per
value of each, e.g. how many route-months got weather on 0, 1 or 2 endpoints.
A join can keep its grain and still match nothing; the profile makes that
visible, and e2e asserts on it.

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

from railfall.config import Settings
from railfall.spark import punctuality, route_weather, weather, weather_monthly
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
    silver_path,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class Contract:
    key: tuple[str, ...]
    columns: Mapping[str, str]  # column -> Spark simpleString type
    # Columns whose value distribution is reported (low-cardinality only).
    profile: tuple[str, ...] = ()


CONTRACTS: dict[str, Contract] = {
    PUNCTUALITY.name: Contract(
        key=punctuality.KEY,
        columns={
            "month": "date",
            "year": "int",
            "route_id": "string",
            "service": "string",
            "departure_station": "string",
            "arrival_station": "string",
            "trains_planned": "bigint",
            "trains_cancelled": "bigint",
            "trains_late_arrival": "bigint",
            "trains_run": "bigint",
            "arrival_punctuality_rate": "double",
            "pct_cause_external": "double",
        },
    ),
    WEATHER.name: Contract(
        key=weather.KEY,
        columns={
            "department": "string",
            "station_id": "string",
            "day": "date",
            "year": "int",
            "month": "date",
            "latitude": "double",
            "longitude": "double",
            "precipitation_mm": "double",
            "temp_min_c": "double",
            "temp_max_c": "double",
            "wind_mean_ms": "double",
        },
    ),
    STATIONS.name: Contract(
        key=("uic_code",),
        columns={
            "uic_code": "string",
            "station_name": "string",
            "name_key": "string",
            "department": "string",
            "latitude": "double",
            "longitude": "double",
        },
    ),
    WEATHER_MONTHLY.name: Contract(
        key=weather_monthly.KEY,
        columns={
            "department": "string",
            "month": "date",
            "year": "int",
            "stations": "bigint",
            "precipitation_mm": "double",
            "rain_days": "double",
            "heavy_rain_days": "double",
            "temp_mean_c": "double",
            "temp_min_c": "double",
            "frost_days": "double",
            "wind_mean_ms": "double",
            "wind_max_ms": "double",
            "strong_wind_days": "double",
        },
    ),
    ROUTE_DEPARTMENTS.name: Contract(
        key=("route_id",),
        columns={
            "route_id": "string",
            "departure_station": "string",
            "arrival_station": "string",
            "departure_department": "string",
            "arrival_department": "string",
            "departure_match": "string",
            "arrival_match": "string",
            "is_domestic": "boolean",
        },
        profile=("departure_match", "arrival_match"),
    ),
    ROUTE_WEATHER.name: Contract(
        key=route_weather.KEY,
        columns={
            "month": "date",
            "year": "int",
            "service": "string",
            "route_id": "string",
            "trains_run": "bigint",
            "arrival_punctuality_rate": "double",
            "departure_department": "string",
            "arrival_department": "string",
            "is_domestic": "boolean",
            "weather_endpoints": "int",
            **{f"route_{m}": "double" for m in route_weather.WEATHER_MEASURES},
            **{
                f"{side}_{m}": "double"
                for side in route_weather.ENDPOINTS
                for m in route_weather.WEATHER_MEASURES
            },
        },
        profile=("weather_endpoints",),
    ),
}


@dataclass(frozen=True)
class SilverReport:
    table: str
    ok: bool
    rows: int = 0
    duplicate_keys: int = 0
    null_keys: int = 0
    schema_errors: list[str] = field(default_factory=list)
    path: str = ""
    error: str = ""
    # column -> {value as string: rows}; null values are reported under "null".
    profile: dict[str, dict[str, int]] = field(default_factory=dict)


def schema_errors(actual: Mapping[str, str], expected: Mapping[str, str]) -> list[str]:
    """Contract violations between a frame's `{column: type}` and the expected one."""
    errors = []
    for column, kind in expected.items():
        if column not in actual:
            errors.append(f"missing column {column}")
        elif actual[column] != kind:
            errors.append(f"{column}: expected {kind}, got {actual[column]}")
    return errors


def value_counts(df: DataFrame, columns: Iterable[str]) -> dict[str, dict[str, int]]:
    """Row count per value of each column, keys stringified and sorted (JSON-friendly)."""
    out: dict[str, dict[str, int]] = {}
    for column in columns:
        rows = df.groupBy(column).count().collect()
        counts = {"null" if r[0] is None else str(r[0]).lower(): int(r[1]) for r in rows}
        out[column] = dict(sorted(counts.items()))
    return out


def check_frame(df: DataFrame, table: Table, path: str = "") -> SilverReport:
    """Contract checks on an already-loaded silver frame."""
    from pyspark.sql import functions as F

    contract = CONTRACTS[table.name]
    errors = schema_errors({f.name: f.dataType.simpleString() for f in df.schema}, contract.columns)
    if errors:
        return SilverReport(table.name, ok=False, schema_errors=errors, path=path)

    key_cols = [F.col(c) for c in contract.key]
    any_null = F.lit(False)
    for col in key_cols:
        any_null = any_null | col.isNull()
    stats = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(any_null, 1).otherwise(0)).alias("null_keys"),
        F.count_distinct(*key_cols).alias("distinct_keys"),
    ).first()
    rows = int(stats["rows"])
    null_keys = int(stats["null_keys"] or 0)
    # count_distinct skips keys with a null part; those are counted separately.
    duplicates = rows - null_keys - int(stats["distinct_keys"])
    ok = rows > 0 and null_keys == 0 and duplicates == 0
    return SilverReport(
        table.name,
        ok=ok,
        rows=rows,
        duplicate_keys=duplicates,
        null_keys=null_keys,
        path=path,
        error="" if rows else "table is empty",
        profile=value_counts(df, contract.profile) if rows else {},
    )


def verify_table(spark: SparkSession, table: Table, root: str) -> SilverReport:
    path = silver_path(root, table)
    try:
        df = read_silver(spark, root, table)
        return check_frame(df, table, path)
    except Exception as exc:  # unreadable/missing table is a failed check, not a crash
        return SilverReport(table.name, ok=False, path=path, error=f"{type(exc).__name__}: {exc}")


def verify(
    spark: SparkSession, *, root: str, names: Iterable[str] | None = None
) -> list[SilverReport]:
    selected = list(names) if names else list(TABLES)
    return [verify_table(spark, TABLES[name], root) for name in selected]


def _format(r: SilverReport) -> str:
    if r.ok:
        line = f"[  ok] {r.table:<20} {r.rows:>8} rows, key unique -> {r.path}"
        for column, counts in r.profile.items():
            values = ", ".join(f"{v}={n}" for v, n in counts.items())
            line += f"\n       {'':<20} {column}: {values}"
        return line
    detail = r.error or "; ".join(r.schema_errors)
    if not detail:
        detail = f"{r.duplicate_keys} duplicate keys, {r.null_keys} null keys"
    return f"[FAIL] {r.table:<20} {detail}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check silver tables against their contract.")
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

    from railfall.spark.session import build_session, lake_uri

    if args.root:
        root = args.root
        spark = build_session(app_name="railfall-verify-silver", with_lake=False)
    else:
        settings = Settings.from_env()
        root = lake_uri(settings).rstrip("/")
        spark = build_session(settings, app_name="railfall-verify-silver")
    try:
        reports = verify(spark, root=root, names=args.table)
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
