"""Prove the quality gate bites: corrupt copies of the real raw tables, expect failures.

    python -m railfall.quality.canary                    # every raw table
    python -m railfall.quality.canary --table stations --report canary.json

A gate that has only ever passed proves little: a suite pointed at the wrong
column, or an expectation GX silently skips, passes too. The canary copies each
real table (as the loader, which owns `raw`), plants one known defect in one
row or one route-month of the copy, and runs the *real* suite over the copy as
the read-only transformer role. Each table must fail, and on the expectation
the defect targets. The copies are dropped afterwards, whatever happened.

Corruptions are plain portable SQL (no `ctid`), so the whole flow is unit
tested on SQLite with the same code that runs against Postgres in e2e.
Exit code 0 only when the gate caught every planted defect.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from railfall.quality import checkpoint
from railfall.quality.suites import RAW_SCHEMA, SUITES

PREFIX = "_canary_"


@dataclass(frozen=True)
class Corruption:
    """One planted defect: SQL over `{t}` (the copy) and the failure it must cause."""

    description: str
    statement: str
    expectation: str
    column: str


def _first(t: str, column: str, within: str = "") -> str:
    where = f" where {within}" if within else ""
    return f"(select min({column}) from {t}{where})"


def _one_of(t: str, a: str, b: str, only: str) -> str:
    """Among rows matching `only`: the smallest `a`, and within it the smallest `b`."""
    first_a = _first(t, a, only)
    pinned = f"{only} and {a} = {first_a}"
    return f"{pinned} and {b} = {_first(t, b, pinned)}"


CORRUPTIONS: dict[str, Corruption] = {
    "punctuality_monthly": Corruption(
        "a route-month ran more trains than it planned",
        "update {t} set trains_run = trains_planned + 1 where {one}",
        "expect_column_pair_values_a_to_be_greater_than_b",
        "trains_planned",
    ),
    "weather_daily": Corruption(
        "one station-day reads a 99 °C minimum",
        "update {t} set temp_min_c = 99.0 where {one}",
        "expect_column_values_to_be_between",
        "temp_min_c",
    ),
    "stations": Corruption(
        "one station is in department XX",
        "update {t} set department = 'XX' where uic_code = {first_uic}",
        "expect_column_values_to_match_regex",
        "department",
    ),
    "weather_monthly": Corruption(
        "a department-month has more heavy-rain days than rain days",
        "update {t} set heavy_rain_days = rain_days + 1 where {one}",
        "expect_column_pair_values_a_to_be_greater_than_b",
        "rain_days",
    ),
    "route_departments": Corruption(
        "one route is mapped twice",
        "insert into {t} select * from {t} where route_id = {first_route}",
        "expect_column_values_to_be_unique",
        "route_id",
    ),
}

# Columns that pin "one row" (or one route-month) per table, among the rows
# where the defect can show (a null counter would make the pair check skip it).
_ONE_ROW: dict[str, tuple[str, str, str]] = {
    "punctuality_monthly": ("month", "route_id", "trains_planned is not null"),
    "weather_daily": ("day", "station_id", "1 = 1"),
    "weather_monthly": ("month", "department", "rain_days is not null"),
}


@dataclass(frozen=True)
class CanaryResult:
    table: str
    defect: str
    caught: bool
    expected: str
    failures: list[str]


def _quote(*parts: str | None) -> str:
    return ".".join(f'"{p}"' for p in parts if p)


def corruption_sql(table: str, copy: str) -> str:
    """The planted-defect statement for `table`, aimed at `copy` (already quoted)."""
    spec = CORRUPTIONS[table]
    a, b, only = _ONE_ROW.get(table, ("", "", ""))
    return spec.statement.format(
        t=copy,
        one=_one_of(copy, a, b, only) if a else "",
        first_uic=_first(copy, "uic_code"),
        first_route=_first(copy, "route_id"),
    )


Execute = Callable[[str], Any]


def plant(execute: Execute, tables: Iterable[str], schema: str | None) -> dict[str, str]:
    """Copy each table and corrupt the copy; returns suite -> copy table name."""
    copies = {}
    for table in tables:
        copy = PREFIX + table
        execute(f"drop table if exists {_quote(schema, copy)}")
        execute(f"create table {_quote(schema, copy)} as select * from {_quote(schema, table)}")
        execute(corruption_sql(table, _quote(schema, copy)))
        copies[table] = copy
    return copies


def remove(execute: Execute, copies: Iterable[str], schema: str | None) -> None:
    for copy in copies:
        execute(f"drop table if exists {_quote(schema, copy)}")


def judge(reports: Iterable[checkpoint.TableReport]) -> list[CanaryResult]:
    """Did each copy fail, and on the expectation its defect targets?"""
    out = []
    for report in reports:
        spec = CORRUPTIONS[report.table]
        seen = [f"{f.expectation}({f.column})" for f in report.failures]
        expected = f"{spec.expectation}({spec.column})"
        out.append(
            CanaryResult(
                table=report.table,
                defect=spec.description,
                caught=not report.ok and expected in seen,
                expected=expected,
                failures=seen,
            )
        )
    return out


def run(
    execute: Execute,
    url: str,
    tables: Iterable[str] | None = None,
    *,
    schema: str | None = RAW_SCHEMA,
) -> list[CanaryResult]:
    """Plant, validate as `url`'s role, judge, and always drop the copies."""
    names = list(CORRUPTIONS) if tables is None else list(tables)
    unknown = sorted(set(names) - set(CORRUPTIONS))
    if unknown:
        raise ValueError(f"no canary for: {', '.join(unknown)}")
    copies: dict[str, str] = {}
    try:
        copies = plant(execute, names, schema)
        reports = checkpoint.run(url, names, schema=schema, suites=SUITES, table_names=copies)
    finally:
        remove(execute, copies.values(), schema)
    return judge(reports)


def _format(results: Sequence[CanaryResult]) -> str:
    lines = []
    for r in results:
        status = "caught" if r.caught else "MISSED"
        lines.append(f"{r.table:<22} {status:<7} {r.defect}")
        if not r.caught:
            lines.append(f"    expected {r.expected}, got {', '.join(r.failures) or 'no failure'}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    import psycopg

    from railfall.config import Settings
    from railfall.transform.verify import transformer_dsn

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--table", action="append", choices=sorted(CORRUPTIONS), dest="tables")
    parser.add_argument("--report", type=Path, default=None, help="write the results as JSON")
    args = parser.parse_args(argv)

    owner_dsn = Settings.from_env(env).warehouse_dsn
    url = checkpoint.sqlalchemy_url(transformer_dsn(env))
    with psycopg.connect(owner_dsn, autocommit=True, connect_timeout=10) as conn:
        results = run(conn.execute, url, args.tables)
    if args.report:
        payload = json.dumps([asdict(r) for r in results], indent=2)
        args.report.write_text(payload + "\n")
    print(_format(results))
    return 0 if all(r.caught for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
