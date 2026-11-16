"""Check what `dbt build` left in the warehouse, independently of dbt's own tests.

    python -m railfall.transform.verify                  # every staging model
    python -m railfall.transform.verify --model stg_sncf__stations
    python -m railfall.transform.verify --json --report out.json

Staging is a 1:1 typed window on `raw`, so for each staging model:

* the relation exists in the `staging` schema as a view (materialisation drift
  would silently freeze staging at the last build),
* it holds exactly as many rows as its `raw` source (no join fan-out, no filter),
* its declared grain is unique and never null.

It connects as the transformer role, with the same `DBT_*` variables as
`dbt/profiles.yml`, so it sees exactly what dbt sees. Exit code 0 only when
every selected model passes.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from psycopg import sql

if TYPE_CHECKING:
    import psycopg

STAGING_SCHEMA = "staging"
RAW_SCHEMA = "raw"


@dataclass(frozen=True)
class StagingModel:
    name: str
    source: str
    key: tuple[str, ...]


STAGING_MODELS: dict[str, StagingModel] = {
    m.name: m
    for m in (
        StagingModel("stg_sncf__punctuality_monthly", "punctuality_monthly", ("route_month_id",)),
        StagingModel("stg_meteo__weather_daily", "weather_daily", ("station_id", "day")),
        StagingModel("stg_sncf__stations", "stations", ("uic_code",)),
        StagingModel("stg_meteo__weather_monthly", "weather_monthly", ("department", "month")),
        StagingModel("stg_sncf__route_departments", "route_departments", ("route_id",)),
    )
}


@dataclass(frozen=True)
class ModelReport:
    model: str
    ok: bool
    relation: str = ""
    kind: str | None = None
    rows: int = 0
    source_rows: int = 0
    duplicate_keys: int = 0
    null_keys: int = 0
    errors: list[str] = field(default_factory=list)


def transformer_dsn(env: Mapping[str, str] | None = None) -> str:
    """libpq URL for the dbt role, resolved like `dbt/profiles.yml`."""
    env = os.environ if env is None else env
    host = env.get("DBT_HOST", "localhost")
    port = env.get("DBT_PORT") or env.get("POSTGRES_PORT") or "5432"
    user = env.get("DBT_USER", "railfall_transformer")
    password = env.get("DBT_PASSWORD") or env.get(
        "TF_VAR_warehouse_transformer_password", "transformer-local"
    )
    dbname = env.get("DBT_DBNAME") or env.get("POSTGRES_DB") or "warehouse"
    return f"postgresql://{quote(user)}:{quote(password, safe='')}@{host}:{port}/{dbname}"


# relkind -> name, per pg_class.
_KINDS = {"v": "view", "m": "materialized view", "r": "table", "p": "table"}

RELKIND_SQL = """
    select c.relkind
    from pg_class c join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = %s and c.relname = %s
"""


def relation_kind(conn: psycopg.Connection, schema: str, name: str) -> str | None:
    row = conn.execute(RELKIND_SQL, (schema, name)).fetchone()
    if row is None:
        return None
    kind = row[0].decode() if isinstance(row[0], bytes) else str(row[0])
    return _KINDS.get(kind, kind)


def count_sql(schema: str, name: str) -> sql.Composed:
    return sql.SQL("select count(*) from {}").format(sql.Identifier(schema, name))


def key_check_sql(schema: str, name: str, key: Sequence[str]) -> sql.Composed:
    """One row: (repeated key groups, rows with any null key column)."""
    cols = sql.SQL(", ").join(sql.Identifier(k) for k in key)
    any_null = sql.SQL(" or ").join(sql.SQL("{} is null").format(sql.Identifier(k)) for k in key)
    return sql.SQL(
        "select"
        " (select count(*) from (select 1 from {rel} group by {cols} having count(*) > 1) d),"
        " (select count(*) from {rel} where {any_null})"
    ).format(rel=sql.Identifier(schema, name), cols=cols, any_null=any_null)


def verify_model(conn: psycopg.Connection, model: StagingModel) -> ModelReport:
    relation = f"{STAGING_SCHEMA}.{model.name}"
    kind = relation_kind(conn, STAGING_SCHEMA, model.name)
    if kind is None:
        return ModelReport(model.name, False, relation, errors=[f"{relation} does not exist"])

    errors: list[str] = []
    if kind != "view":
        errors.append(f"{relation} is a {kind}, expected a view")

    rows = conn.execute(count_sql(STAGING_SCHEMA, model.name)).fetchone()[0]
    source_rows = conn.execute(count_sql(RAW_SCHEMA, model.source)).fetchone()[0]
    dupes, nulls = conn.execute(key_check_sql(STAGING_SCHEMA, model.name, model.key)).fetchone()

    if rows == 0:
        errors.append(f"{relation} is empty")
    if rows != source_rows:
        errors.append(f"{relation} has {rows} rows, raw.{model.source} has {source_rows}")
    if dupes:
        errors.append(f"{dupes} repeated {'/'.join(model.key)} values")
    if nulls:
        errors.append(f"{nulls} rows with a null {'/'.join(model.key)}")

    return ModelReport(
        model=model.name,
        ok=not errors,
        relation=relation,
        kind=kind,
        rows=rows,
        source_rows=source_rows,
        duplicate_keys=dupes,
        null_keys=nulls,
        errors=errors,
    )


def select_models(names: Sequence[str]) -> list[StagingModel]:
    unknown = [n for n in names if n not in STAGING_MODELS]
    if unknown:
        raise ValueError(f"unknown staging model(s): {', '.join(unknown)}")
    return [STAGING_MODELS[n] for n in names] if names else list(STAGING_MODELS.values())


def verify(conn: psycopg.Connection, names: Sequence[str] = ()) -> list[ModelReport]:
    return [verify_model(conn, m) for m in select_models(names)]


def _format(reports: Sequence[ModelReport]) -> str:
    lines = []
    for r in reports:
        lines.append(
            f"[{'  ok' if r.ok else 'FAIL'}] {r.relation:<40} {r.kind or '-':<6} "
            f"rows={r.rows} source={r.source_rows}"
        )
        lines += [f"  error: {e}" for e in r.errors]
    return "\n".join(lines)


def _connect(dsn: str) -> Any:
    import psycopg

    return psycopg.connect(dsn, autocommit=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check dbt staging views against raw.")
    parser.add_argument("--model", action="append", default=[], help="repeatable")
    parser.add_argument("--json", action="store_true", help="print a JSON report")
    parser.add_argument("--report", type=Path, default=None, help="also write the JSON report")
    args = parser.parse_args(argv)

    try:
        select_models(args.model)
    except ValueError as exc:
        parser.error(str(exc))

    with _connect(transformer_dsn()) as conn:
        reports = verify(conn, args.model)

    payload = json.dumps([asdict(r) for r in reports], indent=2)
    if args.report:
        args.report.write_text(payload + "\n")
    print(payload if args.json else _format(reports))
    return 0 if reports and all(r.ok for r in reports) else 1


if __name__ == "__main__":
    sys.exit(main())
