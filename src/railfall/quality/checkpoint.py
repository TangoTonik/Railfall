"""Run the `raw` expectation suites as one Great Expectations checkpoint.

    python -m railfall.quality.checkpoint                    # every raw table
    python -m railfall.quality.checkpoint --table stations --json
    python -m railfall.quality.checkpoint --report quality.json

The context is ephemeral: suites live in `railfall.quality.suites` (versioned
with the code), so there is no `gx/` directory to keep in sync and nothing to
initialise in the Airflow image. Each run builds the datasource, one table
asset + validation definition per table, and a single checkpoint over all of
them, then reduces GX's verbose result to a `TableReport` per table.

It connects with the dbt (transformer) role, which can read `raw` but not
write to it: a quality gate has no business changing what it inspects.
Exit code 0 only when every expectation of every selected table passed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from railfall.quality.suites import RAW_SCHEMA, SUITES, Expectation, suite_name

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext

CHECKPOINT_NAME = "railfall_raw"
DATASOURCE_NAME = "warehouse"


@dataclass(frozen=True)
class Failure:
    expectation: str
    column: str | None = None
    unexpected_count: int | None = None
    observed: Any = None


@dataclass(frozen=True)
class TableReport:
    table: str
    ok: bool
    evaluated: int = 0
    failures: list[Failure] = field(default_factory=list)


def sqlalchemy_url(dsn: str) -> str:
    """A libpq `postgresql://` URL as SQLAlchemy wants it (psycopg2 driver)."""
    for prefix in ("postgresql://", "postgres://"):
        if dsn.startswith(prefix):
            return "postgresql+psycopg2://" + dsn[len(prefix) :]
    return dsn


def _expectation(spec: Expectation) -> Any:
    import great_expectations as gx

    return getattr(gx.expectations, spec.kind)(**dict(spec.kwargs))


def _datasource(context: AbstractDataContext, url: str) -> Any:
    if url.startswith("sqlite"):
        return context.data_sources.add_sqlite(DATASOURCE_NAME, connection_string=url)
    return context.data_sources.add_postgres(DATASOURCE_NAME, connection_string=url)


def build_checkpoint(
    context: AbstractDataContext,
    url: str,
    tables: Iterable[str],
    *,
    schema: str | None = RAW_SCHEMA,
    suites: Mapping[str, Sequence[Expectation]] = SUITES,
) -> Any:
    """Register datasource, suites and validations for `tables`; return the checkpoint."""
    import great_expectations as gx

    source = _datasource(context, url)
    validations = []
    for table in tables:
        location = {"schema_name": schema} if schema else {}
        asset = source.add_table_asset(name=table, table_name=table, **location)
        batch = asset.add_batch_definition_whole_table(f"{table}_all")
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name(table)))
        for spec in suites[table]:
            suite.add_expectation(_expectation(spec))
        validations.append(
            context.validation_definitions.add(
                gx.ValidationDefinition(name=f"validate_{table}", data=batch, suite=suite)
            )
        )
    return context.checkpoints.add(
        gx.Checkpoint(name=CHECKPOINT_NAME, validation_definitions=validations)
    )


def _failure(result: Any) -> Failure:
    config = result.expectation_config
    outcome = result.result or {}
    column = config.kwargs.get("column") or config.kwargs.get("column_A")
    observed = outcome.get("observed_value")
    if result.exception_info and result.exception_info.get("raised_exception"):
        observed = result.exception_info.get("exception_message")
    return Failure(
        expectation=config.type,
        column=column,
        unexpected_count=outcome.get("unexpected_count"),
        observed=observed,
    )


def reports(checkpoint_result: Any) -> list[TableReport]:
    """One report per validated table, from a GX `CheckpointResult`."""
    out = []
    for validation in checkpoint_result.run_results.values():
        table = validation.suite_name.split(".", 1)[-1]
        failures = [_failure(r) for r in validation.results if not r.success]
        out.append(
            TableReport(
                table=table,
                ok=bool(validation.success) and not failures,
                evaluated=len(validation.results),
                failures=failures,
            )
        )
    return sorted(out, key=lambda r: r.table)


def run(
    url: str,
    tables: Iterable[str] | None = None,
    *,
    schema: str | None = RAW_SCHEMA,
    suites: Mapping[str, Sequence[Expectation]] = SUITES,
) -> list[TableReport]:
    """Validate `tables` (default: every suite) at `url` in a fresh ephemeral context."""
    import great_expectations as gx

    names = list(suites) if tables is None else list(tables)
    unknown = sorted(set(names) - set(suites))
    if unknown:
        raise ValueError(f"no expectation suite for: {', '.join(unknown)}")
    # GX phones home by default; a local, offline pipeline has no use for it.
    os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")
    context = gx.get_context(mode="ephemeral")
    checkpoint = build_checkpoint(context, url, names, schema=schema, suites=suites)
    return reports(checkpoint.run())


def _format(table_reports: Sequence[TableReport]) -> str:
    lines = []
    for r in table_reports:
        status = "ok" if r.ok else "FAILED"
        lines.append(f"{r.table:<22} {status:<6} {r.evaluated - len(r.failures)}/{r.evaluated}")
        for f in r.failures:
            where = f" on {f.column}" if f.column else ""
            detail = f"unexpected={f.unexpected_count}" if f.unexpected_count else f"{f.observed}"
            lines.append(f"    {f.expectation}{where}: {detail}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    from railfall.transform.verify import transformer_dsn

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--table", action="append", choices=sorted(SUITES), dest="tables")
    parser.add_argument("--json", action="store_true", help="print the reports as JSON")
    parser.add_argument(
        "--report", type=Path, default=None, help="also write the JSON reports to this file"
    )
    parser.add_argument("--url", default=None, help="SQLAlchemy URL (default: DBT_* role)")
    args = parser.parse_args(argv)

    url = args.url or sqlalchemy_url(transformer_dsn())
    schema = None if url.startswith("sqlite") else RAW_SCHEMA
    table_reports = run(url, args.tables, schema=schema)
    payload = json.dumps([asdict(r) for r in table_reports], indent=2, default=str)
    if args.report:
        args.report.write_text(payload + "\n")
    if args.json:
        print(payload)
    else:
        print(_format(table_reports))
    return 0 if all(r.ok for r in table_reports) else 1


if __name__ == "__main__":
    sys.exit(main())
