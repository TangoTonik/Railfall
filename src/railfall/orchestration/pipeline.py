"""Spark and warehouse tasks for the full `railfall_pipeline` DAG.

Each function runs one step of `make silver` / `make verify-silver` /
`make load` / `make verify-load` inside an Airflow task. Like `railfall.orchestration.ingest`:

* it returns a JSON-serialisable summary (the task's XCom value);
* it raises `TaskFailed` when any table failed, after the other tables were
  still processed, so Airflow marks the task failed and retries it. Silver
  writes are partition overwrites, so a retry rewrites instead of duplicating.

The SparkSession can be injected (tests share one); otherwise each task starts
its own session and always stops it, so a failed task never leaks a JVM into
the worker; the warehouse connection works the same way. `root=None` means
the MinIO lake over S3A, exactly like the CLI; a local directory works for
development and tests.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date
from typing import TYPE_CHECKING

from railfall.config import Settings
from railfall.orchestration.ingest import Summary, _date, _raise_on_failures

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@contextmanager
def _session(
    settings: Settings, app_name: str, *, spark: SparkSession | None, local: bool
) -> Iterator[SparkSession]:
    """The injected session as-is, or a new one stopped on exit."""
    if spark is not None:
        yield spark
        return
    from railfall.spark.session import build_session

    if local:
        session = build_session(app_name=app_name, with_lake=False)
    else:
        session = build_session(settings, app_name=app_name)
    try:
        yield session
    finally:
        session.stop()


def _root(settings: Settings, root: str | None) -> str:
    if root is not None:
        return root.rstrip("/")
    from railfall.spark.session import lake_uri

    return lake_uri(settings).rstrip("/")


def _bronze_keys(settings: Settings, root: str | None, client=None) -> list[str]:
    from railfall.spark import job

    if root is not None:
        return job.list_local_keys(root)
    if client is None:
        from railfall.ingest.run import lake_client

        client = lake_client(settings)
    return job.list_bronze_keys(client, settings.lake_bucket)


def build_silver(
    run_date: str | date,
    *,
    tables: Iterable[str] | None = None,
    settings: Settings | None = None,
    root: str | None = None,
    spark: SparkSession | None = None,
    client=None,
) -> Summary:
    """Bronze -> silver for every table (default) in dependency order.

    `run_date` names the quarantine partition of the rows a transform rejects;
    the DAG passes the run's `ingest_date` so a retry lands in the same place.
    """
    from railfall.spark import job

    settings = settings or Settings.from_env()
    keys = _bronze_keys(settings, root, client)
    with _session(settings, "railfall-silver", spark=spark, local=root is not None) as session:
        results = job.run(
            session, root=_root(settings, root), keys=keys, names=tables, run_date=_date(run_date)
        )
    _raise_on_failures("build silver", [f"{r.table}: {r.error}" for r in results if not r.ok])
    return [asdict(r) for r in results]


def verify_silver(
    *,
    tables: Iterable[str] | None = None,
    settings: Settings | None = None,
    root: str | None = None,
    spark: SparkSession | None = None,
) -> Summary:
    """Contract checks on silver (schema, non-empty, unique non-null business key)."""
    from railfall.spark import verify

    settings = settings or Settings.from_env()
    with _session(
        settings, "railfall-verify-silver", spark=spark, local=root is not None
    ) as session:
        reports = verify.verify(session, root=_root(settings, root), names=tables)
    _raise_on_failures(
        "verify silver",
        [
            f"{r.table}: "
            + (
                r.error
                or "; ".join(r.schema_errors)
                or f"{r.duplicate_keys} duplicate keys, {r.null_keys} null keys"
            )
            for r in reports
            if not r.ok
        ],
    )
    return [asdict(r) for r in reports]


@contextmanager
def _warehouse(settings: Settings, conn) -> Iterator:
    """The injected connection as-is, or a new autocommit one closed on exit.

    Autocommit because every table opens (and commits or rolls back) its own
    transaction in `railfall.load.postgres.merge_rows`.
    """
    if conn is not None:
        yield conn
        return
    import psycopg

    with psycopg.connect(settings.warehouse_dsn, autocommit=True) as own:
        yield own


def load_warehouse(
    *,
    tables: Iterable[str] | None = None,
    years: Iterable[int] | None = None,
    schema: str = "raw",
    settings: Settings | None = None,
    root: str | None = None,
    spark: SparkSession | None = None,
    conn=None,
) -> Summary:
    """Silver -> `raw.*`, merged per table; the run is logged in `raw._load_runs`.

    Every result carries the same `run_id`, which the DAG run report surfaces so
    an operator can find the audit rows of the run.
    """
    from railfall.load import job

    settings = settings or Settings.from_env()
    with (
        _session(settings, "railfall-load", spark=spark, local=root is not None) as session,
        _warehouse(settings, conn) as warehouse,
    ):
        results = job.run(
            session,
            warehouse,
            root=_root(settings, root),
            names=tables,
            years=list(years) if years else None,
            schema=schema,
        )
    _raise_on_failures("load warehouse", [f"{r.table}: {r.error}" for r in results if not r.ok])
    return [asdict(r) for r in results]


def verify_warehouse(
    *,
    tables: Iterable[str] | None = None,
    schema: str = "raw",
    settings: Settings | None = None,
    root: str | None = None,
    spark: SparkSession | None = None,
    conn=None,
) -> Summary:
    """Warehouse = silver: same row counts (per year too), unique non-null primary key."""
    from railfall.load import verify

    settings = settings or Settings.from_env()
    with (
        _session(settings, "railfall-verify-load", spark=spark, local=root is not None) as session,
        _warehouse(settings, conn) as warehouse,
    ):
        reports = verify.verify(
            session, warehouse, root=_root(settings, root), names=tables, schema=schema
        )
    _raise_on_failures(
        "verify warehouse", [f"{r.table}: {'; '.join(r.errors)}" for r in reports if not r.ok]
    )
    return [asdict(r) for r in reports]


def check_quality(
    *,
    tables: Iterable[str] | None = None,
    url: str | None = None,
    schema: str | None = "raw",
) -> Summary:
    """Run the Great Expectations checkpoint over `raw.*` (the DAG's quality gate).

    Connects as the read-only transformer role (`DBT_*` variables, like dbt
    itself) unless a SQLAlchemy `url` is injected. Every table is validated
    before the task fails, so one run lists every broken expectation.
    """
    from railfall.quality import checkpoint
    from railfall.transform.verify import transformer_dsn

    url = url or checkpoint.sqlalchemy_url(transformer_dsn())
    reports = checkpoint.run(url, tables, schema=schema)
    _raise_on_failures(
        "check quality",
        [
            f"{r.table}: "
            + ", ".join(
                f"{f.expectation}" + (f"({f.column})" if f.column else "") for f in r.failures
            )
            for r in reports
            if not r.ok
        ],
    )
    return [asdict(r) for r in reports]


# Anomaly kinds that fail the monitoring task. Stale data and odd volumes are
# reported (XCom, logs, optional LLM wording) but do not turn the run red: the
# open data publishes with a lag the pipeline cannot fix, and a run that loaded
# and validated everything should not be retried because of it.
BLOCKING_ANOMALIES = frozenset({"failed", "missing", "empty"})


def monitor(
    *,
    settings: Settings | None = None,
    conn=None,
    now=None,
    use_llm: bool | None = None,
    schema: str | None = "raw",
    placeholder: str = "%s",
) -> dict:
    """Freshness + run metrics over `raw.*`, condensed into one anomaly summary.

    Reads as the transformer role (like `make monitor`). The LLM wording is only
    attempted when `LLM_SUMMARY_ENABLED=1` and something is wrong; the
    rule-based digest is always in the result.
    """
    import logging

    from railfall.monitoring import freshness, runs, summary

    settings = settings or Settings.from_env()
    if conn is None:
        import psycopg

        from railfall.transform.verify import transformer_dsn

        with psycopg.connect(transformer_dsn(), connect_timeout=10, autocommit=True) as own:
            return monitor(settings=settings, conn=own, now=now, use_llm=use_llm, schema=schema)

    fresh = [
        r.as_dict() for r in freshness.check(conn, now=now, schema=schema, placeholder=placeholder)
    ]
    recent = runs.recent_runs(conn, schema=schema, placeholder=placeholder)
    volumes = runs.volume_checks(conn, schema=schema)
    run_report = runs.report(recent, volumes)
    result = summary.summarize(summary.collect(fresh, run_report), settings, use_llm=use_llm)
    logging.getLogger(__name__).info("monitoring summary (%s):\n%s", result.source, result.text)
    _raise_on_failures(
        "monitor", [a.line() for a in result.anomalies if a.kind in BLOCKING_ANOMALIES]
    )
    return {"summary": result.as_dict(), "freshness": fresh, "runs": run_report}
