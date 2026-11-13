"""Spark and warehouse tasks for the full `railfall_pipeline` DAG.

Each function runs one step of `make silver` / `make verify-silver` (and, below,
the warehouse load) inside an Airflow task. Like `railfall.orchestration.ingest`:

* it returns a JSON-serialisable summary (the task's XCom value);
* it raises `TaskFailed` when any table failed, after the other tables were
  still processed, so Airflow marks the task failed and retries it. Silver
  writes are partition overwrites, so a retry rewrites instead of duplicating.

The SparkSession can be injected (tests share one); otherwise each task starts
its own session and always stops it, so a failed task never leaks a JVM into
the worker. `root=None` means the MinIO lake over S3A, exactly like the CLI; a
local directory works for development and tests.
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
