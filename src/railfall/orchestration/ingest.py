"""Ingestion tasks for the `railfall_ingest` DAG.

Each function runs one step of `make ingest-*` / `make verify-*` for a given
`ingest_date` partition and:

* returns a JSON-serialisable summary (it becomes the task's XCom value);
* raises `TaskFailed` when any dataset failed, after the others were still
  attempted, so Airflow marks the task failed and retries it. Bronze landing is
  idempotent (same content -> "skipped"), so a retry never duplicates data.

The lake client, fetcher and row floor can be injected for tests; in Airflow
they come from the environment exactly like the CLI.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict
from datetime import date

from railfall.config import Settings
from railfall.ingest import run, verify
from railfall.ingest.http import Download, fetch

Summary = list[dict]


class TaskFailed(RuntimeError):
    """Some datasets failed; the message lists them, the others were still processed."""


def _date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _fetcher(settings: Settings) -> Callable[[str], Download]:
    def _fetch(url: str) -> Download:
        return fetch(url, timeout_s=settings.http_timeout_s, max_retries=settings.http_max_retries)

    return _fetch


def _raise_on_failures(step: str, failures: Sequence[str]) -> None:
    if failures:
        raise TaskFailed(f"{step}: {len(failures)} dataset(s) failed: " + "; ".join(failures))


def ingest_sncf(
    ingest_date: str | date,
    *,
    datasets: Iterable[str] | None = None,
    settings: Settings | None = None,
    client=None,
    fetcher: Callable[[str], Download] | None = None,
    min_rows: int | None = None,
) -> Summary:
    settings = settings or Settings.from_env()
    results = run.ingest_sncf(
        client if client is not None else run.lake_client(settings),
        settings.lake_bucket,
        fetcher=fetcher or _fetcher(settings),
        names=datasets,
        ingest_date=_date(ingest_date),
        min_rows=min_rows,
    )
    _raise_on_failures("ingest sncf", [f"{r.dataset}: {r.error}" for r in results if not r.ok])
    return [asdict(r) for r in results]


def ingest_meteo(
    ingest_date: str | date,
    department: str | int,
    *,
    periods: Iterable[str] = run.METEO_DEFAULT_PERIODS,
    settings: Settings | None = None,
    client=None,
    fetcher: Callable[[str], Download] | None = None,
    min_rows: int | None = None,
) -> Summary:
    """One department per task, so one slow or missing department retries alone."""
    settings = settings or Settings.from_env()
    results = run.ingest_meteo(
        client if client is not None else run.lake_client(settings),
        settings.lake_bucket,
        fetcher=fetcher or _fetcher(settings),
        departments=[department],
        periods=periods,
        ingest_date=_date(ingest_date),
        min_rows=min_rows,
    )
    _raise_on_failures(
        f"ingest meteo {department}", [f"{r.dataset}: {r.error}" for r in results if not r.ok]
    )
    return [asdict(r) for r in results]


def _checks(step: str, checks: list[verify.Check]) -> Summary:
    _raise_on_failures(step, [f"{c.dataset}: {'; '.join(c.problems)}" for c in checks if not c.ok])
    return [asdict(c) for c in checks]


def verify_sncf(
    ingest_date: str | date,
    *,
    settings: Settings | None = None,
    client=None,
    min_rows: int | None = None,
) -> Summary:
    settings = settings or Settings.from_env()
    checks = verify.verify_source(
        client if client is not None else run.lake_client(settings),
        settings.lake_bucket,
        verify.SNCF_SOURCE,
        _date(ingest_date),
        min_rows=min_rows,
    )
    return _checks("verify sncf", checks)


def verify_meteo(
    ingest_date: str | date,
    *,
    departments: Iterable[str | int] | None = None,
    periods: Iterable[str] = run.METEO_DEFAULT_PERIODS,
    settings: Settings | None = None,
    client=None,
    min_rows: int | None = None,
) -> Summary:
    settings = settings or Settings.from_env()
    checks = verify.verify_meteo(
        client if client is not None else run.lake_client(settings),
        settings.lake_bucket,
        _date(ingest_date),
        departments=departments if departments is not None else settings.meteo_departments,
        periods=periods,
        min_rows=min_rows,
    )
    return _checks("verify meteo", checks)
