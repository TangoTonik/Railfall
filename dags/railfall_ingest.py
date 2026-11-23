"""Daily ingestion of the open data into bronze: SNCF + Météo-France, then verified.

    ingest_sncf ──> verify_sncf
    meteo_departments ──> ingest_meteo[dept] (one mapped task per department) ──> verify_meteo

Tasks are thin wrappers around `railfall.orchestration.ingest`; all logic and
its tests live in the package. The `ingest_date` partition defaults to the end
of the run's data interval (the day the run downloads), and can be forced with
the `ingest_date` param when triggering by hand or from e2e. The departments
default to METEO_DEPARTMENTS; the `departments` param narrows a run to others
(a backfill of one department, say). An invalid code fails `meteo_departments`
at once, without retries, and nothing is downloaded for Météo-France.

Re-running a day is safe: bronze landing skips content it already holds.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from railfall.config import Settings
from railfall.orchestration import ingest, retry
from railfall.orchestration.retry import fail_fast

INGEST_DATE = "{{ params.ingest_date or (data_interval_end | ds) }}"

# Transient failures retry with exponential backoff (2, 4, 8 min); tasks are
# wrapped in `fail_fast`, so a contract violation fails at once instead.
default_args = retry.task_defaults()


@dag(
    dag_id="railfall_ingest",
    description="Download SNCF + Météo-France open data into bronze and verify it",
    schedule="15 5 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "ingest_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="bronze ingest_date partition (YYYY-MM-DD); default: interval end",
        ),
        "departments": Param(
            None,
            type=["null", "array"],
            items={"type": "string"},
            description="INSEE department codes to ingest; default: METEO_DEPARTMENTS",
        ),
    },
    tags=["railfall", "bronze", "ingest"],
    doc_md=__doc__,
)
def railfall_ingest():
    configured = list(Settings.from_env().meteo_departments)

    @task
    @fail_fast
    def meteo_departments() -> list[str]:
        requested = get_current_context()["params"].get("departments")
        return ingest.resolve_departments(requested, default=configured)

    @task
    @fail_fast
    def ingest_sncf(ingest_date: str) -> list[dict]:
        return ingest.ingest_sncf(ingest_date)

    @task
    @fail_fast
    def verify_sncf(ingest_date: str) -> list[dict]:
        return ingest.verify_sncf(ingest_date)

    @task(map_index_template="{{ department }}")
    @fail_fast
    def ingest_meteo(ingest_date: str, department: str) -> list[dict]:
        # Label the mapped instance with its department in the UI.
        get_current_context()["department"] = department
        return ingest.ingest_meteo(ingest_date, department)

    @task
    @fail_fast
    def verify_meteo(ingest_date: str, departments: list[str]) -> list[dict]:
        return ingest.verify_meteo(ingest_date, departments=departments)

    ingest_sncf(INGEST_DATE) >> verify_sncf(INGEST_DATE)
    departments = meteo_departments()
    (
        ingest_meteo.partial(ingest_date=INGEST_DATE).expand(department=departments)
        >> verify_meteo(INGEST_DATE, departments)
    )


railfall_ingest()
