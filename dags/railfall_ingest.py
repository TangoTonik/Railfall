"""Daily ingestion of the open data into bronze: SNCF + Météo-France, then verified.

    ingest_sncf ──> verify_sncf
    ingest_meteo[dept] (one mapped task per METEO_DEPARTMENTS entry) ──> verify_meteo

Tasks are thin wrappers around `railfall.orchestration.ingest`; all logic and
its tests live in the package. The `ingest_date` partition defaults to the end
of the run's data interval (the day the run downloads), and can be forced with
the `ingest_date` param when triggering by hand or from e2e.

Re-running a day is safe: bronze landing skips content it already holds.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from railfall.config import Settings
from railfall.orchestration import ingest

INGEST_DATE = "{{ params.ingest_date or (data_interval_end | ds) }}"

default_args = {
    "owner": "railfall",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


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
    },
    tags=["railfall", "bronze", "ingest"],
    doc_md=__doc__,
)
def railfall_ingest():
    departments = list(Settings.from_env().meteo_departments)

    @task
    def ingest_sncf(ingest_date: str) -> list[dict]:
        return ingest.ingest_sncf(ingest_date)

    @task
    def verify_sncf(ingest_date: str) -> list[dict]:
        return ingest.verify_sncf(ingest_date)

    @task(map_index_template="{{ department }}")
    def ingest_meteo(ingest_date: str, department: str) -> list[dict]:
        # Label the mapped instance with its department in the UI.
        get_current_context()["department"] = department
        return ingest.ingest_meteo(ingest_date, department)

    @task
    def verify_meteo(ingest_date: str) -> list[dict]:
        return ingest.verify_meteo(ingest_date, departments=departments)

    ingest_sncf(INGEST_DATE) >> verify_sncf(INGEST_DATE)
    (
        ingest_meteo.partial(ingest_date=INGEST_DATE).expand(department=departments)
        >> verify_meteo(INGEST_DATE)
    )


railfall_ingest()
