"""Daily end-to-end pipeline: open data -> bronze -> silver (Spark) -> warehouse.

    ingest_sncf ──> verify_sncf ──┐
    ingest_meteo[dept] ──> verify_meteo ──┴─> build_silver ──> verify_silver
        ──> load_warehouse ──> verify_warehouse

The ingestion half is the same as `railfall_ingest`; the rest runs the Spark
transforms and the silver -> `raw.*` merge through
`railfall.orchestration.pipeline`. Each Spark task starts (and stops) its own
local SparkSession in the worker, reading and writing the MinIO lake over S3A.

Spark only starts once *both* bronze verifications passed: a silver build on a
half-landed day would publish partial months. Every step is idempotent (bronze
skips known content, silver overwrites partitions, the load merges on keys),
so retrying a task or re-triggering a day never duplicates rows.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from railfall.config import Settings
from railfall.orchestration import ingest, pipeline

INGEST_DATE = "{{ params.ingest_date or (data_interval_end | ds) }}"

default_args = {
    "owner": "railfall",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}

# Spark steps rebuild every silver table from the whole bronze history; on a
# cold worker the first one also resolves the hadoop-aws jars.
SPARK_TIMEOUT = timedelta(minutes=60)


@dag(
    dag_id="railfall_pipeline",
    description="Ingest the open data, build silver with Spark and load the warehouse",
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
    tags=["railfall", "bronze", "silver", "warehouse"],
    doc_md=__doc__,
)
def railfall_pipeline():
    departments = list(Settings.from_env().meteo_departments)

    @task
    def ingest_sncf(ingest_date: str) -> list[dict]:
        return ingest.ingest_sncf(ingest_date)

    @task
    def verify_sncf(ingest_date: str) -> list[dict]:
        return ingest.verify_sncf(ingest_date)

    @task(map_index_template="{{ department }}")
    def ingest_meteo(ingest_date: str, department: str) -> list[dict]:
        get_current_context()["department"] = department
        return ingest.ingest_meteo(ingest_date, department)

    @task
    def verify_meteo(ingest_date: str) -> list[dict]:
        return ingest.verify_meteo(ingest_date, departments=departments)

    @task(execution_timeout=SPARK_TIMEOUT)
    def build_silver(ingest_date: str) -> list[dict]:
        return pipeline.build_silver(ingest_date)

    @task(execution_timeout=SPARK_TIMEOUT)
    def verify_silver() -> list[dict]:
        return pipeline.verify_silver()

    @task(execution_timeout=SPARK_TIMEOUT)
    def load_warehouse() -> list[dict]:
        return pipeline.load_warehouse()

    @task(execution_timeout=SPARK_TIMEOUT)
    def verify_warehouse() -> list[dict]:
        return pipeline.verify_warehouse()

    bronze_sncf = ingest_sncf(INGEST_DATE) >> verify_sncf(INGEST_DATE)
    bronze_meteo = ingest_meteo.partial(ingest_date=INGEST_DATE).expand(
        department=departments
    ) >> verify_meteo(INGEST_DATE)
    silver = build_silver(INGEST_DATE)
    [bronze_sncf, bronze_meteo] >> silver
    silver >> verify_silver() >> load_warehouse() >> verify_warehouse()


railfall_pipeline()
