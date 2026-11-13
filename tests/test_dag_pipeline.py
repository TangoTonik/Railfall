"""The `railfall_pipeline` DAG parses and chains bronze -> silver -> warehouse.

Like test_dag_ingest.py, the DagBag tests need Airflow and are skipped without
it; the AST check runs everywhere so a renamed callable breaks CI first.
"""

from __future__ import annotations

import ast
from datetime import timedelta
from pathlib import Path

import pytest

DAG_FILE = Path(__file__).parent.parent / "dags" / "railfall_pipeline.py"


def _called(module: str) -> set[str]:
    tree = ast.parse(DAG_FILE.read_text())
    return {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == module
    }


def test_dag_tasks_only_call_existing_orchestration_functions() -> None:
    from railfall.orchestration import ingest, pipeline

    assert _called("ingest") == {"ingest_sncf", "verify_sncf", "ingest_meteo", "verify_meteo"}
    assert _called("pipeline") == {
        "build_silver",
        "verify_silver",
        "load_warehouse",
        "verify_warehouse",
    }
    for name in _called("ingest"):
        assert callable(getattr(ingest, name))
    for name in _called("pipeline"):
        assert callable(getattr(pipeline, name))


@pytest.fixture(scope="module")
def dag(tmp_path_factory):
    pytest.importorskip("airflow", reason="Airflow is not installed")
    mp = pytest.MonkeyPatch()
    mp.setenv("AIRFLOW_HOME", str(tmp_path_factory.mktemp("airflow")))
    mp.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
    mp.setenv("METEO_DEPARTMENTS", "75,69,13")
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)
    assert bag.import_errors == {}
    yield bag.dags["railfall_pipeline"]
    mp.undo()


def test_dag_shape(dag) -> None:
    downstream = {t.task_id: sorted(t.downstream_task_ids) for t in dag.tasks}
    assert downstream == {
        "ingest_sncf": ["verify_sncf"],
        "verify_sncf": ["build_silver"],
        "ingest_meteo": ["verify_meteo"],
        "verify_meteo": ["build_silver"],
        "build_silver": ["verify_silver"],
        "verify_silver": ["load_warehouse"],
        "load_warehouse": ["verify_warehouse"],
        "verify_warehouse": [],
    }


def test_spark_waits_for_both_bronze_checks(dag) -> None:
    silver = dag.get_task("build_silver")
    assert silver.upstream_task_ids == {"verify_sncf", "verify_meteo"}
    assert silver.trigger_rule == "all_success"


def test_spark_tasks_get_a_longer_timeout(dag) -> None:
    for task_id in ("build_silver", "verify_silver", "load_warehouse", "verify_warehouse"):
        assert dag.get_task(task_id).execution_timeout == timedelta(minutes=60)
    assert dag.get_task("ingest_sncf").execution_timeout == timedelta(minutes=30)


def test_schedule_and_safety_settings(dag) -> None:
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.params.dump() == {"ingest_date": None}
    for task in dag.tasks:
        assert task.retries == 2
