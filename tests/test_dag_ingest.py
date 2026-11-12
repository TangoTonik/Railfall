"""The `railfall_ingest` DAG parses and has the intended shape.

Needs Airflow, which is not a dev dependency (it pins half the ecosystem):
skipped unless it is importable, e.g. inside the Airflow image or a venv built
with Airflow's constraints. The task logic is tested without Airflow in
test_orchestration_ingest.py.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

DAG_FILE = Path(__file__).parent.parent / "dags" / "railfall_ingest.py"


def test_dag_tasks_only_call_existing_orchestration_functions() -> None:
    """Runs everywhere: a renamed task callable must break CI, not the scheduler."""
    from railfall.orchestration import ingest

    tree = ast.parse(DAG_FILE.read_text())
    called = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "ingest"
    }
    assert called == {"ingest_sncf", "verify_sncf", "ingest_meteo", "verify_meteo"}
    for name in called:
        assert callable(getattr(ingest, name))


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
    yield bag.dags["railfall_ingest"]
    mp.undo()


def test_dag_shape(dag) -> None:
    downstream = {t.task_id: sorted(t.downstream_task_ids) for t in dag.tasks}
    assert downstream == {
        "ingest_sncf": ["verify_sncf"],
        "verify_sncf": [],
        "ingest_meteo": ["verify_meteo"],
        "verify_meteo": [],
    }


def test_meteo_is_mapped_over_configured_departments(dag) -> None:
    task = dag.get_task("ingest_meteo")
    assert task.op_kwargs_expand_input.value["department"] == ["75", "69", "13"]


def test_schedule_and_safety_settings(dag) -> None:
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.params.dump() == {"ingest_date": None}
    for task in dag.tasks:
        assert task.retries == 2
