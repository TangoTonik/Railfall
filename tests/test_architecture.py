"""The architecture diagram must name what the code actually runs.

docs/architecture.md is hand-drawn, so these checks keep it honest: every
Compose service, every task of the pipeline DAG and every warehouse schema
Terraform creates has to appear in the Mermaid block.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "architecture.md"
README = ROOT / "README.md"


def _mermaid_block(text: str) -> str:
    blocks = re.findall(r"```mermaid\n(.*?)```", text, flags=re.S)
    assert len(blocks) == 1, "docs/architecture.md must hold exactly one mermaid block"
    return blocks[0]


@pytest.fixture(scope="module")
def diagram() -> str:
    return _mermaid_block(DOC.read_text(encoding="utf-8"))


def _dag_tasks(path: Path) -> set[str]:
    """Names of the functions decorated with @task inside the DAG file."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    tasks = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for deco in node.decorator_list:
            target = deco.func if isinstance(deco, ast.Call) else deco
            if isinstance(target, ast.Name) and target.id == "task":
                tasks.add(node.name)
    return tasks


def _terraform_schemas() -> list[str]:
    text = (ROOT / "infra" / "terraform" / "postgres.tf").read_text(encoding="utf-8")
    match = re.search(r"warehouse_schemas\s*=\s*\[([^\]]*)\]", text)
    assert match, "warehouse_schemas not found in postgres.tf"
    return re.findall(r'"([a-z_]+)"', match.group(1))


def test_every_compose_service_is_drawn(diagram):
    services = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    missing = [s for s in services if f"{s}" not in diagram]
    assert not missing, f"compose services missing from the diagram: {missing}"


def test_every_pipeline_task_is_drawn(diagram):
    tasks = _dag_tasks(ROOT / "dags" / "railfall_pipeline.py")
    assert len(tasks) >= 10
    missing = sorted(t for t in tasks if not re.search(rf"\b{t}\b", diagram))
    assert not missing, f"pipeline tasks missing from the diagram: {missing}"


def test_no_task_is_drawn_that_the_dag_lacks(diagram):
    """A renamed or removed task must not linger in the picture."""
    tasks = _dag_tasks(ROOT / "dags" / "railfall_pipeline.py")
    class_line = next(ln for ln in diagram.splitlines() if ln.strip().startswith("class ingest"))
    drawn = set(class_line.split()[1].split(","))
    assert drawn == tasks


def test_every_warehouse_schema_is_drawn(diagram):
    schemas = _terraform_schemas()
    assert schemas == ["raw", "staging", "marts"]
    for schema in schemas:
        assert re.search(rf'\b{schema}\[\("{schema}\b', diagram), schema


def test_lake_layers_are_drawn(diagram):
    assert "bronze/" in diagram and "silver/" in diagram


def test_readme_links_to_architecture():
    assert "docs/architecture.md" in README.read_text(encoding="utf-8")


def test_readme_diagram_matches_the_doc(diagram):
    """The README shows the same diagram; the first mermaid block there is it."""
    readme = README.read_text(encoding="utf-8")
    first = re.search(r"```mermaid\n(.*?)```", readme, flags=re.S)
    assert first and first.group(1) == diagram, "copy docs/architecture.md's diagram to README"
