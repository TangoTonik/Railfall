"""Lineage and documentation coverage from dbt's own artifacts.

`dbt docs generate` writes `manifest.json` (what the project declares: models,
sources, seeds, tests, descriptions, dependencies) and `catalog.json` (what the
warehouse actually holds: relations and column types). This module reads those
files only; it never talks to dbt or Postgres, so it is cheap to unit test and
runs the same on a laptop and in CI.

* `lineage(manifest)` is the model-level DAG, sources -> staging -> marts,
  without the tests (they are leaves that would triple the graph).
* `lineage_mermaid(...)` renders it as a Mermaid flowchart, one subgraph per
  layer, so it can sit in the README and in the CI job summary.
* `doc_coverage(manifest)` lists every model / source / seed and column that has
  no description, so CI can refuse an undocumented column.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Resource types that are tables or views someone can query.
DOCUMENTED_TYPES = ("source", "seed", "model")
# Display order of the lineage layers, upstream first.
LAYERS = ("raw", "seeds", "staging", "marts")


@dataclass(frozen=True)
class LineageNode:
    unique_id: str
    name: str
    resource_type: str
    layer: str
    relation: str
    materialized: str | None = None


@dataclass
class Lineage:
    nodes: dict[str, LineageNode] = field(default_factory=dict)
    edges: set[tuple[str, str]] = field(default_factory=set)

    def parents(self, unique_id: str) -> list[str]:
        return sorted(p for p, c in self.edges if c == unique_id)

    def children(self, unique_id: str) -> list[str]:
        return sorted(c for p, c in self.edges if p == unique_id)

    def upstream(self, unique_id: str) -> set[str]:
        """Every node `unique_id` is (transitively) built from."""
        seen: set[str] = set()
        todo = [unique_id]
        while todo:
            for parent in self.parents(todo.pop()):
                if parent not in seen:
                    seen.add(parent)
                    todo.append(parent)
        return seen

    def roots(self) -> list[str]:
        return sorted(n for n in self.nodes if not self.parents(n))

    def leaves(self) -> list[str]:
        return sorted(n for n in self.nodes if not self.children(n))

    def by_layer(self) -> dict[str, list[LineageNode]]:
        out: dict[str, list[LineageNode]] = {}
        for node in sorted(self.nodes.values(), key=lambda n: n.name):
            out.setdefault(node.layer, []).append(node)
        order = {layer: i for i, layer in enumerate(LAYERS)}
        return dict(sorted(out.items(), key=lambda kv: (order.get(kv[0], len(order)), kv[0])))

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": [vars(n) for n in sorted(self.nodes.values(), key=lambda n: n.unique_id)],
            "edges": [{"from": p, "to": c} for p, c in sorted(self.edges)],
        }


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def _layer(node: Mapping[str, Any]) -> str:
    rtype = node["resource_type"]
    if rtype == "source":
        return str(node.get("schema") or "raw")
    if rtype == "seed":
        return "seeds"
    # Models: the folder under models/ (staging, marts) is the layer; fall back
    # to the schema for a model sitting at the root.
    fqn = node.get("fqn") or []
    return str(fqn[1]) if len(fqn) > 2 else str(node.get("schema") or "models")


def _relation(node: Mapping[str, Any]) -> str:
    name = node.get("identifier") or node.get("alias") or node["name"]
    return f"{node['schema']}.{name}"


def _lineage_node(node: Mapping[str, Any]) -> LineageNode:
    config = node.get("config") or {}
    return LineageNode(
        unique_id=node["unique_id"],
        name=node["name"],
        resource_type=node["resource_type"],
        layer=_layer(node),
        relation=_relation(node),
        materialized=config.get("materialized") if node["resource_type"] != "source" else None,
    )


def documented_nodes(manifest: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    """Models, seeds and sources of the project itself (not of installed packages)."""
    project = (manifest.get("metadata") or {}).get("project_name")
    pool = [*manifest.get("nodes", {}).values(), *manifest.get("sources", {}).values()]
    for node in pool:
        if node["resource_type"] not in DOCUMENTED_TYPES:
            continue
        if project and node.get("package_name") not in (None, project):
            continue
        yield node


def lineage(manifest: Mapping[str, Any]) -> Lineage:
    graph = Lineage()
    for node in documented_nodes(manifest):
        graph.nodes[node["unique_id"]] = _lineage_node(node)
    for node in documented_nodes(manifest):
        for parent in (node.get("depends_on") or {}).get("nodes", []):
            if parent in graph.nodes:
                graph.edges.add((parent, node["unique_id"]))
    return graph


def _mermaid_id(unique_id: str) -> str:
    return unique_id.replace(".", "_").replace("-", "_")


def lineage_mermaid(graph: Lineage) -> str:
    """A `flowchart LR` with one subgraph per layer; sources and seeds are cylinders."""
    lines = ["flowchart LR"]
    for layer, nodes in graph.by_layer().items():
        lines.append(f"  subgraph {layer}")
        for node in nodes:
            nid = _mermaid_id(node.unique_id)
            shape = f'[("{node.name}")]' if node.resource_type != "model" else f'["{node.name}"]'
            lines.append(f"    {nid}{shape}")
        lines.append("  end")
    for parent, child in sorted(graph.edges):
        lines.append(f"  {_mermaid_id(parent)} --> {_mermaid_id(child)}")
    return "\n".join(lines) + "\n"


@dataclass
class DocCoverage:
    """What has a description and what does not, per relation and per column."""

    relations: int = 0
    columns: int = 0
    missing_relations: list[str] = field(default_factory=list)
    missing_columns: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.missing_relations and not self.missing_columns

    @property
    def pct_columns_documented(self) -> float:
        if not self.columns:
            return 100.0
        return round(100.0 * (self.columns - len(self.missing_columns)) / self.columns, 1)

    def to_dict(self) -> dict[str, Any]:
        out = vars(self).copy()
        out["ok"] = self.ok
        out["pct_columns_documented"] = self.pct_columns_documented
        return out


def doc_coverage(manifest: Mapping[str, Any]) -> DocCoverage:
    cov = DocCoverage()
    for node in sorted(documented_nodes(manifest), key=lambda n: n["unique_id"]):
        cov.relations += 1
        if not (node.get("description") or "").strip():
            cov.missing_relations.append(node["unique_id"])
        for name, column in sorted((node.get("columns") or {}).items()):
            cov.columns += 1
            if not (column.get("description") or "").strip():
                cov.missing_columns.append(f"{node['unique_id']}.{name}")
    return cov
