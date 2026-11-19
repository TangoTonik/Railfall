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
* `data_catalog(manifest, catalog)` joins both files per relation (warehouse
  types, declared descriptions, upstream), `schema_drift(...)` lists what the
  docs describe but the warehouse lacks, and `catalog_markdown(...)` renders it.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
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


# --- data catalog: manifest (what we declare) x catalog.json (what exists) -----


@dataclass(frozen=True)
class CatalogColumn:
    name: str
    type: str | None
    description: str
    declared: bool  # listed in a schema.yml
    in_warehouse: bool


@dataclass(frozen=True)
class CatalogEntry:
    unique_id: str
    name: str
    layer: str
    relation: str
    kind: str  # "view", "table", "source", ... as the warehouse reports it when known
    description: str
    upstream: tuple[str, ...]
    columns: tuple[CatalogColumn, ...]
    in_warehouse: bool


def _warehouse_kind(entry: Mapping[str, Any] | None, node: Mapping[str, Any]) -> str:
    if entry is not None:
        raw = str((entry.get("metadata") or {}).get("type") or "").lower()
        return {"base table": "table"}.get(raw, raw or "unknown")
    if node["resource_type"] == "source":
        return "source"
    return str((node.get("config") or {}).get("materialized") or node["resource_type"])


def _catalog_columns(
    node: Mapping[str, Any], entry: Mapping[str, Any] | None
) -> tuple[CatalogColumn, ...]:
    declared = {name.lower(): col for name, col in (node.get("columns") or {}).items()}
    actual = {
        name.lower(): col
        for name, col in sorted(
            ((entry or {}).get("columns") or {}).items(), key=lambda kv: kv[1].get("index", 0)
        )
    }
    # Warehouse order first (the order a SELECT * returns), then declared-only columns.
    names = [*actual, *(n for n in declared if n not in actual)]
    return tuple(
        CatalogColumn(
            name=name,
            type=(actual.get(name) or {}).get("type"),
            description=((declared.get(name) or {}).get("description") or "").strip(),
            declared=name in declared,
            in_warehouse=name in actual,
        )
        for name in names
    )


def data_catalog(
    manifest: Mapping[str, Any], catalog: Mapping[str, Any] | None = None
) -> list[CatalogEntry]:
    """One entry per project relation, upstream first (by layer, then name)."""
    graph = lineage(manifest)
    warehouse = {**((catalog or {}).get("nodes") or {}), **((catalog or {}).get("sources") or {})}
    raw = {n["unique_id"]: n for n in documented_nodes(manifest)}
    entries = []
    for layer_nodes in graph.by_layer().values():
        for ln in layer_nodes:
            node, entry = raw[ln.unique_id], warehouse.get(ln.unique_id)
            entries.append(
                CatalogEntry(
                    unique_id=ln.unique_id,
                    name=ln.name,
                    layer=ln.layer,
                    relation=ln.relation,
                    kind=_warehouse_kind(entry, node),
                    description=(node.get("description") or "").strip(),
                    upstream=tuple(graph.nodes[p].name for p in graph.parents(ln.unique_id)),
                    columns=_catalog_columns(node, entry),
                    in_warehouse=entry is not None,
                )
            )
    return entries


@dataclass
class SchemaDrift:
    """Differences between the documented schema and the warehouse.

    `missing_relations` and `missing_columns` (declared, not in the warehouse) mean
    the docs describe something that does not exist: a hard error. `undeclared_columns`
    exist but have no schema.yml entry: allowed (technical columns such as loaded_at),
    but listed so the catalog says which columns are undocumented.
    """

    missing_relations: list[str] = field(default_factory=list)
    missing_columns: list[str] = field(default_factory=list)
    undeclared_columns: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.missing_relations and not self.missing_columns

    def to_dict(self) -> dict[str, Any]:
        out = vars(self).copy()
        out["ok"] = self.ok
        return out


def schema_drift(entries: Iterable[CatalogEntry]) -> SchemaDrift:
    drift = SchemaDrift()
    for e in entries:
        if not e.in_warehouse:
            drift.missing_relations.append(e.unique_id)
            continue
        for c in e.columns:
            if c.declared and not c.in_warehouse:
                drift.missing_columns.append(f"{e.unique_id}.{c.name}")
            elif c.in_warehouse and not c.declared:
                drift.undeclared_columns.append(f"{e.unique_id}.{c.name}")
    return drift


def _cell(text: str | None) -> str:
    return (text or "").replace("|", "\\|").replace("\n", " ").strip()


def catalog_markdown(entries: Sequence[CatalogEntry], *, title: str = "Data catalog") -> str:
    """A README-ready catalog: an index table, then one column table per relation."""
    lines = [f"# {title}", "", "| Layer | Relation | Kind | Description | Built from |"]
    lines.append("|---|---|---|---|---|")
    for e in entries:
        anchor = e.relation.replace(".", "")
        lines.append(
            f"| {e.layer} | [`{e.relation}`](#{anchor}) | {e.kind} | {_cell(e.description)} "
            f"| {', '.join(f'`{u}`' for u in e.upstream)} |"
        )
    for e in entries:
        lines += ["", f"## {e.relation}", ""]
        if e.description:
            lines += [e.description, ""]
        if not e.in_warehouse:
            lines += ["> Not found in the warehouse when the catalog was generated.", ""]
        lines += ["| Column | Type | Description |", "|---|---|---|"]
        for c in e.columns:
            flag = "" if c.in_warehouse else " *(not in warehouse)*"
            lines.append(f"| `{c.name}` | {c.type or '?'} | {_cell(c.description)}{flag} |")
    return "\n".join(lines) + "\n"
