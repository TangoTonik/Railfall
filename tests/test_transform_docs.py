"""railfall.transform.docs: lineage and documentation coverage from dbt artifacts.

Most tests read a small hand-written manifest under tests/fixtures/dbt; the last
ones run the real `dbt parse` (no database needed) and check the project's own
lineage, so a model wired to the wrong layer fails here before it reaches CI.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from railfall.transform import docs

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "dbt"


@pytest.fixture
def manifest():
    return docs.load_json(FIXTURES / "manifest.json")


def test_lineage_keeps_queryable_nodes_of_the_project_only(manifest):
    graph = docs.lineage(manifest)
    assert set(graph.nodes) == {
        "source.railfall.raw.punctuality_monthly",
        "seed.railfall.departments",
        "model.railfall.stg_sncf__punctuality_monthly",
        "model.railfall.dim_route",
        "model.railfall.fact_route_month",
    }  # no test node, no model of another package


def test_lineage_edges_follow_depends_on(manifest):
    graph = docs.lineage(manifest)
    assert graph.parents("model.railfall.dim_route") == [
        "model.railfall.stg_sncf__punctuality_monthly",
        "seed.railfall.departments",
    ]
    assert graph.children("model.railfall.stg_sncf__punctuality_monthly") == [
        "model.railfall.dim_route",
        "model.railfall.fact_route_month",
    ]
    assert graph.roots() == [
        "seed.railfall.departments",
        "source.railfall.raw.punctuality_monthly",
    ]
    assert graph.leaves() == ["model.railfall.fact_route_month"]


def test_upstream_is_transitive(manifest):
    graph = docs.lineage(manifest)
    assert graph.upstream("model.railfall.fact_route_month") == {
        "model.railfall.dim_route",
        "model.railfall.stg_sncf__punctuality_monthly",
        "seed.railfall.departments",
        "source.railfall.raw.punctuality_monthly",
    }
    assert graph.upstream("source.railfall.raw.punctuality_monthly") == set()


def test_layers_and_relations(manifest):
    graph = docs.lineage(manifest)
    layers = {layer: [n.name for n in nodes] for layer, nodes in graph.by_layer().items()}
    assert list(layers) == ["raw", "seeds", "staging", "marts"]
    assert layers["marts"] == ["dim_route", "fact_route_month"]
    src = graph.nodes["source.railfall.raw.punctuality_monthly"]
    assert (src.relation, src.materialized) == ("raw.punctuality_monthly", None)
    dim = graph.nodes["model.railfall.dim_route"]
    assert (dim.relation, dim.materialized) == ("marts.dim_route", "table")


def test_mermaid_has_a_subgraph_per_layer_and_every_edge(manifest):
    graph = docs.lineage(manifest)
    chart = docs.lineage_mermaid(graph)
    lines = chart.splitlines()
    assert lines[0] == "flowchart LR"
    assert [ln.strip() for ln in lines if ln.strip().startswith("subgraph")] == [
        "subgraph raw",
        "subgraph seeds",
        "subgraph staging",
        "subgraph marts",
    ]
    assert sum("-->" in ln for ln in lines) == len(graph.edges) == 5
    # Sources and seeds are drawn as cylinders, models as boxes; ids have no dots.
    assert 'source_railfall_raw_punctuality_monthly[("punctuality_monthly")]' in chart
    assert 'model_railfall_dim_route["dim_route"]' in chart


def test_lineage_dict_is_stable(manifest):
    out = docs.lineage(manifest).to_dict()
    assert [n["unique_id"] for n in out["nodes"]] == sorted(n["unique_id"] for n in out["nodes"])
    assert {"from": "model.railfall.dim_route", "to": "model.railfall.fact_route_month"} in out[
        "edges"
    ]


def test_doc_coverage_lists_missing_descriptions(manifest):
    cov = docs.doc_coverage(manifest)
    assert cov.relations == 5
    assert cov.missing_relations == ["model.railfall.dim_route"]
    assert cov.missing_columns == [
        "model.railfall.dim_route.route_id",
        "seed.railfall.departments.region_code",
    ]
    assert cov.columns == 6
    assert cov.pct_columns_documented == pytest.approx(66.7)
    assert not cov.ok
    assert cov.to_dict()["ok"] is False


def test_empty_manifest_is_fully_documented():
    cov = docs.doc_coverage({"nodes": {}, "sources": {}})
    assert cov.ok and cov.pct_columns_documented == 100.0


# --- the real project ---------------------------------------------------------

needs_dbt = pytest.mark.skipif(
    importlib.util.find_spec("dbt") is None, reason="dbt-core is not installed"
)


@pytest.fixture(scope="module")
def real_manifest(tmp_path_factory):
    from railfall.transform import dbt as tdbt

    target = tmp_path_factory.mktemp("dbt") / "target"
    mp = pytest.MonkeyPatch()
    mp.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    mp.setenv("DBT_TARGET_PATH", str(target))
    mp.setenv("DBT_LOG_PATH", str(target.parent / "logs"))
    try:
        report = tdbt.run_dbt("parse")
    finally:
        mp.undo()
    assert report.ok, report.errors
    return docs.load_json(target / "manifest.json")


@needs_dbt
def test_project_lineage_flows_raw_to_staging_to_marts(real_manifest):
    graph = docs.lineage(real_manifest)
    layers = graph.by_layer()
    assert list(layers) == ["raw", "seeds", "staging", "marts"]
    # Each staging model is a 1:1 window on exactly one raw table.
    for node in layers["staging"]:
        parents = graph.parents(node.unique_id)
        assert len(parents) == 1 and parents[0].startswith("source.railfall.raw."), node.name
    # Marts never read raw directly, and every mart traces back to raw.
    for node in layers["marts"]:
        assert not any(p.startswith("source.") for p in graph.parents(node.unique_id)), node.name
        assert any(u.startswith("source.") for u in graph.upstream(node.unique_id)), node.name
    # Only marts are end points, plus two known ones: daily weather is exposed
    # as-is for the statistical analysis (Spark already rolls it up monthly), and
    # raw.route_weather_monthly, Spark's joined table, is declared (and tested)
    # as a source but the star schema rebuilds that join in int_route_month_weather.
    dangling = {graph.nodes[leaf].name for leaf in graph.leaves()} - {
        n.name for n in layers["marts"]
    }
    assert dangling == {"stg_meteo__weather_daily", "route_weather_monthly"}
