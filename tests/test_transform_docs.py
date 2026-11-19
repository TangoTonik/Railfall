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


@pytest.fixture
def catalog():
    return docs.load_json(FIXTURES / "catalog.json")


def test_data_catalog_is_ordered_upstream_first(manifest, catalog):
    entries = docs.data_catalog(manifest, catalog)
    assert [e.layer for e in entries] == ["raw", "seeds", "staging", "marts", "marts"]
    assert entries[0].relation == "raw.punctuality_monthly"


def test_data_catalog_joins_warehouse_types_and_declared_descriptions(manifest, catalog):
    stg = {e.name: e for e in docs.data_catalog(manifest, catalog)}["stg_sncf__punctuality_monthly"]
    assert stg.kind == "view" and stg.in_warehouse
    assert stg.upstream == ("punctuality_monthly",)
    cols = {c.name: c for c in stg.columns}
    assert [c.name for c in stg.columns] == ["route_month_id", "trains_run", "loaded_at"]
    assert cols["trains_run"].type == "integer"
    assert cols["trains_run"].description == "Trains that ran."
    assert cols["loaded_at"].in_warehouse and not cols["loaded_at"].declared


def test_relation_missing_from_warehouse_falls_back_to_materialization(manifest, catalog):
    fact = {e.name: e for e in docs.data_catalog(manifest, catalog)}["fact_route_month"]
    assert not fact.in_warehouse
    assert fact.kind == "table"


def test_data_catalog_without_catalog_json_marks_everything_missing(manifest):
    entries = docs.data_catalog(manifest)
    assert not any(e.in_warehouse for e in entries)
    assert docs.schema_drift(entries).missing_relations == [e.unique_id for e in entries]


def test_schema_drift(manifest, catalog):
    drift = docs.schema_drift(docs.data_catalog(manifest, catalog))
    assert drift.missing_relations == ["model.railfall.fact_route_month"]
    assert drift.missing_columns == []
    assert drift.undeclared_columns == [
        "source.railfall.raw.punctuality_monthly.year",
        "model.railfall.stg_sncf__punctuality_monthly.loaded_at",
    ]
    assert not drift.ok and drift.to_dict()["ok"] is False


def test_declared_column_absent_from_warehouse_is_drift(manifest, catalog):
    del catalog["nodes"]["model.railfall.dim_route"]["columns"]["route_id"]
    drift = docs.schema_drift(docs.data_catalog(manifest, catalog))
    assert drift.missing_columns == ["model.railfall.dim_route.route_id"]


def test_catalog_markdown(manifest, catalog):
    md = docs.catalog_markdown(docs.data_catalog(manifest, catalog))
    assert md.startswith("# Data catalog\n")
    assert "| raw | [`raw.punctuality_monthly`](#rawpunctuality_monthly) | table |" in md
    assert "## staging.stg_sncf__punctuality_monthly" in md
    assert "| `trains_run` | integer | Trains that ran. |" in md
    assert "> Not found in the warehouse" in md  # fact_route_month
    assert md.count("\n## ") == 5


def test_markdown_cells_escape_pipes():
    assert docs._cell("a | b\nc") == "a \\| b c"


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


# --- CLI ---------------------------------------------------------------------------


def _target(tmp_path, *, with_catalog=True):
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text((FIXTURES / "manifest.json").read_text())
    if with_catalog:
        (target / "catalog.json").write_text((FIXTURES / "catalog.json").read_text())
    return target


def test_write_bundle_writes_every_file(tmp_path):
    out = tmp_path / "bundle"
    summary = docs.write_bundle(_target(tmp_path), out)
    assert sorted(p.name for p in out.iterdir()) == sorted(docs.BUNDLE_FILES)
    assert summary["relations"] == 5 and summary["edges"] == 5
    assert summary["drift"]["missing_relations"] == ["model.railfall.fact_route_month"]
    assert not summary["ok"]
    assert (out / "lineage.mmd").read_text().startswith("flowchart LR")
    assert docs.load_json(out / "lineage.json")["edges"]


def test_write_bundle_without_catalog_skips_drift(tmp_path):
    summary = docs.write_bundle(_target(tmp_path, with_catalog=False), tmp_path / "b")
    assert summary["drift"] is None
    assert docs.load_json(tmp_path / "b" / "drift.json") is None


def test_cli_strict_fails_on_gaps_and_lenient_does_not(tmp_path, capsys):
    target = _target(tmp_path)
    assert docs.main(["--target-dir", str(target), "--out-dir", str(tmp_path / "a")]) == 0
    assert (
        docs.main(["--target-dir", str(target), "--out-dir", str(tmp_path / "b"), "--strict"]) == 1
    )
    assert '"relations": 5' in capsys.readouterr().out


def test_cli_without_manifest_is_a_usage_error(tmp_path):
    with pytest.raises(SystemExit) as exc:
        docs.main(["--target-dir", str(tmp_path)])
    assert exc.value.code == 2


def test_cli_reads_dbt_target_path(tmp_path, monkeypatch):
    monkeypatch.setenv("DBT_TARGET_PATH", str(_target(tmp_path)))
    assert docs.main(["--out-dir", str(tmp_path / "out")]) == 0


# --- README catalog block ----------------------------------------------------------


def _readme(tmp_path, body="old"):
    path = tmp_path / "README.md"
    path.write_text(f"# Title\n\n{docs.README_START}\n{body}\n{docs.README_END}\n\n## After\n")
    return path


def test_readme_section_lists_every_relation_with_lineage(manifest):
    section = docs.readme_section(manifest)
    assert section.startswith(docs.README_START) and section.rstrip().endswith(docs.README_END)
    assert "```mermaid\nflowchart LR" in section
    for entry in docs.data_catalog(manifest):
        assert f"`{entry.relation}`" in section
    assert "<details><summary>Columns of the marts layer</summary>" in section


def test_summary_keeps_the_first_sentence():
    assert docs._summary("One route-month.  Grain is month x route.") == "One route-month."
    assert docs._summary("No full stop") == "No full stop"


def test_sync_readme_replaces_only_the_block(tmp_path, manifest):
    path = _readme(tmp_path)
    section = docs.readme_section(manifest)
    assert docs.sync_readme(path, section) is False
    text = path.read_text()
    assert text.startswith("# Title\n\n") and text.endswith("\n\n## After\n")
    assert "old" not in text
    # Idempotent: a second run finds nothing to change.
    assert docs.sync_readme(path, section) is True
    assert path.read_text() == text


def test_sync_readme_check_never_writes(tmp_path, manifest):
    path = _readme(tmp_path)
    before = path.read_text()
    assert docs.sync_readme(path, docs.readme_section(manifest), check=True) is False
    assert path.read_text() == before


def test_sync_readme_without_markers_is_an_error(tmp_path):
    path = tmp_path / "README.md"
    path.write_text(f"{docs.README_END}\n{docs.README_START}\n")
    with pytest.raises(ValueError, match="markers"):
        docs.sync_readme(path, "x")


def test_cli_readme_write_then_check(tmp_path, capsys):
    target, readme = _target(tmp_path), _readme(tmp_path)
    args = ["--target-dir", str(target), "--readme", str(readme)]
    assert docs.main([*args, "--check"]) == 1
    assert docs.main(args) == 0
    assert docs.main([*args, "--check"]) == 0
    assert "up to date" in capsys.readouterr().out
    assert not (tmp_path / "dbt-docs-bundle").exists()


def test_cli_check_needs_readme(tmp_path):
    with pytest.raises(SystemExit) as exc:
        docs.main(["--target-dir", str(_target(tmp_path)), "--check"])
    assert exc.value.code == 2


@needs_dbt
def test_repo_readme_catalog_is_up_to_date(real_manifest):
    """The README block matches the dbt project; `make readme-catalog` fixes it."""
    readme = Path(__file__).resolve().parents[1] / "README.md"
    assert docs.sync_readme(readme, docs.readme_section(real_manifest), check=True), (
        "README data catalog is stale: run `make readme-catalog`"
    )
