"""Dashboard specs: the shipped ones are valid, and broken ones fail with a clear message."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from railfall.bi.dashboards import (
    GRID_COLUMNS,
    DashboardSpecError,
    check_manifest,
    check_sql,
    load_dashboard,
    load_dashboards,
    manifest_tables,
)
from railfall.bi.metabase import EXPECTED_TABLES

REPO_ROOT = Path(__file__).resolve().parents[1]
SPEC_ROOT = REPO_ROOT / "bi" / "dashboards"


def test_shipped_dashboards_are_valid():
    specs = load_dashboards(SPEC_ROOT, known_tables=EXPECTED_TABLES)
    assert [s.slug for s in specs] == ["weather_impact"]
    spec = specs[0]
    assert spec.collection == "Railfall"
    assert len(spec.cards) >= 5
    assert spec.card("overall_punctuality").display == "scalar"


def test_every_synced_mart_feeds_a_card():
    used = {t for s in load_dashboards(SPEC_ROOT) for c in s.cards for t in c.tables}
    assert used == set(EXPECTED_TABLES)


def test_shipped_cards_fill_the_grid_width():
    spec = load_dashboards(SPEC_ROOT)[0]
    assert max(c.layout.col + c.layout.size_x for c in spec.cards) == GRID_COLUMNS


def _write(tmp_path: Path, cards: list[dict], sql: dict[str, str], **extra) -> Path:
    d = tmp_path / "demo"
    d.mkdir()
    spec = {"name": "Demo", "collection": "Railfall", "cards": cards, **extra}
    (d / "dashboard.json").write_text(json.dumps(spec))
    for name, body in sql.items():
        (d / name).write_text(body)
    return d


def _card(key="a", **over) -> dict:
    card = {
        "key": key,
        "name": f"Card {key}",
        "display": "table",
        "sql": f"{key}.sql",
        "tables": ["fact_route_month"],
        "layout": {"row": 0, "col": 0, "size_x": 6, "size_y": 4},
    }
    card.update(over)
    return card


GOOD_SQL = "-- comment with marts.ignored\nselect * from marts.fact_route_month"


def test_minimal_spec_loads(tmp_path):
    d = _write(tmp_path, [_card()], {"a.sql": GOOD_SQL})
    spec = load_dashboard(d, known_tables=EXPECTED_TABLES)
    assert spec.slug == "demo"
    assert spec.cards[0].sql.endswith("marts.fact_route_month")
    assert spec.cards[0].visualization_settings == {}


@pytest.mark.parametrize(
    ("cards", "sql", "message"),
    [
        ([_card(), _card()], {"a.sql": GOOD_SQL}, "duplicate key"),
        ([_card(), _card("b", name="Card a")], {"a.sql": GOOD_SQL, "b.sql": GOOD_SQL}, "name"),
        ([_card(display="sankey")], {"a.sql": GOOD_SQL}, "unknown display"),
        ([_card(sql="missing.sql")], {}, "not found"),
        ([_card(tables=["raw_stuff"])], {"a.sql": GOOD_SQL}, "not synced"),
        (
            [_card(layout={"row": 0, "col": 20, "size_x": 6, "size_y": 2})],
            {"a.sql": GOOD_SQL},
            "overflows",
        ),
        ([_card(layout={"row": 0, "col": 0, "size_x": 6})], {"a.sql": GOOD_SQL}, "size_y"),
        (
            [_card(), _card("b", layout={"row": 3, "col": 5, "size_x": 4, "size_y": 4})],
            {"a.sql": GOOD_SQL, "b.sql": GOOD_SQL},
            "overlaps card 'a'",
        ),
    ],
)
def test_broken_specs_are_rejected(tmp_path, cards, sql, message):
    d = _write(tmp_path, cards, sql)
    with pytest.raises(DashboardSpecError, match=message):
        load_dashboard(d, known_tables=EXPECTED_TABLES)


def test_missing_top_level_key(tmp_path):
    d = tmp_path / "demo"
    d.mkdir()
    (d / "dashboard.json").write_text(json.dumps({"name": "x", "cards": []}))
    with pytest.raises(DashboardSpecError, match="collection"):
        load_dashboard(d)


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("", "empty"),
        ("select 1 from marts.fact_route_month; select 2", "more than one"),
        ("delete from marts.fact_route_month", "SELECT or WITH"),
        ("with x as (delete from marts.fact_route_month returning *) select * from x", "DELETE"),
        ("select 1", "reads no marts"),
        ("select * from marts.fact_route_month join marts.dim_route using (route_id)", "declares"),
    ],
)
def test_check_sql_rejects(sql, message):
    with pytest.raises(DashboardSpecError, match=message):
        check_sql(sql, ["fact_route_month"], where="t")


def test_check_sql_allows_trailing_semicolon_and_lookalike_columns():
    check_sql(
        "select _updated_at, created_month from marts.fact_route_month;",
        ["fact_route_month"],
        where="t",
    )


def test_duplicate_dashboard_names(tmp_path):
    for slug in ("one", "two"):
        d = tmp_path / slug
        d.mkdir()
        (d / "a.sql").write_text(GOOD_SQL)
        (d / "dashboard.json").write_text(
            json.dumps({"name": "Same", "collection": "Railfall", "cards": [_card()]})
        )
    with pytest.raises(DashboardSpecError, match="duplicate dashboard names"):
        load_dashboards(tmp_path)


def test_empty_root(tmp_path):
    with pytest.raises(DashboardSpecError, match="no dashboard.json"):
        load_dashboards(tmp_path)


# --- cross-check against the dbt manifest -----------------------------------

FIXTURE_MANIFEST = REPO_ROOT / "tests" / "fixtures" / "dbt" / "manifest.json"


def _manifest(tmp_path: Path, nodes: dict) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": nodes}), encoding="utf-8")
    return path


def _node(name: str, schema: str = "marts", **extra) -> dict:
    return {
        "resource_type": extra.pop("resource_type", "model"),
        "name": name,
        "schema": schema,
        "config": {"materialized": extra.pop("materialized", "table")},
        **extra,
    }


def test_manifest_tables_reads_only_the_marts_relations():
    assert manifest_tables(FIXTURE_MANIFEST) == {"dim_route", "fact_route_month"}
    assert "stg_sncf__punctuality_monthly" in manifest_tables(FIXTURE_MANIFEST, schema="staging")


def test_manifest_tables_uses_aliases_and_skips_ephemeral_models_and_tests(tmp_path):
    path = _manifest(
        tmp_path,
        {
            "model.a": _node("int_x", alias="Mart_X"),
            "model.b": _node("helper", materialized="ephemeral"),
            "test.c": _node("not_null_x", resource_type="test"),
            "seed.d": _node("departments", resource_type="seed"),
        },
    )
    assert manifest_tables(path) == {"mart_x", "departments"}


def test_manifest_tables_missing_or_broken_file(tmp_path):
    with pytest.raises(DashboardSpecError, match="make dbt-docs"):
        manifest_tables(tmp_path / "nope.json")
    (tmp_path / "bad.json").write_text("{", encoding="utf-8")
    with pytest.raises(DashboardSpecError, match="invalid JSON"):
        manifest_tables(tmp_path / "bad.json")


def test_check_manifest_names_the_cards_dbt_cannot_feed():
    specs = load_dashboards(SPEC_ROOT)
    assert check_manifest(specs, EXPECTED_TABLES) == {}
    problems = check_manifest(specs, {"fact_route_month"})
    assert problems, "the weather marts are not in this manifest"
    assert "weather_impact.overall_punctuality" not in problems
    assert problems["weather_impact.punctuality_by_weather"] == ["mart_punctuality_by_weather"]
