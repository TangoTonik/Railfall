"""railfall.transform.verify: staging views checked against raw with a fake connection."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from railfall.transform import verify as tv


class FakeCursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class FakeConn:
    """Answers the three queries verify issues from in-memory relation state.

    `relations` maps "schema.name" -> {"kind": relkind, "rows": n, "dupes": d, "nulls": k}.
    """

    def __init__(self, relations: dict[str, dict]):
        self.relations = relations
        self.queries: list[str] = []

    def execute(self, query, params=None):
        text = query if isinstance(query, str) else query.as_string(None)
        self.queries.append(text)
        if "pg_class" in text:
            rel = self.relations.get(".".join(params))
            return FakeCursor(None if rel is None else (rel["kind"],))
        name = ".".join(re.search(r'"(\w+)"\."(\w+)"', text).groups())
        rel = self.relations[name]
        if "having count(*) > 1" in text:
            return FakeCursor((rel.get("dupes", 0), rel.get("nulls", 0)))
        return FakeCursor((rel["rows"],))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def healthy(rows: int = 120) -> dict[str, dict]:
    rels = {}
    for m in tv.STAGING_MODELS.values():
        rels[f"staging.{m.name}"] = {"kind": "v", "rows": rows}
        rels[f"raw.{m.source}"] = {"kind": "r", "rows": rows}
    return rels


def test_models_match_the_dbt_project():
    # Every staging model dbt builds is verified, and nothing else.
    models = Path(__file__).resolve().parents[1] / "dbt" / "models" / "staging"
    assert {p.stem for p in models.glob("stg_*.sql")} == set(tv.STAGING_MODELS)


def test_healthy_staging_passes():
    reports = tv.verify(FakeConn(healthy()))
    assert [r.model for r in reports] == list(tv.STAGING_MODELS)
    assert all(r.ok and r.kind == "view" and r.rows == r.source_rows == 120 for r in reports)


def test_missing_view_fails_without_querying_it():
    rels = healthy()
    del rels["staging.stg_sncf__stations"]
    conn = FakeConn(rels)
    (report,) = tv.verify(conn, ["stg_sncf__stations"])
    assert not report.ok and "does not exist" in report.errors[0]
    assert len(conn.queries) == 1


def test_row_count_drift_and_table_materialisation_fail():
    rels = healthy()
    rels["staging.stg_meteo__weather_daily"] = {"kind": "r", "rows": 119}
    (report,) = tv.verify(FakeConn(rels), ["stg_meteo__weather_daily"])
    assert not report.ok
    assert any("expected a view" in e for e in report.errors)
    assert any("119 rows, raw.weather_daily has 120" in e for e in report.errors)


def test_duplicate_and_null_keys_fail():
    rels = healthy()
    rels["staging.stg_sncf__punctuality_monthly"].update(dupes=2, nulls=1)
    (report,) = tv.verify(FakeConn(rels), ["stg_sncf__punctuality_monthly"])
    assert (report.duplicate_keys, report.null_keys) == (2, 1)
    assert not report.ok


def test_empty_staging_fails_even_if_raw_is_empty():
    (report,) = tv.verify(FakeConn(healthy(rows=0)), ["stg_sncf__stations"])
    assert not report.ok and any("empty" in e for e in report.errors)


def test_composite_key_sql_checks_every_column():
    text = tv.key_check_sql("staging", "stg_meteo__weather_daily", ["station_id", "day"])
    text = text.as_string(None)
    assert 'group by "station_id", "day"' in text
    assert '"station_id" is null or "day" is null' in text


def test_unknown_model_is_rejected():
    with pytest.raises(ValueError, match="stg_nope"):
        tv.select_models(["stg_nope"])


def test_transformer_dsn_follows_profiles_yml():
    assert tv.transformer_dsn({}) == (
        "postgresql://railfall_transformer:transformer-local@localhost:5432/warehouse"
    )
    env = {
        "POSTGRES_PORT": "5433",
        "POSTGRES_DB": "wh",
        "TF_VAR_warehouse_transformer_password": "p@ss/word",
    }
    assert tv.transformer_dsn(env) == (
        "postgresql://railfall_transformer:p%40ss%2Fword@localhost:5433/wh"
    )
    # Explicit DBT_* wins over the compose fallbacks.
    env |= {"DBT_PORT": "6543", "DBT_DBNAME": "other", "DBT_HOST": "postgres"}
    assert tv.transformer_dsn(env).endswith("@postgres:6543/other")


def test_cli_writes_report_and_exit_code(monkeypatch, tmp_path, capsys):
    rels = healthy()
    monkeypatch.setattr(tv, "_connect", lambda dsn: FakeConn(rels))
    out = tmp_path / "staging.json"
    assert tv.main(["--json", "--report", str(out)]) == 0
    assert json.loads(out.read_text()) == json.loads(capsys.readouterr().out)

    rels["staging.stg_sncf__stations"]["rows"] = 1
    assert tv.main([]) == 1
    assert "FAIL" in capsys.readouterr().out


def healthy_marts(rows: int = 120) -> dict[str, dict]:
    rels = healthy(rows)
    for m in tv.MART_MODELS.values():
        rels[f"marts.{m.name}"] = {"kind": "r", "rows": rows}
    return rels


def test_mart_models_match_the_dbt_project():
    models = Path(__file__).resolve().parents[1] / "dbt" / "models" / "marts" / "core"
    assert {p.stem for p in models.glob("*.sql")} == set(tv.MART_MODELS)
    # Facts are pinned to the staging model that drives them.
    assert all(m.rows_like in tv.STAGING_MODELS for m in tv.MART_MODELS.values() if m.rows_like)


def test_healthy_marts_pass():
    reports = tv.verify(FakeConn(healthy_marts()), layer="marts")
    assert [r.model for r in reports] == list(tv.MART_MODELS)
    assert all(r.ok and r.kind == "table" for r in reports)


def test_fact_that_drops_rows_fails():
    rels = healthy_marts()
    rels["marts.fact_route_month"]["rows"] = 119
    [report] = tv.verify(FakeConn(rels), ["fact_route_month"], layer="marts")
    assert not report.ok
    assert report.source_rows == 120
    assert "stg_sncf__punctuality_monthly has 120" in report.errors[0]


def test_dimension_has_no_row_target_but_must_be_a_keyed_table():
    rels = healthy_marts()
    rels["marts.dim_route"] |= {"kind": "v", "dupes": 2}
    [report] = tv.verify(FakeConn(rels), ["dim_route"], layer="marts")
    assert report.source_rows == 0
    assert report.errors == [
        "marts.dim_route is a view, expected a table",
        "2 repeated route_id values",
    ]


def test_missing_mart_and_wrong_layer_are_reported():
    rels = healthy_marts()
    del rels["marts.dim_month"]
    [report] = tv.verify(FakeConn(rels), ["dim_month"], layer="marts")
    assert report.errors == ["marts.dim_month does not exist"]
    with pytest.raises(ValueError, match="unknown staging model"):
        tv.select_models(["dim_month"])
    with pytest.raises(ValueError, match="unknown layer"):
        tv.select_models([], layer="gold")


def test_all_layer_checks_staging_then_marts(monkeypatch, capsys):
    monkeypatch.setattr(tv, "_connect", lambda dsn: FakeConn(healthy_marts()))
    assert tv.main(["--layer", "all", "--json"]) == 0
    models = [r["model"] for r in json.loads(capsys.readouterr().out)]
    assert models == [*tv.STAGING_MODELS, *tv.MART_MODELS]
