"""Dashboard provisioning against the scripted Metabase fake."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from metabase_fake import FakeMetabase

from railfall.bi.dashboards import load_dashboards
from railfall.bi.metabase import EXPECTED_TABLES, MetabaseClient, MetabaseSettings, main
from railfall.bi.provision import DashboardProvisioner, card_body

SPEC_ROOT = Path(__file__).resolve().parents[1] / "bi" / "dashboards"
SPECS = load_dashboards(SPEC_ROOT, known_tables=EXPECTED_TABLES)
SPEC = SPECS[0]
SETTINGS = MetabaseSettings(admin_password="pw-1", reader_password="reader-pw")


def _bootstrap(fake, specs=SPECS):
    client = MetabaseClient(fake, sleep=lambda _s: None)
    return client.bootstrap(SETTINGS, dashboards=specs)


def test_first_run_creates_collection_cards_and_dashboard():
    fake = FakeMetabase()
    report = _bootstrap(fake)

    assert report.ok
    [dash] = report.dashboards
    assert dash["dashboard_created"]
    assert sorted(dash["created"]) == sorted(c.key for c in SPEC.cards)
    assert dash["updated"] == [] and dash["archived"] == []
    assert dash["url"] == f"{SETTINGS.url}/dashboard/{dash['dashboard_id']}"
    assert [c["name"] for c in fake.collections.values()] == ["Railfall"]

    card = fake.cards[dash["cards"]["weather_gap"]]
    assert card["collection_id"] == dash["collection_id"]
    assert card["dataset_query"]["database"] == report.database_id
    assert "marts.mart_punctuality_by_weather" in card["dataset_query"]["native"]["query"]

    dashcards = fake.dashboards[dash["dashboard_id"]]["dashcards"]
    assert {d["card_id"] for d in dashcards} == set(dash["cards"].values())
    gap = next(d for d in dashcards if d["card_id"] == dash["cards"]["weather_gap"])
    lay = SPEC.card("weather_gap").layout
    assert (gap["row"], gap["col"], gap["size_x"], gap["size_y"]) == (
        lay.row,
        lay.col,
        lay.size_x,
        lay.size_y,
    )
    assert dash["row_counts"] == {c.key: 3 for c in SPEC.cards}


def test_second_run_updates_in_place():
    fake = FakeMetabase()
    first = _bootstrap(fake).dashboards[0]
    dashcard_ids = {d["id"] for d in fake.dashboards[first["dashboard_id"]]["dashcards"]}

    again = _bootstrap(fake).dashboards[0]
    assert not again["dashboard_created"]
    assert again["created"] == [] and sorted(again["updated"]) == sorted(first["created"])
    assert again["cards"] == first["cards"]
    assert len(fake.collections) == 1 and len(fake.dashboards) == 1
    assert len(fake.cards) == len(SPEC.cards)
    # Same dashcards, same ids: Metabase sees no delete/re-create churn.
    assert {d["id"] for d in fake.dashboards[first["dashboard_id"]]["dashcards"]} == dashcard_ids


def test_card_removed_from_spec_is_archived_and_leaves_the_dashboard():
    fake = FakeMetabase()
    first = _bootstrap(fake).dashboards[0]
    trimmed = replace(SPEC, cards=tuple(c for c in SPEC.cards if c.key != "monthly_trend"))

    again = _bootstrap(fake, [trimmed]).dashboards[0]
    assert again["archived"] == [SPEC.card("monthly_trend").name]
    old_id = first["cards"]["monthly_trend"]
    assert fake.cards[old_id]["archived"] is True
    dashcards = fake.dashboards[first["dashboard_id"]]["dashcards"]
    assert old_id not in {d["card_id"] for d in dashcards}


def test_failing_and_empty_cards_make_the_run_fail():
    fake = FakeMetabase()
    fake.broken_sql = ("cancellation_rate",)
    fake.empty_tables = ("mart_punctuality_by_region_season",)
    report = _bootstrap(fake)

    assert not report.ok
    dash = report.dashboards[0]
    assert "monthly_trend" in dash["failed"] and "weather_gap" in dash["failed"]
    assert dash["empty"] == ["region_season_gaps"]


def test_dashboards_are_skipped_when_marts_are_missing():
    fake = FakeMetabase(tables=EXPECTED_TABLES[:-1])
    clock = iter(range(0, 10_000, 100))
    client = MetabaseClient(fake, sleep=lambda _s: None, clock=lambda: next(clock))
    report = client.bootstrap(SETTINGS, dashboards=SPECS, sync_timeout_s=250)
    assert not report.ok and report.dashboards == []
    assert fake.cards == {}


def test_existing_collection_is_reused_and_personal_ones_ignored():
    fake = FakeMetabase()
    fake.collections[7] = {"id": 7, "name": "Railfall", "personal_owner_id": 1, "location": "/"}
    fake.collections[8] = {"id": 8, "name": "Railfall", "personal_owner_id": None, "location": "/"}
    client = MetabaseClient(fake, sleep=lambda _s: None)
    client.login(SETTINGS.admin_email, SETTINGS.admin_password)
    assert DashboardProvisioner(client, base_url="http://mb").ensure_collection("Railfall") == 8


def test_card_body_is_a_native_question():
    body = card_body(SPEC.card("overall_punctuality"), database_id=2, collection_id=5)
    assert body["display"] == "scalar"
    assert body["dataset_query"]["type"] == "native"
    assert body["dataset_query"]["native"]["template-tags"] == {}
    assert body["collection_id"] == 5


def test_main_provisions_and_reports(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("METABASE_ADMIN_PASSWORD", "pw-1")
    out = tmp_path / "metabase.json"
    client = MetabaseClient(FakeMetabase(), sleep=lambda _s: None)
    rc = main(["bootstrap", "--dashboards", str(SPEC_ROOT), "--report", str(out)], client=client)
    assert rc == 0
    payload = json.loads(out.read_text())
    assert payload["ok"] and payload["dashboards"][0]["ok"]
    assert "dashboard 'Weather impact on TGV punctuality'" in capsys.readouterr().out


def test_main_skip_dashboards(tmp_path, monkeypatch):
    monkeypatch.setenv("METABASE_ADMIN_PASSWORD", "pw-1")
    fake = FakeMetabase()
    rc = main(["bootstrap", "--skip-dashboards"], client=MetabaseClient(fake, sleep=lambda _s: 0))
    assert rc == 0 and fake.cards == {}


def test_main_rejects_a_broken_spec_before_calling_metabase(tmp_path, capsys):
    (tmp_path / "bad").mkdir()
    (tmp_path / "bad" / "dashboard.json").write_text("{not json")
    fake = FakeMetabase()
    rc = main(["bootstrap", "--dashboards", str(tmp_path)], client=MetabaseClient(fake))
    assert rc == 1 and fake.calls == []
    assert "invalid JSON" in capsys.readouterr().err
