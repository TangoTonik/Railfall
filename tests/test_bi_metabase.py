"""The Metabase bootstrap, against a scripted fake of the `/api` endpoints."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from metabase_fake import FakeMetabase

from railfall.bi.metabase import (
    EXPECTED_TABLES,
    MetabaseClient,
    MetabaseError,
    MetabaseSettings,
    main,
)


def _client(fake):
    return MetabaseClient(fake, sleep=lambda _s: None)


SETTINGS = MetabaseSettings(admin_password="pw-1", reader_password="reader-pw")


def test_fresh_instance_creates_admin_and_registers_warehouse():
    fake = FakeMetabase(boot_polls=2)
    report = _client(fake).bootstrap(SETTINGS)

    assert report.ok and report.created_admin and report.database_created
    assert report.tables == sorted(EXPECTED_TABLES)  # staging is filtered out
    db = fake.dbs[-1]
    assert db["engine"] == "postgres"
    assert db["details"]["user"] == "railfall_reader"
    assert db["details"]["password"] == "reader-pw"
    assert db["details"]["host"] == "postgres"
    assert db["details"]["schema-filters-patterns"] == "marts"
    setup = next(c for c in fake.calls if c[1] == "/setup")
    assert setup[2]["prefs"]["allow_tracking"] is False


def test_second_run_logs_in_and_updates_instead_of_duplicating():
    fake = FakeMetabase()
    client = _client(fake)
    first = client.bootstrap(SETTINGS)
    again = _client(fake).bootstrap(SETTINGS)

    assert not again.created_admin and not again.database_created
    assert again.database_id == first.database_id
    assert sum(d["name"] == SETTINGS.warehouse_name for d in fake.dbs) == 1
    assert sum(1 for c in fake.calls if c[0] == "POST" and c[1] == "/setup") == 1


def test_wrong_admin_password_on_existing_instance_fails():
    fake = FakeMetabase()
    _client(fake).bootstrap(SETTINGS)
    with pytest.raises(MetabaseError, match="HTTP 401"):
        _client(fake).bootstrap(MetabaseSettings(admin_password="nope"))


def test_missing_mart_is_reported_not_raised():
    fake = FakeMetabase(tables=EXPECTED_TABLES[:-1])
    clock = iter(range(0, 10_000, 100))
    client = MetabaseClient(fake, sleep=lambda _s: None, clock=lambda: next(clock))
    report = client.bootstrap(SETTINGS, sync_timeout_s=250)
    assert not report.ok
    assert report.missing == ["mart_punctuality_by_region_season"]


def test_health_timeout_raises():
    fake = FakeMetabase(boot_polls=10**6)
    clock = iter(range(0, 10_000, 100))
    client = MetabaseClient(fake, sleep=lambda _s: None, clock=lambda: next(clock))
    with pytest.raises(MetabaseError, match="not healthy"):
        client.bootstrap(SETTINGS, health_timeout_s=300)


def test_settings_from_env():
    s = MetabaseSettings.from_env(
        {
            "METABASE_PORT": "3100",
            "METABASE_ADMIN_EMAIL": "ops@example.org",
            "TF_VAR_warehouse_reader_password": "from-tf",
            "POSTGRES_DB": "dwh",
        }
    )
    assert s.url == "http://localhost:3100"
    assert s.admin_email == "ops@example.org"
    assert s.reader_password == "from-tf"
    assert s.warehouse_db == "dwh"
    assert "from-tf" not in repr(s)


def test_main_writes_report(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("METABASE_ADMIN_PASSWORD", "pw-1")
    out = tmp_path / "metabase.json"
    rc = main(["bootstrap", "--report", str(out)], client=_client(FakeMetabase()))
    assert rc == 0
    payload = json.loads(out.read_text())
    assert payload["ok"] and payload["missing"] == []
    assert "[  ok] Metabase" in capsys.readouterr().out


SPEC_ROOT = Path(__file__).resolve().parents[1] / "bi" / "dashboards"


def _full_manifest(tmp_path, tables):
    nodes = {
        f"model.railfall.{t}": {
            "resource_type": "model",
            "name": t,
            "schema": "marts",
            "config": {"materialized": "table"},
        }
        for t in tables
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": nodes}))
    return path


def test_check_is_offline_and_passes_on_the_shipped_specs(tmp_path, capsys):
    fake = FakeMetabase()
    manifest = _full_manifest(tmp_path, [*EXPECTED_TABLES, "dim_route"])
    rc = main(
        ["check", "--dashboards", str(SPEC_ROOT), "--manifest", str(manifest)],
        client=_client(fake),
    )
    assert rc == 0
    assert fake.calls == [], "check must never call Metabase"
    out = capsys.readouterr().out
    assert "weather_impact" in out and "relations dbt builds in marts" in out


def test_check_without_manifest_only_validates_the_specs(capsys):
    assert main(["check", "--dashboards", str(SPEC_ROOT)]) == 0
    assert "relations dbt builds" not in capsys.readouterr().out


def test_check_fails_when_dbt_no_longer_builds_a_mart(tmp_path, capsys):
    gone = "mart_punctuality_by_region_season"
    manifest = _full_manifest(tmp_path, [t for t in EXPECTED_TABLES if t != gone])
    rc = main(["check", "--dashboards", str(SPEC_ROOT), "--manifest", str(manifest)])
    assert rc == 1
    err = capsys.readouterr().err
    assert f"weather_impact.region_season_gaps reads {gone}" in err
    assert f"bootstrap waits for {gone}" in err


def test_check_fails_on_a_missing_manifest(tmp_path, capsys):
    rc = main(["check", "--dashboards", str(SPEC_ROOT), "--manifest", str(tmp_path / "x.json")])
    assert rc == 1
    assert "make dbt-docs" in capsys.readouterr().err
