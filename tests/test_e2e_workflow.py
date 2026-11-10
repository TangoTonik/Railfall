"""Guards on the e2e workflow: the Terraform steps must stay real and strict."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "e2e.yml"


@pytest.fixture(scope="module")
def steps() -> list[dict]:
    doc = yaml.safe_load(WORKFLOW.read_text())
    return doc["jobs"]["e2e"]["steps"]


def _step(steps: list[dict], uses_prefix: str) -> dict:
    return next(s for s in steps if s.get("uses", "").startswith(uses_prefix))


def _index(steps: list[dict], needle: str) -> int:
    return next(i for i, s in enumerate(steps) if needle in s.get("run", ""))


def test_terraform_is_pinned_and_unwrapped(steps):
    with_ = _step(steps, "hashicorp/setup-terraform@")["with"]
    assert with_["terraform_version"].count(".") == 2, "pin an exact Terraform version"
    # The wrapper swallows exit code 2, which would make the drift check pass silently.
    assert with_["terraform_wrapper"] is False


def test_terraform_is_installed_before_the_stack_boots(steps):
    install = next(i for i, s in enumerate(steps) if "setup-terraform" in s.get("uses", ""))
    assert install < _index(steps, "make up")


def test_drift_is_checked_after_apply_and_after_restart(steps):
    runs = [s.get("run", "") for s in steps]
    checks = [i for i, run in enumerate(runs) if "make infra-check" in run]
    assert len(checks) >= 2
    assert min(checks) > _index(steps, "make up")
    assert max(checks) >= _index(steps, "docker compose down\n")


def test_no_mocks_in_e2e(steps):
    commands = " ".join(s.get("run", "") for s in steps).lower()
    for word in ("moto", "mock", "tests/fixtures"):
        assert word not in commands


def test_privileges_are_audited_after_apply_and_after_restart(steps):
    audits = [i for i, s in enumerate(steps) if "make access" in s.get("run", "")]
    assert audits, "e2e must run the least-privilege audit"
    assert min(audits) > _index(steps, "make up")
    assert max(audits) >= _index(steps, "docker compose down\n")


def test_sncf_ingestion_is_real_verified_and_rerun(steps):
    runs = [s.get("run", "") for s in steps]
    ingests = [i for i, r in enumerate(runs) if "make -s ingest-sncf" in r]
    verifies = [i for i, r in enumerate(runs) if "verify-sncf" in r]
    assert len(ingests) == 2, "ingest twice to prove re-runs do not duplicate bronze"
    assert _index(steps, "make up") < ingests[0] < verifies[0] < ingests[1] <= verifies[-1]
    # Row floors are asserted on the real download, not left to the fixtures.
    assert ".rows >= 5000" in runs[ingests[0]] and ".rows >= 2000" in runs[ingests[0]]
    assert "--min-rows" not in "".join(runs)


def test_meteo_ingestion_is_real_verified_and_rerun(steps):
    runs = [s.get("run", "") for s in steps]
    ingests = [i for i, r in enumerate(runs) if "make -s ingest-meteo" in r]
    verifies = [i for i, r in enumerate(runs) if "verify-meteo" in r]
    assert len(ingests) == 2, "ingest twice to prove re-runs do not duplicate bronze"
    assert _index(steps, "make up") < ingests[0] < verifies[0] < ingests[1] <= verifies[-1]
    # 2-3 real departments, each above the real contract floor.
    first = runs[ingests[0]]
    assert "length == 3" in first and ".rows >= 1000" in first
    assert "--department" not in "".join(runs), "e2e uses METEO_DEPARTMENTS from .env"


def test_bronze_is_verified_after_restart(steps):
    restart = _index(steps, "docker compose down\n")
    for target in ("verify-sncf", "verify-meteo"):
        assert any(target in s.get("run", "") for s in steps[restart:]), target


def test_silver_is_built_on_real_bronze_verified_and_rerun(steps):
    runs = [s.get("run", "") for s in steps]
    builds = [i for i, r in enumerate(runs) if "make --no-print-directory silver " in r]
    checks = [i for i, r in enumerate(runs) if "verify-silver" in r]
    assert len(builds) == 2, "build silver twice to prove re-runs do not duplicate rows"
    last_ingest = max(i for i, r in enumerate(runs) if "ingest-meteo" in r or "ingest-sncf" in r)
    assert last_ingest < builds[0] < checks[0] < builds[1] <= checks[-1]
    assert "duplicate_keys == 0" in "".join(runs)
    assert "--root" not in "".join(runs), "e2e reads the real lake, not a local directory"


def test_e2e_covers_every_silver_table(steps):
    from railfall.spark.write import TABLES

    runs = "".join(s.get("run", "") for s in steps)
    assert f"length == {len(TABLES)} and all(.[]; .ok)" in runs
    for name in TABLES:
        assert f'select(.table == "{name}")' in runs, name


def test_spark_has_a_jvm_and_silver_survives_restart(steps):
    java = _step(steps, "actions/setup-java@")
    assert int(java["with"]["java-version"]) >= 17
    assert any("[spark]" in s.get("run", "") for s in steps)
    restart = _index(steps, "docker compose down\n")
    assert any("verify-silver" in s.get("run", "") for s in steps[restart:])


def test_weather_join_coverage_is_asserted_on_the_real_lake(steps):
    # Equal row counts alone would pass with every weather column null.
    verify = steps[_index(steps, "verify-silver-1.json")]["run"]
    checks = [line for line in verify.splitlines() if line.strip().startswith("jq -e")]
    assert any("route_weather_monthly" in c for c in checks)
    assert ".profile.weather_endpoints" in verify
