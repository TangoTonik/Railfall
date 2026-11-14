"""The dbt project stays in sync with the warehouse the loader writes.

The YAML checks are pure Python. `dbt parse` compiles every model and test
without a database and runs wherever dbt is installed (the dev extra).
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from railfall.spark.verify import CONTRACTS
from railfall.spark.write import TABLES

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"
SOURCES = DBT_DIR / "models" / "staging" / "_sources.yml"


def _raw_source() -> dict:
    doc = yaml.safe_load(SOURCES.read_text(encoding="utf-8"))
    (raw,) = [s for s in doc["sources"] if s["name"] == "raw"]
    return raw


def _tables() -> dict[str, dict]:
    return {t["name"]: t for t in _raw_source()["tables"]}


def test_raw_source_points_at_the_loader_schema():
    raw = _raw_source()
    assert raw["schema"] == "raw"
    assert raw["loaded_at_field"] == "_updated_at"


def test_every_warehouse_table_is_a_source():
    assert set(_tables()) == set(TABLES)


@pytest.mark.parametrize("name", sorted(TABLES))
def test_source_columns_match_the_silver_contract(name):
    declared = [c["name"] for c in _tables()[name].get("columns", [])]
    assert len(declared) == len(set(declared)), "duplicate column in sources"
    contract = CONTRACTS[name]
    # route_weather_monthly has ~40 measure columns; only its key is declared.
    expected = set(contract.key) if name == "route_weather_monthly" else set(contract.columns)
    assert set(declared) == expected


def _key_tests(table: dict) -> list[tuple[str, ...]]:
    keys = []
    for test in table.get("data_tests", []):
        if isinstance(test, dict) and "unique_combination_of_columns" in test:
            keys.append(tuple(test["unique_combination_of_columns"]["arguments"]["columns"]))
    for column in table.get("columns", []):
        if "unique" in column.get("data_tests", []):
            keys.append((column["name"],))
    return keys


@pytest.mark.parametrize("name", sorted(TABLES))
def test_source_uniqueness_test_is_the_contract_key(name):
    assert _key_tests(_tables()[name]) == [tuple(CONTRACTS[name].key)]


def test_profile_reads_credentials_from_the_environment():
    profile = yaml.safe_load((DBT_DIR / "profiles.yml").read_text(encoding="utf-8"))
    dev = profile["railfall"]["outputs"]["dev"]
    for key in ("host", "port", "user", "password", "dbname"):
        assert "env_var(" in str(dev[key]), key
    assert "railfall_transformer" in dev["user"], "dbt must not run as the admin role"


def test_staging_models_build_into_the_staging_schema():
    project = yaml.safe_load((DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8"))
    staging = project["models"]["railfall"]["staging"]
    assert staging["+schema"] == "staging"
    assert staging["+materialized"] == "view"


@pytest.mark.skipif(shutil.which("dbt") is None, reason="dbt is not installed")
def test_dbt_parse_compiles_the_project(tmp_path):
    env = {**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    result = subprocess.run(
        [
            "dbt",
            "parse",
            "--warn-error",
            "--profiles-dir",
            str(DBT_DIR),
            "--project-dir",
            str(DBT_DIR),
            "--target-path",
            str(tmp_path / "target"),
            "--log-path",
            str(tmp_path / "logs"),
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert (tmp_path / "target" / "manifest.json").exists()
