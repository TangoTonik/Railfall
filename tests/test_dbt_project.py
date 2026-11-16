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
from railfall.spark.weather import BOUNDS
from railfall.spark.write import TABLES

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"
SOURCES = DBT_DIR / "models" / "staging" / "_sources.yml"
STG_MODELS = DBT_DIR / "models" / "staging" / "_stg_models.yml"


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


def _model_columns(model: str) -> dict[str, dict]:
    doc = yaml.safe_load(STG_MODELS.read_text(encoding="utf-8"))
    (spec,) = [m for m in doc["models"] if m["name"] == model]
    return {c["name"]: c for c in spec.get("columns", [])}


def _range(column: dict) -> dict | None:
    for test in column.get("data_tests", []):
        if isinstance(test, dict) and "accepted_range" in test:
            return test["accepted_range"]
    return None


@pytest.mark.parametrize("measure", sorted(BOUNDS))
def test_staging_weather_ranges_are_the_silver_bounds(measure):
    spec = _range(_model_columns("stg_meteo__weather_daily").get(measure, {}))
    assert spec is not None, f"{measure} has no accepted_range test"
    args = spec["arguments"]
    assert (args["min_value"], args["max_value"]) == BOUNDS[measure]
    assert args.get("inclusive", True), "silver bounds are inclusive"
    assert spec.get("config", {}).get("severity", "error") == "error"


@pytest.mark.parametrize("rate", ["arrival_punctuality_rate", "cancellation_rate"])
def test_staging_rates_are_shares_between_zero_and_one(rate):
    spec = _range(_model_columns("stg_sncf__punctuality_monthly")[rate])
    assert spec is not None
    assert (spec["arguments"]["min_value"], spec["arguments"]["max_value"]) == (0, 1)


def test_only_aqst_cause_shares_may_warn():
    # e2e accepts `warn` statuses for these tests only; anything else must fail the build.
    for model in ("stg_sncf__punctuality_monthly", "stg_meteo__weather_daily"):
        for name, column in _model_columns(model).items():
            spec = _range(column)
            if spec and spec.get("config", {}).get("severity") == "warn":
                assert name.startswith("pct_cause_"), f"{model}.{name}"


def test_local_generic_tests_cover_what_the_project_uses():
    generic = {p.stem for p in (DBT_DIR / "tests" / "generic").glob("*.sql")}
    assert {"accepted_range", "unique_combination_of_columns"} <= generic


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


def _accepted_values(column: dict) -> list | None:
    for test in column.get("data_tests", []):
        if isinstance(test, dict) and "accepted_values" in test:
            return test["accepted_values"]["arguments"]["values"]
    return None


@pytest.mark.parametrize("side", ["departure_match", "arrival_match"])
def test_route_match_methods_are_the_spark_methods(side):
    from railfall.spark.route_departments import METHODS

    column = _model_columns("stg_sncf__route_departments")[side]
    assert _accepted_values(column) == list(METHODS)


@pytest.mark.parametrize(
    "measure", ["rain_days", "heavy_rain_days", "frost_days", "strong_wind_days"]
)
def test_monthly_day_counts_never_exceed_a_month(measure):
    spec = _range(_model_columns("stg_meteo__weather_monthly")[measure])
    assert spec is not None
    assert (spec["arguments"]["min_value"], spec["arguments"]["max_value"]) == (0, 31)
