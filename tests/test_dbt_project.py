"""The dbt project stays in sync with the warehouse the loader writes.

The YAML checks are pure Python. `dbt parse` compiles every model and test
without a database and runs wherever dbt is installed (the dev extra).
"""

from __future__ import annotations

import os
import re
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
    assert {
        "accepted_range",
        "unique_combination_of_columns",
        "expression_is_true",
        "row_count_in_range",
    } <= generic


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


def _departments_seed() -> list[dict[str, str]]:
    import csv

    with (DBT_DIR / "seeds" / "departments.csv").open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def test_departments_seed_covers_every_insee_department_once():
    rows = _departments_seed()
    codes = [r["department"] for r in rows]
    assert len(codes) == len(set(codes)) == 101  # 96 metropolitan + 5 overseas
    assert {"01", "2A", "2B", "75", "974"} <= set(codes)
    assert "20" not in codes, "Corsica is split into 2A / 2B since 1976"
    regions = {(r["region_code"], r["region_name"]) for r in rows}
    assert len(regions) == 18
    assert len({code for code, _ in regions}) == 18, "one name per region code"


def test_route_aliases_point_at_known_departments():
    from railfall.spark.route_departments import ALIASES

    assert set(ALIASES.values()) <= {r["department"] for r in _departments_seed()}


def test_seed_codes_stay_text_in_the_warehouse():
    project = yaml.safe_load((DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8"))
    types = project["seeds"]["railfall"]["departments"]["+column_types"]
    assert types["department"] == "text"
    assert project["seeds"]["railfall"]["+schema"] in {"staging", "marts"}


def test_marts_build_as_tables_into_the_marts_schema():
    project = yaml.safe_load((DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8"))
    marts = project["models"]["railfall"]["marts"]
    assert (marts["+schema"], marts["+materialized"]) == ("marts", "table")


CORE_MODELS = DBT_DIR / "models" / "marts" / "core" / "_core_models.yml"


def _core_model(model: str) -> dict:
    doc = yaml.safe_load(CORE_MODELS.read_text(encoding="utf-8"))
    (spec,) = [m for m in doc["models"] if m["name"] == model]
    return spec


def _relationship(column: dict) -> tuple[str, str] | None:
    for test in column.get("data_tests", []):
        if isinstance(test, dict) and "relationships" in test:
            args = test["relationships"]["arguments"]
            return args["to"], args["field"]
    return None


def test_fact_route_month_grain_is_the_staging_grain():
    spec = _core_model("fact_route_month")
    keys = [
        tuple(t["unique_combination_of_columns"]["arguments"]["columns"])
        for t in spec.get("data_tests", [])
        if isinstance(t, dict) and "unique_combination_of_columns" in t
    ]
    assert keys == [("month", "service", "route_id")]


@pytest.mark.parametrize(
    ("column", "dimension"),
    [
        ("route_id", ("ref('dim_route')", "route_id")),
        ("month", ("ref('dim_month')", "month")),
        ("departure_department", ("ref('dim_department')", "department")),
        ("arrival_department", ("ref('dim_department')", "department")),
    ],
)
def test_fact_route_month_foreign_keys_are_tested(column, dimension):
    columns = {c["name"]: c for c in _core_model("fact_route_month")["columns"]}
    assert _relationship(columns[column]) == dimension


def test_dim_route_match_methods_are_the_spark_methods():
    from railfall.spark.route_departments import METHODS

    columns = {c["name"]: c for c in _core_model("dim_route")["columns"]}
    for side in ("departure_match", "arrival_match"):
        assert _accepted_values(columns[side]) == list(METHODS)
    assert "unmatched" in METHODS, "dim_route falls back to 'unmatched'"


def test_fact_does_not_drop_rows_singular_test_exists():
    assert (
        DBT_DIR / "tests" / "marts" / "assert_fact_route_month_keeps_every_staging_row.sql"
    ).exists()


def test_fact_weather_grain_is_department_month():
    spec = _core_model("fact_weather_department_month")
    keys = [
        tuple(t["unique_combination_of_columns"]["arguments"]["columns"])
        for t in spec.get("data_tests", [])
        if isinstance(t, dict) and "unique_combination_of_columns" in t
    ]
    assert keys == [("department", "month")]


@pytest.mark.parametrize(
    ("column", "dimension"),
    [
        ("department", ("ref('dim_department')", "department")),
        ("month", ("ref('dim_month')", "month")),
    ],
)
def test_fact_weather_foreign_keys_are_tested(column, dimension):
    columns = {c["name"]: c for c in _core_model("fact_weather_department_month")["columns"]}
    assert _relationship(columns[column]) == dimension


def test_fact_weather_documents_every_selected_flag():
    sql = (DBT_DIR / "models" / "marts" / "core" / "fact_weather_department_month.sql").read_text(
        encoding="utf-8"
    )
    columns = {c["name"] for c in _core_model("fact_weather_department_month")["columns"]}
    for flag in ("is_wet_month", "is_frosty_month", "is_windy_month"):
        assert f"as {flag}" in sql
        assert flag in columns


def test_fact_weather_does_not_drop_rows_singular_test_exists():
    assert (
        DBT_DIR / "tests" / "marts" / "assert_fact_weather_keeps_every_staging_row.sql"
    ).exists()


WEATHER_IMPACT_DIR = DBT_DIR / "models" / "marts" / "weather_impact"


def _weather_impact_model(model: str) -> dict:
    doc = yaml.safe_load(
        (WEATHER_IMPACT_DIR / "_weather_impact_models.yml").read_text(encoding="utf-8")
    )
    (spec,) = [m for m in doc["models"] if m["name"] == model]
    return spec


def test_route_month_weather_keeps_the_fact_grain():
    columns = {c["name"]: c for c in _weather_impact_model("int_route_month_weather")["columns"]}
    assert {"not_null", "unique"} <= set(columns["route_month_id"]["data_tests"])
    assert (
        DBT_DIR / "tests" / "marts" / "assert_route_month_weather_keeps_every_fact_row.sql"
    ).exists()


def test_route_month_weather_conditions_match_the_sql():
    sql = (WEATHER_IMPACT_DIR / "int_route_month_weather.sql").read_text(encoding="utf-8")
    columns = {c["name"]: c for c in _weather_impact_model("int_route_month_weather")["columns"]}
    for value in _accepted_values(columns["weather_condition"]):
        assert f"then '{value}'" in sql or f"else '{value}'" in sql, value
    for value in _accepted_values(columns["weather_coverage"]):
        assert f"then '{value}'" in sql or f"else '{value}'" in sql, value


def test_route_month_weather_left_joins_both_endpoints():
    # An inner join would silently drop foreign / unplaced routes from the marts.
    sql = (WEATHER_IMPACT_DIR / "int_route_month_weather.sql").read_text(encoding="utf-8")
    assert sql.count("left join weather as") == 2
    assert "inner join" not in sql


def test_weather_mart_grain_is_factor_by_exposure():
    spec = _weather_impact_model("mart_punctuality_by_weather")
    keys = [
        tuple(t["unique_combination_of_columns"]["arguments"]["columns"])
        for t in spec.get("data_tests", [])
        if isinstance(t, dict) and "unique_combination_of_columns" in t
    ]
    assert keys == [("weather_factor", "exposure")]
    columns = {c["name"]: c for c in spec["columns"]}
    assert _accepted_values(columns["weather_factor"]) == ["rain", "wind", "frost"]
    assert _accepted_values(columns["exposure"]) == ["adverse", "normal"]


def test_weather_mart_rebuilds_rates_from_counts():
    sql = (WEATHER_IMPACT_DIR / "mart_punctuality_by_weather.sql").read_text(encoding="utf-8")
    assert "avg(" not in sql, "rates must be recomputed from summed counts, not averaged"
    assert "sum(trains_run)" in sql


def test_region_season_mart_grain_extends_the_national_one():
    spec = _weather_impact_model("mart_punctuality_by_region_season")
    keys = [
        tuple(t["unique_combination_of_columns"]["arguments"]["columns"])
        for t in spec.get("data_tests", [])
        if isinstance(t, dict) and "unique_combination_of_columns" in t
    ]
    assert keys == [("region_code", "season", "weather_factor", "exposure")]
    columns = {c["name"]: c for c in spec["columns"]}
    national = {
        c["name"]: c for c in _weather_impact_model("mart_punctuality_by_weather")["columns"]
    }
    for shared in ("weather_factor", "exposure"):
        assert _accepted_values(columns[shared]) == _accepted_values(national[shared])


def test_region_season_mart_seasons_match_dim_month():
    dim_month = (DBT_DIR / "models" / "marts" / "core" / "dim_month.sql").read_text(
        encoding="utf-8"
    )
    columns = {
        c["name"]: c for c in _weather_impact_model("mart_punctuality_by_region_season")["columns"]
    }
    for season in _accepted_values(columns["season"]):
        assert f"'{season}'" in dim_month, season


def test_region_season_mart_rebuilds_rates_and_reconciles():
    sql = (WEATHER_IMPACT_DIR / "mart_punctuality_by_region_season.sql").read_text(encoding="utf-8")
    assert "avg(" not in sql, "rates must be recomputed from summed counts, not averaged"
    # Region from the departure end, the arrival end as a fallback, never a fan-out.
    assert "coalesce(dep.region_code, arr.region_code)" in sql
    assert "partition by region_code, season, weather_factor" in sql
    assert (
        DBT_DIR / "tests" / "marts" / "assert_region_season_mart_reconciles_with_national.sql"
    ).exists()


@pytest.mark.parametrize(
    "model", ["mart_punctuality_by_weather", "mart_punctuality_by_region_season"]
)
def test_weather_marts_share_one_rate_definition(model):
    macro = (DBT_DIR / "macros" / "punctuality_rates.sql").read_text(encoding="utf-8")
    assert "avg(" not in macro, "rates must be recomputed from summed counts, not averaged"
    sql = (WEATHER_IMPACT_DIR / f"{model}.sql").read_text(encoding="utf-8")
    assert "{{ punctuality_rates() }}" in sql
    assert "nullif(trains_run, 0)" not in sql, "rate formula duplicated outside the macro"


INTENSITY_SQL = WEATHER_IMPACT_DIR / "mart_punctuality_by_weather_intensity.sql"
_BAND_ROW = re.compile(
    r"\('(?P<factor>\w+)',\s*'(?P<band>\w+)',\s*(?P<order>\d+),\s*(?P<lower>[\w.:]+),"
    r"\s*(?P<upper>[\w.:]+),\s*(?P<baseline>true|false)\)"
)


def _bound(text: str) -> float | None:
    value = text.split("::")[0]
    return None if value == "null" else float(value)


def _intensity_bands() -> dict[str, list[dict]]:
    sql = INTENSITY_SQL.read_text(encoding="utf-8")
    bands: dict[str, list[dict]] = {}
    for m in _BAND_ROW.finditer(sql):
        bands.setdefault(m["factor"], []).append(
            {
                "band": m["band"],
                "order": int(m["order"]),
                "lower": _bound(m["lower"]),
                "upper": _bound(m["upper"]),
                "baseline": m["baseline"] == "true",
            }
        )
    return bands


def test_intensity_bands_tile_each_factor_without_gaps():
    bands = _intensity_bands()
    assert set(bands) == {"rain", "wind", "frost"}
    for factor, rows in bands.items():
        rows = sorted(rows, key=lambda r: r["order"])
        assert [r["order"] for r in rows] == list(range(1, len(rows) + 1)), factor
        assert rows[0]["lower"] is None and rows[-1]["upper"] is None, factor
        for below, above in zip(rows, rows[1:], strict=False):
            assert below["upper"] == above["lower"], (factor, below["band"], above["band"])
        assert sum(r["baseline"] for r in rows) == 1, f"{factor} needs one baseline band"


def test_intensity_band_edges_include_the_flag_thresholds():
    # The binary mart's adverse split must fall on a band edge, so grading only
    # refines it. Thresholds as written in fact_weather_department_month.
    fact = (DBT_DIR / "models" / "marts" / "core" / "fact_weather_department_month.sql").read_text(
        encoding="utf-8"
    )
    assert "1.5 * normal_precipitation_mm" in fact
    assert "frost_days >= 10" in fact and "strong_wind_days >= 5" in fact
    edges = {f: {r["lower"] for r in rows} for f, rows in _intensity_bands().items()}
    assert 1.5 in edges["rain"] and 10 in edges["frost"] and 5 in edges["wind"]


def test_intensity_mart_documents_every_band_and_its_grain():
    spec = _weather_impact_model("mart_punctuality_by_weather_intensity")
    columns = {c["name"]: c for c in spec["columns"]}
    declared = set(_accepted_values(columns["intensity_band"]))
    assert declared == {r["band"] for rows in _intensity_bands().values() for r in rows}
    keys = [
        tuple(t["unique_combination_of_columns"]["arguments"]["columns"])
        for t in spec.get("data_tests", [])
        if isinstance(t, dict) and "unique_combination_of_columns" in t
    ]
    assert ("weather_factor", "intensity_band") in keys


def test_intensity_mart_shares_rates_and_reconciles():
    sql = INTENSITY_SQL.read_text(encoding="utf-8")
    assert "{{ punctuality_rates() }}" in sql and "avg(" not in sql
    assert (
        DBT_DIR / "tests" / "marts" / "assert_intensity_bands_cover_every_route_month.sql"
    ).exists()


def _all_models() -> dict[str, dict]:
    models: dict[str, dict] = {}
    for path in (DBT_DIR / "models").rglob("*.yml"):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for model in doc.get("models", []):
            models[model["name"]] = model
    return models


def _model_test_names(model: dict) -> list[str]:
    names = []
    for test in model.get("data_tests", []):
        names.append(test if isinstance(test, str) else next(iter(test)))
    return names


def test_every_mart_is_checked_for_emptiness():
    # A model that builds empty passes every column test vacuously; each mart and
    # the staging tables they rest on must fail the build instead.
    sql_models = {p.stem for p in (DBT_DIR / "models" / "marts").rglob("*.sql")}
    models = _all_models()
    for name in sorted(sql_models) + [
        "stg_sncf__punctuality_monthly",
        "stg_meteo__weather_monthly",
    ]:
        assert "row_count_in_range" in _model_test_names(models[name]), name


def _expressions(model: str) -> list[str]:
    return [
        test["expression_is_true"]["arguments"]["expression"]
        for test in _all_models()[model].get("data_tests", [])
        if isinstance(test, dict) and "expression_is_true" in test
    ]


def test_staging_punctuality_rechecks_the_silver_derivations():
    expressions = " ".join(_expressions("stg_sncf__punctuality_monthly"))
    assert "trains_run = trains_planned - trains_cancelled" in expressions
    assert "arrival_punctuality_rate" in expressions


def test_staging_weather_monthly_checks_day_and_station_counts():
    expressions = " ".join(_expressions("stg_meteo__weather_monthly"))
    assert "heavy_rain_days <= rain_days" in expressions
    for family in ("rain", "temp", "wind"):
        assert f"stations_{family} <= stations" in expressions


def test_dim_month_gap_free_singular_test_exists():
    sql = (DBT_DIR / "tests" / "marts" / "assert_dim_month_is_gap_free.sql").read_text(
        encoding="utf-8"
    )
    assert "ref('dim_month')" in sql
    assert "interval '1 month'" in sql


def _documented_relations() -> list[tuple[str, dict]]:
    """Every source table, model and seed declared in a schema file."""
    found: list[tuple[str, dict]] = []
    for path in sorted([*(DBT_DIR / "models").rglob("*.yml"), *(DBT_DIR / "seeds").glob("*.yml")]):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for source in doc.get("sources", []):
            found += [(f"{source['name']}.{t['name']}", t) for t in source.get("tables", [])]
        for kind in ("models", "seeds"):
            found += [(r["name"], r) for r in doc.get(kind, [])]
    return found


def test_every_declared_relation_and_column_is_described():
    # The docs bundle runs with --strict in e2e; catch a bare column here first.
    relations = _documented_relations()
    assert len(relations) == 21
    missing = [
        f"{name}.{column['name']}" if column else name
        for name, relation in relations
        for column in [None, *relation.get("columns", [])]
        if not ((column or relation).get("description") or "").strip()
    ]
    assert missing == []
