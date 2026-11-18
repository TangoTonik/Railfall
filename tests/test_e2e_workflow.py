"""Guards on the e2e workflow: the Terraform steps must stay real and strict."""

from __future__ import annotations

import re
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
    # The spark extra, possibly alongside others (e.g. ".[spark,dbt]").
    assert any(re.search(r"\.\[[^\]]*\bspark\b", s.get("run", "")) for s in steps)
    restart = _index(steps, "docker compose down\n")
    assert any("verify-silver" in s.get("run", "") for s in steps[restart:])


def test_weather_join_coverage_is_asserted_on_the_real_lake(steps):
    # Equal row counts alone would pass with every weather column null.
    verify = steps[_index(steps, "verify-silver-1.json")]["run"]
    checks = [line for line in verify.splitlines() if line.strip().startswith("jq -e")]
    assert any("route_weather_monthly" in c for c in checks)
    assert ".profile.weather_endpoints" in verify


def test_load_runs_on_real_silver_verified_and_rerun(steps):
    runs = [s.get("run", "") for s in steps]
    loads = [i for i, r in enumerate(runs) if "make --no-print-directory load " in r]
    checks = [i for i, r in enumerate(runs) if "verify-load" in r]
    assert len(loads) >= 2, "load twice to prove re-runs do not duplicate warehouse rows"
    last_silver = max(i for i, r in enumerate(runs) if "make --no-print-directory silver " in r)
    assert last_silver < loads[0] < checks[0] < loads[1] <= checks[-1]
    # The second load must be a no-op, not merely "ok".
    rerun = runs[loads[1]]
    assert ".inserted == 0" in rerun and ".updated == 0" in rerun and ".deleted == 0" in rerun
    assert 'LOAD_ARGS="--root' not in "".join(runs), "e2e loads from the real lake"


def test_warehouse_is_verified_after_restart(steps):
    restart = _index(steps, "docker compose down\n")
    assert any("verify-load" in s.get("run", "") for s in steps[restart:])


def test_provider_download_goes_through_the_retrying_init(steps):
    validate = steps[_index(steps, "terraform -chdir=infra/terraform validate")]["run"]
    assert "make infra-init" in validate
    tf = [line for line in validate.splitlines() if line.strip().startswith("terraform ")]
    assert not any(" init" in line for line in tf), "no un-retried terraform init"


def test_incremental_load_repairs_one_year_on_the_real_warehouse(steps):
    i = _index(steps, 'LOAD_ARGS="--year $year')
    run = steps[i]["run"]
    # Drift is created in the real warehouse, then repaired by the scoped load.
    assert run.index("delete from raw.punctuality_monthly") < run.index("--year $year")
    assert '.scope == ("year=" + $y)' in run and '.scope == "full"' in run
    assert "verify-load" in run
    reruns = [j for j, s in enumerate(steps) if "load-2.json" in s.get("run", "")]
    assert reruns and max(reruns) < i, "the full re-run is proven before the incremental one"


def test_ingest_dag_is_run_for_real_after_airflow_boots(steps):
    boot = _index(steps, "make airflow")
    assert boot > _index(steps, "make up"), "Airflow needs the Terraform-provisioned DB"
    dag = _index(steps, "make dag-run DAG=railfall_ingest")
    assert dag > boot
    run = steps[dag]["run"]
    assert '.state == "success"' in run
    assert "--report" in run, "assertions must read the run report, not the log"


def test_dag_run_is_checked_for_duplicates_in_bronze(steps):
    dag = _index(steps, "make dag-run DAG=railfall_ingest")
    after = " ".join(s.get("run", "") for s in steps[dag + 1 :])
    assert "verify-sncf INGEST_ARGS=--list" in after
    assert "verify-meteo INGEST_ARGS=--list" in after


def test_airflow_is_cleaned_up_without_touching_the_core_stack(steps):
    rm = next(s["run"] for s in steps if "rm --stop" in s.get("run", ""))
    # A bare `compose rm` would remove Postgres and MinIO (and their data) too.
    assert rm.split("--force", 1)[1].split() == [
        "airflow-webserver",
        "airflow-scheduler",
        "airflow-init",
    ]
    teardown = steps[-1]["run"]
    assert "--profile airflow" in teardown and "down -v" in teardown


def test_full_pipeline_dag_is_run_for_real_after_the_cli_stages(steps):
    dag = _index(steps, "make dag-run DAG=railfall_pipeline")
    # Airflow is booted again after the CLI Spark/load steps freed the runner.
    boots = [i for i, s in enumerate(steps) if "make airflow" in s.get("run", "")]
    assert len(boots) == 2 and boots[1] < dag
    assert _index(steps, "load-3.json") < boots[1], "the CLI load chain runs first"
    run = steps[dag]["run"]
    assert '.state == "success"' in run and "--report dag-pipeline.json" in run
    assert "--xcom" in run, "assertions must read what the Spark and load tasks returned"
    for task in ("build_silver", "verify_silver", "load_warehouse", "verify_warehouse"):
        assert f'"{task}"' in run, task
    # Converges on the CLI's state: same silver, nothing merged.
    assert "--slurpfile silver silver-2.json" in run and ".inserted == 0" in run


def test_e2e_never_runs_timetable_runs_beside_the_triggered_ones(steps):
    boots = [s["run"] for s in steps if "make airflow" in s.get("run", "")]
    assert boots and all("AIRFLOW_USE_JOB_SCHEDULE=false" in b for b in boots)


def test_airflow_is_removed_before_the_restart_and_the_warehouse_reread(steps):
    dag = _index(steps, "make dag-run DAG=railfall_pipeline")
    rms = [i for i, s in enumerate(steps) if "rm --stop" in s.get("run", "")]
    restart = _index(steps, "docker compose down\n")
    assert any(dag < i < restart for i in rms)
    reread = _index(steps, "verify-load-dag.json")
    assert dag < reread < restart


def test_dbt_builds_staging_on_the_final_warehouse_and_is_rerun(steps):
    runs = [s.get("run", "") for s in steps]
    install = next(r for r in runs if "pip install -e" in r)
    assert "dbt" in install, "e2e must install the dbt extra"
    builds = [
        i
        for i, r in enumerate(runs)
        if "make --no-print-directory dbt-build" in r and "--select staging" in r
    ]
    assert len(builds) == 2, "build twice to prove dbt re-runs are idempotent"
    # dbt reads the warehouse the full DAG left behind, before the restart.
    assert _index(steps, "verify-load-dag.json") < _index(steps, "dbt-debug") < builds[0]
    assert builds[1] < _index(steps, "docker compose down\n")
    first = runs[builds[0]]
    assert "--select staging" in first
    # Only the AQST cause-share range tests may come back as `warn`.
    assert 'all(.status == "pass" or .status == "warn")' in first
    assert "pct_cause_" in first
    assert "--slurpfile one dbt-build-1.json" in runs[builds[1]]


def test_star_schema_is_built_after_staging_with_its_seed(steps):
    runs = [s.get("run", "") for s in steps]
    marts = [i for i, r in enumerate(runs) if "dbt-build" in r and "marts" in r]
    assert len(marts) == 2, "build the star schema twice to prove it is idempotent"
    staging = _index(steps, "--select staging")
    assert staging < marts[0] < marts[1] < _index(steps, "docker compose down\n")
    run = runs[marts[0]]
    assert "--select departments marts" in run, "dims read the departments seed"
    # Every model under dbt/models/marts is built and counted, so a new mart
    # cannot be added without the e2e assertion being updated with it.
    marts_dir = Path(__file__).resolve().parents[1] / "dbt" / "models" / "marts"
    n_models = len(list(marts_dir.rglob("*.sql")))
    assert f'.resource_type == "model")] | length == {n_models}' in run
    assert 'all(.status == "pass")' in run, "no warn-severity tests on the marts"
    assert "--slurpfile one dbt-build-marts-1.json" in runs[marts[1]]


def test_star_schema_is_verified_outside_dbt_and_after_restart(steps):
    runs = [s.get("run", "") for s in steps]
    marts = [i for i, r in enumerate(runs) if "dbt-build" in r and "marts" in r]
    checks = [i for i, r in enumerate(runs) if "verify-marts" in r]
    assert marts[0] < checks[0] < marts[1], "checked before the rebuild"
    assert ".rows == .source_rows" in runs[checks[0]], "facts keep every staging row"
    assert ".trains_mismatches == 0" in runs[checks[0]], "weather marts reconcile trains"
    assert ".weather_factors == 3" in runs[checks[0]], "rain, wind and frost all present"
    assert "--slurpfile one verify-marts-1.json" in runs[marts[1]]
    assert max(checks) == _index(steps, "docker compose down\n"), "tables survive a restart"


def test_raw_sources_are_fresh_before_dbt_builds(steps):
    runs = [s.get("run", "") for s in steps]
    fresh = _index(steps, "dbt-freshness")
    builds = [i for i, r in enumerate(runs) if "make --no-print-directory dbt-build" in r]
    # Freshness is judged on what the DAG just loaded, before staging is built.
    assert _index(steps, "dbt-debug") < fresh < builds[0]
    assert '.status == "pass"' in runs[fresh], "a just-loaded source must not merely warn"
    assert '.resource_type == "source"' in runs[fresh]


def test_staging_is_verified_against_raw_and_after_restart(steps):
    runs = [s.get("run", "") for s in steps]
    checks = [i for i, r in enumerate(runs) if "verify-staging" in r]
    assert "--slurpfile raw verify-load-dag.json" in runs[checks[0]]
    assert max(checks) == _index(steps, "docker compose down\n"), "views survive a restart"


def test_weather_report_reads_the_rebuilt_marts_as_the_reader(steps):
    runs = [s.get("run", "") for s in steps]
    report = _index(steps, "make --no-print-directory weather-report")
    rebuild = max(i for i, r in enumerate(runs) if "dbt-build" in r and "marts" in r)
    assert rebuild < report < _index(steps, "docker compose down\n")
    assert ".headlines | length == 3" in runs[report], "a headline per factor"
    assert "select(.is_baseline)" in runs[report], "every factor has its baseline band"
    assert "GITHUB_STEP_SUMMARY" in runs[report]


def test_quality_results_reach_the_summary_and_are_kept_as_an_artifact(steps):
    runs = [s.get("run", "") for s in steps]
    gate = _index(steps, "--markdown quality-1.md")
    assert "GITHUB_STEP_SUMMARY" in runs[gate]
    upload = next(
        s
        for s in steps
        if s.get("uses", "").startswith("actions/upload-artifact@")
        and s["with"]["name"] == "data-quality-reports"
    )
    assert upload.get("if") == "always()", "reports are kept even when the gate fails"
    assert "canary.json" in upload["with"]["path"]
    assert steps.index(upload) > _index(steps, "make --no-print-directory quality-canary")
