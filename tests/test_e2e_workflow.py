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
