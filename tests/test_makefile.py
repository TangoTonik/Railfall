"""Dry-run checks on the Makefile: no Docker daemon needed."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")


def _make(*args: str) -> str:
    result = subprocess.run(
        ["make", "--no-print-directory", "-C", str(ROOT), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def test_help_lists_documented_targets():
    out = _make("help")
    targets = (
        "core",
        "up",
        "urls",
        "infra",
        "infra-check",
        "access",
        "health",
        "smoke",
        "ingest-sncf",
        "verify-sncf",
        "ingest-meteo",
        "verify-meteo",
        "silver",
        "load",
        "airflow",
        "metabase",
        "dashboards",
        "dag-run",
        "dbt-debug",
        "dbt-build",
        "dbt-freshness",
        "verify-staging",
        "verify-marts",
        "weather-report",
        "weather-stats",
        "results",
        "freshness",
        "run-metrics",
        "monitor",
        "down",
    )
    for target in (*targets, "clean"):
        assert f"  {target} " in out, f"`make help` misses {target}"


def test_core_builds_then_waits_then_provisions_with_terraform():
    out = _make("-n", "core")
    order = [
        "docker compose build minio",
        "docker compose up -d --wait postgres minio",
        "terraform -chdir=infra/terraform init -input=false",
        "terraform -chdir=infra/terraform apply -input=false -auto-approve",
    ]
    positions = [out.find(cmd) for cmd in order]
    assert -1 not in positions, positions
    assert positions == sorted(positions)


def test_up_starts_every_service_then_checks_them_all():
    out = _make("-n", "up", "PYTHON=python", "WAIT=7")
    order = [
        "docker compose up -d --wait postgres minio",
        "terraform -chdir=infra/terraform apply",
        "docker compose --profile airflow up -d --wait airflow-webserver airflow-scheduler",
        "docker compose --profile bi up -d --wait metabase",
        "python -m railfall.healthcheck --all --wait 7",
    ]
    positions = [out.find(cmd) for cmd in order]
    assert -1 not in positions, positions
    assert positions == sorted(positions)


def test_urls_follow_the_configured_ports():
    out = _make("urls", "AIRFLOW_WEB_PORT=8181", "METABASE_PORT=3100", "POSTGRES_PORT=5433")
    assert "http://localhost:8181" in out
    assert "http://localhost:3100" in out
    assert "localhost:5433/" in out


def test_infra_check_fails_on_drift():
    """`-detailed-exitcode` makes plan exit 2 when changes are pending."""
    out = _make("-n", "infra-check")
    assert "plan -input=false -detailed-exitcode" in out
    assert "apply" not in out


def test_terraform_targets_the_compose_ports():
    probe = "--eval=probe: ; @echo $$TF_VAR_minio_server $$TF_VAR_postgres_port"
    out = _make(probe, "probe", "MINIO_API_PORT=9100", "POSTGRES_PORT=5433")
    assert out.split() == ["localhost:9100", "5433"]


def test_down_keeps_volumes_but_clean_removes_them():
    assert "-v" not in _make("-n", "down").split()
    assert "-v" in _make("-n", "clean").split()


def test_health_wait_is_overridable():
    out = _make("-n", "health", "WAIT=5")
    assert "-m railfall.healthcheck --wait 5" in out


def test_access_runs_the_privilege_audit():
    assert "-m railfall.access" in _make("-n", "access")


def test_ingest_sncf_runs_the_ingestion_cli_with_extra_args():
    out = _make("-n", "ingest-sncf", "PYTHON=python", "INGEST_ARGS=--json --dataset stations")
    assert out.strip() == "python -m railfall.ingest.run sncf --json --dataset stations"


def test_ingest_meteo_runs_the_ingestion_cli_with_extra_args():
    out = _make("-n", "ingest-meteo", "PYTHON=python", "INGEST_ARGS=--department 33 --json")
    assert out.strip() == "python -m railfall.ingest.run meteo --department 33 --json"


def test_silver_runs_the_spark_job_with_extra_args():
    out = _make("-n", "silver", "PYTHON=python", "SILVER_ARGS=--table stations --json")
    assert "python -m railfall.spark.job --table stations --json" in out


def test_verify_silver_runs_the_contract_check():
    out = _make("-n", "verify-silver", "PYTHON=python", "SILVER_ARGS=--json")
    assert "python -m railfall.spark.verify --json" in out


def test_load_runs_the_warehouse_loader_with_extra_args():
    out = _make("-n", "load", "PYTHON=python", "LOAD_ARGS=--year 2025 --json")
    assert "python -m railfall.load.job --year 2025 --json" in out


def test_verify_load_runs_the_warehouse_check():
    out = _make("-n", "verify-load", "PYTHON=python", "LOAD_ARGS=--report v.json")
    assert "python -m railfall.load.verify --report v.json" in out


def test_dbt_build_runs_the_dbt_wrapper_with_extra_args():
    out = _make("-n", "dbt-build", "PYTHON=python", "DBT_ARGS=--select staging --json")
    assert "python -m railfall.transform.dbt build --select staging --json" in out


def test_dbt_freshness_runs_source_freshness_with_extra_args():
    out = _make("-n", "dbt-freshness", "PYTHON=python", "DBT_ARGS=--json")
    assert "python -m railfall.transform.dbt source freshness --json" in out


def test_dbt_debug_checks_the_connection():
    assert "-m railfall.transform.dbt debug" in _make("-n", "dbt-debug", "PYTHON=python")


def test_verify_staging_runs_the_staging_checks():
    out = _make("-n", "verify-staging", "PYTHON=python", "DBT_VERIFY_ARGS=--json")
    assert "python -m railfall.transform.verify --json" in out


def test_terraform_init_is_retried():
    out = _make("-n", "infra-init")
    assert out.count("terraform -chdir=") == 1
    assert "for i in 1 2 3" in out and "sleep" in out


def test_airflow_builds_then_waits_for_webserver_and_scheduler():
    out = _make("-n", "airflow")
    build = out.find("docker compose --profile airflow build airflow-init")
    up = out.find(
        "docker compose --profile airflow up -d --wait airflow-webserver airflow-scheduler"
    )
    assert -1 < build < up


def test_down_and_clean_include_the_airflow_profile():
    for target in ("down", "clean"):
        assert "--profile airflow" in _make("-n", target), target


def test_dag_run_triggers_the_ingest_dag_by_default():
    out = _make("-n", "dag-run", "PYTHON=python", "DAG_ARGS=--report r.json")
    assert (
        out.strip()
        == "python -m railfall.orchestration.airflow_api railfall_ingest --report r.json"
    )
    assert "airflow_api other_dag" in _make("-n", "dag-run", "PYTHON=python", "DAG=other_dag")


def test_verify_marts_checks_the_marts_layer():
    out = _make("-n", "verify-marts", "PYTHON=python", "DBT_VERIFY_ARGS=--json")
    assert "python -m railfall.transform.verify --layer marts --json" in out


def test_weather_report_reads_the_marts_as_the_reader():
    out = _make("-n", "weather-report", "PYTHON=python", "REPORT_ARGS=--out w.md")
    assert out.strip() == "python -m railfall.transform.report --out w.md"


def test_weather_stats_runs_the_significance_tests():
    out = _make("-n", "weather-stats", "PYTHON=python", "STATS_ARGS=--report s.json")
    assert out.strip() == "python -m railfall.analysis.weather --report s.json"


def test_results_writes_the_page_from_the_results_json():
    out = _make("-n", "results", "PYTHON=python", "RESULTS_ARGS=--check")
    assert out.strip() == (
        "python -m railfall.analysis.writeup --stats weather-stats.json "
        "--report weather-report.json --check"
    )


@pytest.mark.parametrize(
    ("target", "module"),
    [("freshness", "railfall.monitoring.freshness"), ("run-metrics", "railfall.monitoring.runs")],
)
def test_monitoring_targets_run_their_cli_with_extra_args(target, module):
    out = _make("-n", target, "PYTHON=python", "MONITOR_ARGS=--json")
    assert f"python -m {module} --json" in out


def test_monitor_writes_both_reports_then_summarises_them():
    out = _make("-n", "monitor", "PYTHON=python", "MONITOR_DIR=out", "SUMMARY_ARGS=--no-llm")
    lines = [line for line in out.splitlines() if "railfall.monitoring" in line]
    assert "-m railfall.monitoring.freshness --report out/freshness.json" in lines[0]
    assert "-m railfall.monitoring.runs --report out/runs.json" in lines[1]
    assert "-m railfall.monitoring.summary --freshness out/freshness.json" in lines[2]
    assert "--report out/summary.json --no-llm" in out


def test_builds_skip_default_attestations():
    """Rebuilding an unchanged image must keep its digest, or `make up` recreates
    the containers (the e2e idempotency check). Check what a recipe's shell -
    and so `docker compose build` - actually sees."""
    out = _make("--eval", "print-attest: ; @echo $$BUILDX_NO_DEFAULT_ATTESTATIONS", "print-attest")
    assert out.strip() == "1"
