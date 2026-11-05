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
        "up",
        "infra",
        "infra-check",
        "access",
        "health",
        "smoke",
        "ingest-sncf",
        "verify-sncf",
        "down",
    )
    for target in (*targets, "clean"):
        assert f"  {target} " in out, f"`make help` misses {target}"


def test_up_builds_then_waits_then_provisions_with_terraform():
    lines = [
        line
        for line in _make("-n", "up").splitlines()
        if line.startswith(("docker compose", "terraform"))
    ]
    assert lines == [
        "docker compose build minio",
        "docker compose up -d --wait postgres minio",
        "terraform -chdir=infra/terraform init -input=false",
        "terraform -chdir=infra/terraform apply -input=false -auto-approve",
    ]


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
