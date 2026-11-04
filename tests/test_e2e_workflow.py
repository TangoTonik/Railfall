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
