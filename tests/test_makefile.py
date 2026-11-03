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
    for target in ("up", "health", "smoke", "down", "clean"):
        assert f"  {target} " in out, f"`make help` misses {target}"


def test_up_builds_then_waits_then_creates_bucket():
    lines = [line for line in _make("-n", "up").splitlines() if "docker compose" in line]
    assert lines == [
        "docker compose build minio",
        "docker compose up -d --wait postgres minio",
        "docker compose run --rm minio-init",
    ]


def test_down_keeps_volumes_but_clean_removes_them():
    assert "-v" not in _make("-n", "down").split()
    assert "-v" in _make("-n", "clean").split()


def test_health_wait_is_overridable():
    out = _make("-n", "health", "WAIT=5")
    assert "-m railfall.healthcheck --wait 5" in out
