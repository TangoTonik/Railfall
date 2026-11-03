"""Static checks on docker-compose.yml: they run without a Docker daemon."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.example"


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text())


def _env_example_keys() -> set[str]:
    keys = set()
    for line in ENV_EXAMPLE.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            keys.add(line.split("=", 1)[0])
    return keys


def test_core_services_present(compose):
    assert {"postgres", "minio", "minio-init"} <= set(compose["services"])


@pytest.mark.parametrize("service", ["postgres", "minio"])
def test_long_running_services_have_healthchecks(compose, service):
    hc = compose["services"][service]["healthcheck"]
    assert hc["test"], f"{service} healthcheck has no test command"
    assert int(hc["retries"]) > 0


def test_images_are_pinned(compose):
    for name, svc in compose["services"].items():
        image = svc["image"]
        tag = image.rsplit(":", 1)[-1] if ":" in image else ""
        assert tag and tag != "latest", f"{name} uses an unpinned image: {image}"


def test_bucket_init_waits_for_healthy_minio(compose):
    deps = compose["services"]["minio-init"]["depends_on"]
    assert deps["minio"]["condition"] == "service_healthy"


def test_data_persists_in_named_volumes(compose):
    assert {"pg_data", "minio_data"} <= set(compose["volumes"])


def test_every_compose_variable_is_documented():
    """Each ${VAR:-default} used in compose must appear in .env.example."""
    used = set(re.findall(r"\$\{([A-Z_]+)(?::-[^}]*)?\}", COMPOSE.read_text()))
    missing = used - _env_example_keys()
    assert not missing, f"undocumented compose variables: {sorted(missing)}"
