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
    assert {"postgres", "minio"} <= set(compose["services"])


def test_provisioning_is_left_to_terraform(compose):
    """Day 3: the bucket comes from Terraform; a compose init job would race it."""
    assert "minio-init" not in compose["services"]


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


def test_data_persists_in_named_volumes(compose):
    assert {"pg_data", "minio_data"} <= set(compose["volumes"])


def test_every_compose_variable_is_documented():
    """Each ${VAR:-default} used in compose must appear in .env.example."""
    used = set(re.findall(r"\$\{([A-Z_]+)(?::-[^}]*)?\}", COMPOSE.read_text()))
    missing = used - _env_example_keys()
    assert not missing, f"undocumented compose variables: {sorted(missing)}"


def test_minio_is_built_from_pinned_source(compose):
    """MinIO images are no longer pullable (Docker Hub denied, quay.io unauthorized)."""
    for name in ("minio",):
        svc = compose["services"][name]
        assert svc["build"]["context"] == "./infra/minio", name
        for arg, release in svc["build"]["args"].items():
            assert release.startswith("RELEASE.20"), f"{name}: {arg} not pinned: {release}"


def test_dockerfile_build_args_match_compose(compose):
    dockerfile = (ROOT / "infra/minio/Dockerfile").read_text()
    for arg, release in compose["services"]["minio"]["build"]["args"].items():
        assert f"ARG {arg}={release}" in dockerfile, f"Dockerfile default for {arg} drifted"


def test_dockerfile_base_images_are_pinned():
    dockerfile = (ROOT / "infra/minio/Dockerfile").read_text()
    bases = re.findall(r"^FROM\s+(\S+)", dockerfile, flags=re.M)
    assert bases
    for base in bases:
        assert ":" in base and not base.endswith(":latest"), base
