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


def _env_example() -> dict[str, str]:
    values = {}
    for line in ENV_EXAMPLE.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            values[key] = value
    return values


def test_app_uses_terraform_identities_not_admin():
    """The pipelines must connect as the least-privilege identities Terraform creates."""
    from urllib.parse import urlsplit

    env = _env_example()
    assert env["LAKE_ACCESS_KEY"] != env["MINIO_ROOT_USER"]
    assert env["LAKE_SECRET_KEY"] == env["TF_VAR_lake_pipeline_secret"]

    dsn = urlsplit(env["WAREHOUSE_DSN"])
    assert dsn.username == "railfall_loader"
    assert dsn.username != env["POSTGRES_USER"]
    assert dsn.password == env["TF_VAR_warehouse_loader_password"]
    assert dsn.path.lstrip("/") == env["POSTGRES_DB"]


AIRFLOW_SERVICES = ("airflow-init", "airflow-webserver", "airflow-scheduler")


def test_airflow_services_sit_behind_a_profile(compose):
    """`docker compose up` must not start Airflow before Terraform made its database."""
    for name in AIRFLOW_SERVICES:
        assert compose["services"][name]["profiles"] == ["airflow"], name
    for name in ("postgres", "minio"):
        assert "profiles" not in compose["services"][name], name


def test_airflow_runs_the_local_executor_on_its_own_database(compose):
    env = compose["services"]["airflow-scheduler"]["environment"]
    assert env["AIRFLOW__CORE__EXECUTOR"] == "LocalExecutor"
    assert env["AIRFLOW__CORE__LOAD_EXAMPLES"] == "false"
    conn = env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    assert "@postgres:5432/${AIRFLOW_DB_NAME:-airflow}" in conn
    assert "${POSTGRES_USER" not in conn, "Airflow must not log in as the Postgres admin"


def test_airflow_tasks_use_pipeline_identities_on_the_compose_network(compose):
    env = compose["services"]["airflow-scheduler"]["environment"]
    assert env["LAKE_ENDPOINT_URL"] == "http://minio:9000"
    assert env["LAKE_ACCESS_KEY"].startswith("${LAKE_ACCESS_KEY")
    assert "${MINIO_ROOT" not in env["LAKE_SECRET_KEY"]
    assert env["WAREHOUSE_DSN"].startswith("postgresql://railfall_loader:")
    assert "@postgres:5432/" in env["WAREHOUSE_DSN"]


def test_airflow_waits_for_its_migration(compose):
    for name in ("airflow-webserver", "airflow-scheduler"):
        deps = compose["services"][name]["depends_on"]
        assert deps["airflow-init"]["condition"] == "service_completed_successfully", name
    init = compose["services"]["airflow-init"]
    assert init["environment"]["_AIRFLOW_DB_MIGRATE"] == "true"
    assert init["depends_on"]["postgres"]["condition"] == "service_healthy"


@pytest.mark.parametrize("service", ["airflow-webserver", "airflow-scheduler"])
def test_airflow_long_running_services_have_healthchecks(compose, service):
    hc = compose["services"][service]["healthcheck"]
    assert "curl" in hc["test"] and int(hc["retries"]) > 0


def test_dags_are_mounted_read_only(compose):
    for name in AIRFLOW_SERVICES:
        assert "./dags:/opt/airflow/dags:ro" in compose["services"][name]["volumes"], name
    assert (ROOT / "dags").is_dir()


def test_airflow_image_matches_its_dockerfile(compose):
    svc = compose["services"]["airflow-scheduler"]
    dockerfile = (ROOT / svc["build"]["dockerfile"]).read_text()
    version = svc["build"]["args"]["AIRFLOW_VERSION"]
    python = svc["build"]["args"]["PYTHON_VERSION"]
    assert svc["image"] == f"railfall-airflow:{version}"
    assert f"FROM apache/airflow:{version}-python{python}" in dockerfile
    assert f"ARG AIRFLOW_VERSION={version}" in dockerfile
    assert "constraints-${AIRFLOW_VERSION}" in dockerfile, "install railfall under constraints"


def test_airflow_db_password_matches_terraform():
    env = _env_example()
    assert env["AIRFLOW_DB_PASSWORD"] == env["TF_VAR_airflow_db_password"]


def test_airflow_image_can_run_the_spark_tasks():
    """The full DAG runs PySpark in the worker: it needs a JRE and Spark 4.0."""
    dockerfile = (ROOT / "infra/airflow/Dockerfile").read_text()
    assert "openjdk-17-jre-headless" in dockerfile
    assert "ENV JAVA_HOME=" in dockerfile
    assert '"pyspark>=4.0,<4.1"' in dockerfile
    pyproject = (ROOT / "pyproject.toml").read_text()
    assert 'spark = ["pyspark>=4.0,<4.1"]' in pyproject, "image and spark extra drifted"
