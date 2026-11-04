"""Static checks on the Terraform config: they run without terraform or a provider.

`terraform validate` and `apply` run in the e2e workflow against the real stack;
these tests catch structural mistakes (unpinned providers, secrets with defaults,
dangling references) in the fast unit-test loop.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import hcl2
import pytest

TF_DIR = Path(__file__).resolve().parents[1] / "infra" / "terraform"


def _clean(node: Any) -> Any:
    """Normalise python-hcl2 output: drop ``__meta__`` keys and unquote literals.

    hcl2 >= 7 keeps string literals quoted (``'"private"'``) and block names quoted
    (``'"lake"'``) so they round-trip; the tests want plain Python values.
    """
    if isinstance(node, dict):
        return {
            _clean(k): _clean(v)
            for k, v in node.items()
            if not (isinstance(k, str) and k.startswith("__"))
        }
    if isinstance(node, list):
        return [_clean(v) for v in node]
    if isinstance(node, str) and len(node) >= 2 and node[0] == node[-1] == '"':
        return node[1:-1]
    return node


def _load(name: str) -> dict:
    with (TF_DIR / name).open() as fh:
        return _clean(hcl2.load(fh))


def _blocks(doc: dict, kind: str) -> dict[str, dict]:
    """Flatten hcl2's list-of-single-key-dicts into {name: body}."""
    out: dict[str, dict] = {}
    for item in doc.get(kind, []):
        out.update(item)
    return out


def _resources(doc: dict) -> dict[str, dict]:
    """{"type.name": body} for every resource block in a document."""
    out: dict[str, dict] = {}
    for item in doc.get("resource", []):
        for rtype, named in item.items():
            for name, body in named.items():
                out[f"{rtype}.{name}"] = body
    return out


@pytest.fixture(scope="module")
def variables() -> dict[str, dict]:
    return _blocks(_load("variables.tf"), "variable")


@pytest.fixture(scope="module")
def minio_resources() -> dict[str, dict]:
    return _resources(_load("minio.tf"))


def test_every_tf_file_parses():
    files = sorted(TF_DIR.glob("*.tf"))
    assert files, "no Terraform files found"
    for path in files:
        with path.open() as fh:
            hcl2.load(fh)


def test_providers_are_pinned():
    tf = _load("versions.tf")["terraform"][0]
    providers = tf["required_providers"][0]
    assert set(providers) >= {"minio", "postgresql"}
    for name, spec in providers.items():
        assert "/" in spec["source"], f"{name}: source must be namespaced"
        assert re.match(r"^~> \d+\.\d+\.\d+$", spec["version"]), f"{name}: pin a patch range"


def test_secret_variables_are_sensitive_and_have_no_default(variables):
    secrets = [n for n in variables if re.search(r"password|secret", n)]
    assert secrets, "expected at least one secret variable"
    for name in secrets:
        body = variables[name]
        assert body.get("sensitive") is True, f"{name} must be sensitive"
        assert "default" not in body, f"{name} must not have a default"


def test_variables_are_documented(variables):
    for name, body in variables.items():
        assert body.get("description"), f"variable {name} has no description"


def test_lake_bucket_default_matches_settings(variables):
    from railfall.config import Settings

    assert variables["lake_bucket"]["default"] == Settings().lake_bucket


def test_lake_bucket_is_protected_from_destroy(minio_resources):
    bucket = minio_resources["minio_s3_bucket.lake"]
    assert bucket["force_destroy"] is False
    assert bucket["acl"] == "private"


def test_pipeline_user_gets_the_pipeline_policy(minio_resources):
    attachment = minio_resources["minio_iam_user_policy_attachment.pipeline"]
    assert "minio_iam_user.pipeline" in attachment["user_name"]
    assert "minio_iam_policy.pipeline" in attachment["policy_name"]


def test_pipeline_policy_is_scoped_to_the_lake_bucket():
    doc = _load("minio.tf")
    policy = doc["data"][0]["minio_iam_policy_document"]["pipeline"]
    for statement in policy["statement"]:
        for resource in statement["resources"]:
            assert "minio_s3_bucket.lake.bucket" in resource, resource
            assert "*" not in resource.split(":::", 1)[1].split("}", 1)[0]
        assert not any(a in ("s3:*", "admin:*") for a in statement["actions"])


def test_every_referenced_variable_is_declared(variables):
    used: set[str] = set()
    for path in TF_DIR.glob("*.tf"):
        used |= set(re.findall(r"\bvar\.([a-z_][a-z0-9_]*)", path.read_text()))
    assert used, "no variable references found"
    assert used <= set(variables), f"undeclared: {sorted(used - set(variables))}"


@pytest.fixture(scope="module")
def postgres_doc() -> dict:
    return _load("postgres.tf")


def _locals(doc: dict) -> dict:
    out: dict = {}
    for item in doc.get("locals", []):
        out.update(item)
    return out


def test_warehouse_has_one_schema_per_elt_stage(postgres_doc):
    assert _locals(postgres_doc)["warehouse_schemas"] == ["raw", "staging", "marts"]


def test_every_schema_has_exactly_one_writer(postgres_doc):
    local = _locals(postgres_doc)
    writers = local["schema_writers"]
    assert set(writers) == set(local["warehouse_schemas"])
    assert set(writers.values()) <= set(local["warehouse_roles"])


def test_reader_role_only_reads_marts(postgres_doc):
    local = _locals(postgres_doc)
    assert "reader" not in local["schema_writers"].values()
    readable = {s for s, roles in local["schema_readers"].items() if "reader" in roles}
    assert readable == {"marts"}


def test_pipeline_roles_have_no_admin_rights(postgres_doc):
    role = _resources(postgres_doc)["postgresql_role.pipeline"]
    assert role["login"] is True
    for flag in ("superuser", "create_database", "create_role"):
        assert role[flag] is False, flag


def test_role_passwords_come_from_sensitive_variables(postgres_doc, variables):
    roles = _locals(postgres_doc)["warehouse_roles"]
    for actor, spec in roles.items():
        match = re.fullmatch(r"\$\{var\.([a-z_]+)\}", spec["password"])
        assert match, f"{actor}: password must be a variable reference"
        assert variables[match.group(1)]["sensitive"] is True


def test_schemas_are_not_dropped_with_cascade(postgres_doc):
    schema = _resources(postgres_doc)["postgresql_schema.warehouse"]
    assert schema["drop_cascade"] is False


def test_postgres_db_default_matches_compose(variables):
    import yaml

    compose = yaml.safe_load((TF_DIR.parents[1] / "docker-compose.yml").read_text())
    env = compose["services"]["postgres"]["environment"]
    assert env["POSTGRES_DB"] == f"${{POSTGRES_DB:-{variables['postgres_db']['default']}}}"
