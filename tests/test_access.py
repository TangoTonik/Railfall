"""Unit tests for railfall.access. The real privilege checks run in e2e against the stack."""

from __future__ import annotations

import psycopg
import pytest
from botocore.exceptions import ClientError

from railfall import access
from railfall.access import ALLOW, DENY, Identities, Probe, evaluate

TF_ENV = {
    "TF_VAR_lake_pipeline_secret": "pipeline-secret",
    "TF_VAR_warehouse_loader_password": "lp",
    "TF_VAR_warehouse_transformer_password": "tp",
    "TF_VAR_warehouse_reader_password": "rp",
}


def _raise(exc: BaseException):
    def run() -> None:
        raise exc

    return run


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "PutObject")


# ------------------------------------------------------------------ classification


@pytest.mark.parametrize(
    "exc",
    [
        psycopg.errors.InsufficientPrivilege("permission denied for schema marts"),
        _client_error("AccessDenied"),
    ],
)
def test_permission_errors_are_denials(exc):
    assert access.is_denied(exc)


@pytest.mark.parametrize(
    "exc",
    [
        psycopg.OperationalError("connection refused"),
        psycopg.errors.UndefinedTable("relation does not exist"),
        _client_error("NoSuchBucket"),
        ConnectionError("boom"),
    ],
)
def test_other_errors_are_not_denials(exc):
    assert not access.is_denied(exc)


def test_expected_allow_that_succeeds_passes():
    result = evaluate(Probe("loader", "create in raw", ALLOW, lambda: None))
    assert result.ok
    assert result.detail == "allowed"


def test_expected_allow_that_is_refused_fails():
    exc = psycopg.errors.InsufficientPrivilege("permission denied for schema raw")
    result = evaluate(Probe("loader", "create in raw", ALLOW, _raise(exc)))
    assert not result.ok
    assert "InsufficientPrivilege" in result.detail


def test_expected_deny_that_is_refused_passes():
    result = evaluate(Probe("lake", "create bucket", DENY, _raise(_client_error("AccessDenied"))))
    assert result.ok
    assert result.detail.startswith("denied")


def test_expected_deny_that_succeeds_fails():
    result = evaluate(Probe("reader", "insert into marts", DENY, lambda: None))
    assert not result.ok
    assert "should have been denied" in result.detail


def test_outage_never_counts_as_denied():
    exc = psycopg.OperationalError("connection to server failed")
    result = evaluate(Probe("reader", "insert into marts", DENY, _raise(exc)))
    assert not result.ok
    assert "OperationalError" in result.detail


# ------------------------------------------------------------------- identities


def test_identities_from_env():
    ids = Identities.from_env({**TF_ENV, "TF_VAR_lake_pipeline_user": "etl"})
    assert ids.lake_user == "etl"
    assert ids.warehouse_roles() == {
        "railfall_loader": "lp",
        "railfall_transformer": "tp",
        "railfall_reader": "rp",
    }


def test_identities_default_lake_user_matches_terraform():
    assert Identities.from_env(TF_ENV).lake_user == "railfall-pipeline"


def test_identities_report_every_missing_variable():
    with pytest.raises(ValueError) as err:
        Identities.from_env({"TF_VAR_lake_pipeline_secret": "x"})
    msg = str(err.value)
    assert "TF_VAR_warehouse_loader_password" in msg
    assert "TF_VAR_warehouse_reader_password" in msg
    assert "TF_VAR_lake_pipeline_secret" not in msg


def test_identities_repr_hides_secrets():
    assert "pipeline-secret" not in repr(Identities.from_env(TF_ENV))


def test_role_names_match_terraform_config():
    hcl2 = pytest.importorskip("hcl2")
    from pathlib import Path

    tf = Path(__file__).resolve().parents[1] / "infra" / "terraform" / "postgres.tf"
    with tf.open() as fh:
        config = hcl2.load(fh)
    roles = config["locals"][0]["warehouse_roles"]
    assert {r["name"].strip('"') for r in roles.values()} == set(Identities().warehouse_roles())


def test_role_dsn_keeps_host_and_database():
    dsn = access.role_dsn("postgresql://rail:rail@db:6543/warehouse", "railfall_reader", "pw")
    params = psycopg.conninfo.conninfo_to_dict(dsn)
    assert params["user"] == "railfall_reader"
    assert params["password"] == "pw"
    assert params["host"] == "db"
    assert params["port"] == "6543"
    assert params["dbname"] == "warehouse"


def test_warehouse_probes_cover_every_role_both_ways():
    dsns = dict.fromkeys(Identities().warehouse_roles(), "postgresql://x")
    probes = access.warehouse_probes(dsns, "raw.t", "marts.t")
    for role in dsns:
        expects = {p.expect for p in probes if p.identity == role}
        assert expects == {ALLOW, DENY}, role


# --------------------------------------------------------------------------- lake


@pytest.fixture
def s3():
    moto = pytest.importorskip("moto")
    import boto3

    with moto.mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="railfall-lake")
        yield client


def test_lake_allowed_probes_work_and_leave_nothing(s3):
    probes = [p for p in access.lake_probes(s3, "railfall-lake") if p.expect == ALLOW]
    results = access.run_probes(probes)
    assert results
    assert all(r.ok for r in results)
    assert s3.list_objects_v2(Bucket="railfall-lake").get("KeyCount", 0) == 0


def test_lake_audit_flags_an_admin_identity(s3):
    """moto enforces no IAM, like a root key would: the deny probes must fail."""
    results = access.run_probes(access.lake_probes(s3, "railfall-lake"))
    create = next(r for r in results if r.action == "create bucket")
    assert not create.ok


# ---------------------------------------------------------------------------- CLI


def test_main_fails_cleanly_without_terraform_inputs(monkeypatch, capsys):
    for name in TF_ENV:
        monkeypatch.delenv(name, raising=False)
    assert access.main([]) == 1
    assert "missing environment variables" in capsys.readouterr().out


def test_main_reports_unreachable_services(monkeypatch, capsys):
    for name, value in TF_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("WAREHOUSE_DSN", "postgresql://rail:rail@127.0.0.1:1/warehouse")
    assert access.main(["--only", "warehouse"]) == 1
    out = capsys.readouterr().out
    assert "audit setup" in out
    assert "0/1 access probes" in out
