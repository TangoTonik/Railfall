"""Least-privilege audit of the identities Terraform provisions.

`terraform plan` proves the *declared* grants exist; this module proves they
*behave*: it connects as each pipeline identity and checks that what should work
works and what should be refused is refused.

    python -m railfall.access

Identities and secrets are read from the same `TF_VAR_*` variables Terraform is
applied with (see `.env.example`), so the audit checks exactly what was
provisioned. Everything it creates is dropped again before it exits.

A probe that is expected to be *denied* only passes on a real permission error
(Postgres `InsufficientPrivilege`, S3 `AccessDenied`). Any other failure, such as
an unreachable service, fails the probe: an outage must never look like a
correctly locked-down role.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field

from railfall.config import Settings

ALLOW = "allow"
DENY = "deny"

LAKE_DENIED_CODES = frozenset({"AccessDenied", "AllAccessDisabled"})


@dataclass(frozen=True)
class Identities:
    """Credentials of the pipeline identities, as given to Terraform."""

    lake_user: str = "railfall-pipeline"
    lake_secret: str = field(default="", repr=False)
    loader_password: str = field(default="", repr=False)
    transformer_password: str = field(default="", repr=False)
    reader_password: str = field(default="", repr=False)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Identities:
        env = os.environ if env is None else env
        missing = [
            name
            for name in (
                "TF_VAR_lake_pipeline_secret",
                "TF_VAR_warehouse_loader_password",
                "TF_VAR_warehouse_transformer_password",
                "TF_VAR_warehouse_reader_password",
            )
            if not env.get(name)
        ]
        if missing:
            raise ValueError(f"missing environment variables: {', '.join(missing)}")
        return cls(
            lake_user=env.get("TF_VAR_lake_pipeline_user", cls.lake_user),
            lake_secret=env["TF_VAR_lake_pipeline_secret"],
            loader_password=env["TF_VAR_warehouse_loader_password"],
            transformer_password=env["TF_VAR_warehouse_transformer_password"],
            reader_password=env["TF_VAR_warehouse_reader_password"],
        )

    def warehouse_roles(self) -> dict[str, str]:
        """Role name -> password, matching `local.warehouse_roles` in postgres.tf."""
        return {
            "railfall_loader": self.loader_password,
            "railfall_transformer": self.transformer_password,
            "railfall_reader": self.reader_password,
        }


@dataclass(frozen=True)
class Probe:
    identity: str
    action: str
    expect: str  # ALLOW or DENY
    run: Callable[[], object]


@dataclass(frozen=True)
class ProbeResult:
    identity: str
    action: str
    expect: str
    ok: bool
    detail: str


def is_denied(exc: BaseException) -> bool:
    """True only for genuine permission errors from Postgres or S3."""
    try:
        import psycopg

        if isinstance(exc, psycopg.errors.InsufficientPrivilege):
            return True
    except ImportError:  # pragma: no cover - psycopg is a hard dependency
        pass
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = response.get("Error", {}).get("Code", "")
        return code in LAKE_DENIED_CODES
    return False


def evaluate(probe: Probe) -> ProbeResult:
    def result(ok: bool, detail: str) -> ProbeResult:
        return ProbeResult(probe.identity, probe.action, probe.expect, ok, detail)

    try:
        probe.run()
    except Exception as exc:  # noqa: BLE001 - classified below, never swallowed silently
        detail = _first_line(exc)
        if probe.expect == DENY and is_denied(exc):
            return result(True, f"denied: {detail}")
        return result(False, f"{type(exc).__name__}: {detail}")
    if probe.expect == ALLOW:
        return result(True, "allowed")
    return result(False, "allowed, but should have been denied")


def run_probes(probes: Sequence[Probe]) -> list[ProbeResult]:
    return [evaluate(p) for p in probes]


# --------------------------------------------------------------------------- warehouse


def role_dsn(base_dsn: str, role: str, password: str) -> str:
    """Same host/port/database as `base_dsn`, connecting as another role."""
    from psycopg.conninfo import make_conninfo

    return make_conninfo(base_dsn, user=role, password=password)


def _sql(dsn: str, *statements: str) -> Callable[[], None]:
    """Run statements in one transaction that is always rolled back."""

    def run() -> None:
        import psycopg

        with psycopg.connect(dsn, connect_timeout=5) as conn:
            try:
                for stmt in statements:
                    conn.execute(stmt)
            finally:
                conn.rollback()

    return run


@contextmanager
def committed_table(dsn: str, qualified_name: str) -> Iterator[str]:
    """Create (and commit) a one-row table as the role in `dsn`; drop it on exit.

    Cross-role probes need a committed table: a reader cannot see another
    session's uncommitted DDL. Creating it as the schema owner also exercises
    the `ALTER DEFAULT PRIVILEGES` grants Terraform set up.
    """
    import psycopg

    with psycopg.connect(dsn, connect_timeout=5, autocommit=True) as conn:
        conn.execute(f"create table {qualified_name} (id int primary key)")
        conn.execute(f"insert into {qualified_name} values (1)")
    try:
        yield qualified_name
    finally:
        with psycopg.connect(dsn, connect_timeout=5, autocommit=True) as conn:
            conn.execute(f"drop table if exists {qualified_name}")


def warehouse_probes(dsns: Mapping[str, str], raw_table: str, marts_table: str) -> list[Probe]:
    """Expected behaviour of each role. `raw_table`/`marts_table` must already exist."""
    loader = dsns["railfall_loader"]
    transformer = dsns["railfall_transformer"]
    reader = dsns["railfall_reader"]
    tmp = f"_access_{uuid.uuid4().hex[:8]}"

    def create(schema: str) -> str:
        return f"create table {schema}.{tmp} (id int)"

    return [
        Probe("railfall_loader", "create table in raw", ALLOW, _sql(loader, create("raw"))),
        Probe(
            "railfall_loader",
            "insert into raw",
            ALLOW,
            _sql(loader, f"insert into {raw_table} values (2)"),
        ),
        Probe("railfall_loader", "create table in staging", DENY, _sql(loader, create("staging"))),
        Probe("railfall_loader", "create table in marts", DENY, _sql(loader, create("marts"))),
        Probe("railfall_loader", "create table in public", DENY, _sql(loader, create("public"))),
        Probe("railfall_loader", "create schema", DENY, _sql(loader, f"create schema {tmp}")),
        Probe(
            "railfall_transformer",
            "select from raw",
            ALLOW,
            _sql(transformer, f"select * from {raw_table}"),
        ),
        Probe(
            "railfall_transformer",
            "insert into raw",
            DENY,
            _sql(transformer, f"insert into {raw_table} values (2)"),
        ),
        Probe(
            "railfall_transformer", "create table in raw", DENY, _sql(transformer, create("raw"))
        ),
        Probe(
            "railfall_transformer",
            "create table in staging",
            ALLOW,
            _sql(transformer, create("staging")),
        ),
        Probe(
            "railfall_transformer",
            "create table in marts",
            ALLOW,
            _sql(transformer, create("marts")),
        ),
        Probe(
            "railfall_reader",
            "select from marts",
            ALLOW,
            _sql(reader, f"select * from {marts_table}"),
        ),
        Probe(
            "railfall_reader",
            "insert into marts",
            DENY,
            _sql(reader, f"insert into {marts_table} values (2)"),
        ),
        Probe("railfall_reader", "create table in marts", DENY, _sql(reader, create("marts"))),
        Probe(
            "railfall_reader", "select from raw", DENY, _sql(reader, f"select * from {raw_table}")
        ),
    ]


def audit_warehouse(settings: Settings, identities: Identities) -> list[ProbeResult]:
    dsns = {
        role: role_dsn(settings.warehouse_dsn, role, password)
        for role, password in identities.warehouse_roles().items()
    }
    suffix = uuid.uuid4().hex[:8]
    with (
        committed_table(dsns["railfall_loader"], f"raw._access_probe_{suffix}") as raw_table,
        committed_table(
            dsns["railfall_transformer"], f"marts._access_probe_{suffix}"
        ) as marts_table,
    ):
        return run_probes(warehouse_probes(dsns, raw_table, marts_table))


# --------------------------------------------------------------------------------- lake


def _lake_client(settings: Settings, identities: Identities):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=settings.lake_endpoint_url,
        aws_access_key_id=identities.lake_user,
        aws_secret_access_key=identities.lake_secret,
        region_name="us-east-1",
        config=Config(connect_timeout=5, read_timeout=5, retries={"max_attempts": 1}),
    )


def lake_probes(client, bucket: str) -> list[Probe]:
    key = f"bronze/_access/{uuid.uuid4().hex}.txt"
    other_bucket = f"railfall-access-{uuid.uuid4().hex[:8]}"

    def put_get_delete() -> None:
        client.put_object(Bucket=bucket, Key=key, Body=b"access probe")
        try:
            client.get_object(Bucket=bucket, Key=key)["Body"].read()
        finally:
            client.delete_object(Bucket=bucket, Key=key)

    user = "lake pipeline user"
    return [
        Probe(user, "list lake bucket", ALLOW, lambda: client.list_objects_v2(Bucket=bucket)),
        Probe(user, "put/get/delete in bronze/", ALLOW, put_get_delete),
        Probe(user, "create bucket", DENY, lambda: client.create_bucket(Bucket=other_bucket)),
        Probe(user, "delete lake bucket", DENY, lambda: client.delete_bucket(Bucket=bucket)),
    ]


def audit_lake(settings: Settings, identities: Identities) -> list[ProbeResult]:
    client = _lake_client(settings, identities)
    return run_probes(lake_probes(client, settings.lake_bucket))


# --------------------------------------------------------------------------------- CLI


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip() or type(exc).__name__
    return text.splitlines()[0]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit pipeline identities' privileges.")
    parser.add_argument("--only", choices=["lake", "warehouse"], help="audit one service only")
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    try:
        identities = Identities.from_env()
    except ValueError as exc:
        print(f"[FAIL] {exc}")
        return 1

    results: list[ProbeResult] = []
    audits = {"lake": audit_lake, "warehouse": audit_warehouse}
    for name, audit in audits.items():
        if args.only not in (None, name):
            continue
        try:
            results += audit(settings, identities)
        except Exception as exc:  # noqa: BLE001 - setup failure, e.g. service unreachable
            results.append(ProbeResult(name, "audit setup", ALLOW, False, _first_line(exc)))

    for r in results:
        status = "ok" if r.ok else "FAIL"
        print(f"[{status:>4}] {r.identity:<22} {r.expect:<5} {r.action:<26} {r.detail}")
    failed = sum(not r.ok for r in results)
    print(f"{len(results) - failed}/{len(results)} access probes as expected")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
