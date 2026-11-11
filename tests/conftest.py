"""Shared fixtures: a local SparkSession for the silver transform tests."""

from __future__ import annotations

import shutil

import pytest


@pytest.fixture(scope="session")
def spark():
    """A single-core local SparkSession, or skip when no JVM is available."""
    if shutil.which("java") is None:
        pytest.skip("java is not installed; PySpark tests need a JVM")
    pytest.importorskip("pyspark")
    from railfall.spark.session import build_session

    session = build_session(app_name="railfall-tests", master="local[1]", with_lake=False)
    session.conf.set("spark.sql.shuffle.partitions", "1")
    yield session
    session.stop()


@pytest.fixture
def pg_schema():
    """A throwaway schema in a real Postgres, as `(dsn, schema)`; skipped without one.

    Point RAILFALL_TEST_WAREHOUSE_DSN at a database where the user may create
    schemas (the compose admin user works). The schema is dropped afterwards.
    """
    import os
    import uuid

    dsn = os.environ.get("RAILFALL_TEST_WAREHOUSE_DSN")
    if not dsn:
        pytest.skip("RAILFALL_TEST_WAREHOUSE_DSN is not set")
    import psycopg

    try:
        admin = psycopg.connect(dsn, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"warehouse not reachable: {exc}")
    schema = f"test_{uuid.uuid4().hex[:8]}"
    with admin:
        admin.execute(f"create schema {schema}")
        try:
            yield dsn, schema
        finally:
            admin.execute(f"drop schema {schema} cascade")
