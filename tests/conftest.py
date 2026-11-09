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
