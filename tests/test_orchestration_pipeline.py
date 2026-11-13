"""Unit tests for the full DAG's Spark task callables, on a local lake (no S3A)."""

from __future__ import annotations

import json
import shutil

import pytest
from lakes import derived_lake

from railfall.config import Settings
from railfall.orchestration import pipeline
from railfall.orchestration.ingest import TaskFailed
from railfall.spark.write import TABLES

DAY = "2026-11-12"
SETTINGS = Settings(lake_bucket="railfall-lake")


class FakeSession:
    stopped = False

    def stop(self) -> None:
        self.stopped = True


def test_own_session_is_stopped_even_when_the_task_fails(monkeypatch, tmp_path) -> None:
    session = FakeSession()
    monkeypatch.setattr(
        "railfall.spark.session.build_session", lambda *a, **k: session, raising=True
    )

    def boom(*args, **kwargs):
        raise RuntimeError("spark died")

    monkeypatch.setattr("railfall.spark.job.run", boom)
    with pytest.raises(RuntimeError, match="spark died"):
        pipeline.build_silver(DAY, settings=SETTINGS, root=str(tmp_path))
    assert session.stopped


def test_injected_session_is_left_running(monkeypatch, tmp_path) -> None:
    session = FakeSession()
    monkeypatch.setattr("railfall.spark.job.run", lambda *a, **k: [])
    assert pipeline.build_silver(DAY, settings=SETTINGS, root=str(tmp_path), spark=session) == []
    assert not session.stopped


def test_lake_root_is_the_s3a_bucket() -> None:
    assert pipeline._root(SETTINGS, None) == "s3a://railfall-lake"
    assert pipeline._root(SETTINGS, "/tmp/lake/") == "/tmp/lake"


@pytest.mark.spark
def test_build_then_verify_silver_on_a_local_lake(spark, tmp_path) -> None:
    derived_lake(tmp_path)
    root = str(tmp_path)

    built = pipeline.build_silver(DAY, settings=SETTINGS, root=root, spark=spark)
    assert [r["table"] for r in built] == list(TABLES)
    assert all(r["ok"] and r["rows"] > 0 for r in built)
    json.dumps(built)  # XCom-safe

    again = pipeline.build_silver(DAY, settings=SETTINGS, root=root, spark=spark)
    assert [(r["table"], r["rows"]) for r in again] == [(r["table"], r["rows"]) for r in built]

    checked = pipeline.verify_silver(settings=SETTINGS, root=root, spark=spark)
    assert all(r["ok"] for r in checked), checked
    assert {r["table"]: r["rows"] for r in checked} == {r["table"]: r["rows"] for r in built}
    json.dumps(checked)


@pytest.mark.spark
def test_missing_bronze_fails_the_task_after_building_the_rest(spark, tmp_path) -> None:
    derived_lake(tmp_path)
    shutil.rmtree(tmp_path / "bronze" / "meteo")
    root = str(tmp_path)

    with pytest.raises(TaskFailed, match=r"build silver: \d dataset\(s\) failed: .*MissingBronze"):
        pipeline.build_silver(DAY, settings=SETTINGS, root=root, spark=spark)
    # Tables that do not need weather were still written.
    assert (tmp_path / "silver" / "stations").exists()


@pytest.mark.spark
def test_verify_silver_fails_on_a_table_never_built(spark, tmp_path) -> None:
    with pytest.raises(TaskFailed, match="verify silver: 1 dataset"):
        pipeline.verify_silver(
            tables=["stations"], settings=SETTINGS, root=str(tmp_path), spark=spark
        )
