"""The DAG's quality-gate callable: GX checkpoint result -> XCom summary or TaskFailed."""

from __future__ import annotations

import pytest
from test_quality_checkpoint import STATIONS, _db

from railfall.orchestration import pipeline
from railfall.orchestration.ingest import TaskFailed

pytest.importorskip("great_expectations")


def test_clean_warehouse_returns_one_report_per_table(tmp_path):
    summary = pipeline.check_quality(
        tables=["stations", "weather_monthly"], url=_db(tmp_path), schema=None
    )
    assert [r["table"] for r in summary] == ["stations", "weather_monthly"]
    assert all(r["ok"] and r["failures"] == [] for r in summary)
    assert all(r["evaluated"] > 0 for r in summary)


def test_failed_expectations_fail_the_task_with_every_table_listed(tmp_path):
    stations = [*STATIONS, STATIONS[0]]
    url = _db(tmp_path, stations=stations, weather=[])
    with pytest.raises(TaskFailed) as excinfo:
        pipeline.check_quality(tables=["stations", "weather_monthly"], url=url, schema=None)
    message = str(excinfo.value)
    assert message.startswith("check quality: 2 dataset(s) failed")
    assert "stations: expect_column_values_to_be_unique(uic_code)" in message
    assert "weather_monthly: expect_table_row_count_to_be_between" in message


def test_default_url_is_the_transformer_role(monkeypatch):
    seen = {}

    def fake_run(url, tables, *, schema):
        seen.update(url=url, tables=tables, schema=schema)
        return []

    monkeypatch.setenv("DBT_HOST", "postgres")
    monkeypatch.setenv("DBT_PASSWORD", "s3cret")
    monkeypatch.setattr("railfall.quality.checkpoint.run", fake_run)
    assert pipeline.check_quality() == []
    assert seen["url"].startswith("postgresql+psycopg2://railfall_transformer:s3cret@postgres:")
    assert seen == {**seen, "tables": None, "schema": "raw"}
