"""The DAG's monitoring task: anomalies are summarised; only broken loads fail it."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from railfall.config import Settings
from railfall.monitoring import freshness, runs
from railfall.orchestration import pipeline
from railfall.orchestration.ingest import TaskFailed

NOW = datetime(2026, 11, 22, 6, 0, tzinfo=UTC)
SETTINGS = Settings(llm_summary_enabled=False)


def _fresh(table: str, status: str = freshness.OK, *reasons: str) -> freshness.Freshness:
    return freshness.Freshness(table, status, rows=10, reasons=reasons)


def _run(failed: tuple[str, ...] = ()) -> runs.RunSummary:
    return runs.RunSummary(
        run_id="r1",
        started_at=datetime(2026, 11, 22, 5, 20, tzinfo=UTC),
        finished_at=datetime(2026, 11, 22, 5, 24, tzinfo=UTC),
        tables=2,
        failed=failed,
        totals=dict.fromkeys(runs.COUNTS, 0),
    )


@pytest.fixture
def checks(monkeypatch):
    """Replace the three database reads; tests set what they return."""
    state = {"fresh": [_fresh("stations")], "runs": [_run()], "volumes": []}
    monkeypatch.setattr(freshness, "check", lambda conn, **kw: state["fresh"])
    monkeypatch.setattr(runs, "recent_runs", lambda conn, **kw: state["runs"])
    monkeypatch.setattr(runs, "volume_checks", lambda conn, **kw: state["volumes"])
    return state


def test_healthy_warehouse(checks) -> None:
    out = pipeline.monitor(settings=SETTINGS, conn=object(), now=NOW)
    assert out["summary"]["healthy"]
    assert out["summary"]["text"] == "All monitoring checks passed."
    assert out["runs"]["healthy"]
    json.dumps(out)  # XCom-serialisable


def test_stale_data_is_reported_but_does_not_fail(checks) -> None:
    checks["fresh"] = [_fresh("punctuality_monthly", freshness.STALE, "latest month is old")]
    out = pipeline.monitor(settings=SETTINGS, conn=object(), now=NOW)
    assert not out["summary"]["healthy"]
    assert [a["kind"] for a in out["summary"]["anomalies"]] == ["stale"]


def test_failed_load_fails_the_task(checks) -> None:
    checks["runs"] = [_run(failed=("stations",))]
    with pytest.raises(TaskFailed, match=r"monitor: 1 .*\[failed\] stations"):
        pipeline.monitor(settings=SETTINGS, conn=object(), now=NOW)


def test_llm_is_not_called_when_disabled(checks, monkeypatch) -> None:
    from railfall.monitoring import summary

    checks["fresh"] = [_fresh("stations", freshness.STALE, "loaded 80 h ago")]
    monkeypatch.setattr(summary, "ask_llm", lambda *a, **k: pytest.fail("LLM called"))
    out = pipeline.monitor(settings=SETTINGS, conn=object(), now=NOW)
    assert out["summary"]["source"] == "rules"
