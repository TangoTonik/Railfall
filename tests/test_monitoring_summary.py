import io
import json
import urllib.error

import pytest

from railfall.config import Settings
from railfall.monitoring import summary as sm

FRESHNESS = [
    {"table": "stations", "status": "ok", "reasons": []},
    {
        "table": "punctuality_monthly",
        "status": "stale",
        "reasons": ["newest month 2026-06-01 is 144 days old (max 95)"],
    },
    {"table": "weather_daily", "status": "empty", "reasons": ["table has no rows"]},
]

RUNS = {
    "healthy": False,
    "runs": [
        {"run_id": "r2", "failed": ["route_departments"]},
        {"run_id": "r1", "failed": ["stations"]},  # older run: not reported
    ],
    "volumes": [
        {"table": "stations", "ok": True, "reasons": []},
        {
            "table": "weather_daily",
            "ok": False,
            "reasons": ["staged 10 rows, 1% of the usual 1000"],
        },
    ],
}


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def opener_returning(payload, calls=None):
    def opener(request, timeout):
        if calls is not None:
            calls.append((request, timeout))
        raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        return FakeResponse(raw)

    return opener


def failing_opener(request, timeout):
    raise urllib.error.URLError("connection refused")


ENABLED = Settings(llm_summary_enabled=True, llm_url="http://ollama:11434", llm_timeout_s=7)


def test_collect_orders_by_severity_and_skips_old_runs():
    anomalies = sm.collect(FRESHNESS, RUNS)
    assert [(a.kind, a.table) for a in anomalies] == [
        ("failed", "route_departments"),
        ("empty", "weather_daily"),
        ("stale", "punctuality_monthly"),
        ("volume", "weather_daily"),
    ]


def test_collect_deduplicates_and_handles_missing_inputs():
    assert sm.collect() == []
    assert len(sm.collect(FRESHNESS + FRESHNESS)) == 2


def test_unknown_status_is_treated_as_stale():
    [a] = sm.freshness_anomalies([{"table": "t", "status": "weird"}])
    assert a.kind == "stale"
    assert a.detail == "status weird"


def test_digest_lists_every_anomaly():
    text = sm.digest(sm.collect(FRESHNESS, RUNS))
    assert text.splitlines()[0] == "4 anomalies on 3 tables:"
    assert "- [failed] route_departments: load failed in run r2" in text
    assert sm.digest([]) == "All monitoring checks passed."


def test_llm_off_by_default_never_calls_the_model():
    calls = []
    s = sm.summarize(sm.collect(FRESHNESS), Settings(), opener=opener_returning({}, calls))
    assert s.source == "rules"
    assert s.text == s.digest
    assert calls == []


def test_healthy_pipeline_skips_the_model_even_when_enabled():
    calls = []
    s = sm.summarize([], ENABLED, opener=opener_returning({"response": "x"}, calls))
    assert s.healthy and s.source == "rules" and calls == []


def test_llm_summary_used_when_enabled():
    calls = []
    answer = {"response": "  The weather load staged almost nothing; check the Météo-France file. "}
    s = sm.summarize(sm.collect(FRESHNESS, RUNS), ENABLED, opener=opener_returning(answer, calls))
    assert s.source == "llm"
    assert s.text.startswith("The weather load")
    assert "[failed] route_departments" in s.digest

    [(request, timeout)] = calls
    assert request.full_url == "http://ollama:11434/api/generate"
    assert timeout == 7
    body = json.loads(request.data)
    assert body["stream"] is False
    assert body["options"] == {"temperature": 0}
    assert '"table": "route_departments"' in body["prompt"]
    assert "Use only the facts given" in body["prompt"]


@pytest.mark.parametrize(
    ("opener", "error"),
    [
        (failing_opener, "unreachable"),
        (opener_returning(b"<html>"), "invalid JSON"),
        (opener_returning({"response": "   "}), "empty"),
        (opener_returning({"response": "x" * (sm.MAX_LLM_CHARS + 1)}), "too long"),
        (opener_returning(["not", "a", "dict"]), "empty"),
    ],
)
def test_llm_failures_fall_back_to_the_digest(opener, error):
    s = sm.summarize(sm.collect(FRESHNESS), ENABLED, opener=opener)
    assert s.source == "rules"
    assert s.text == s.digest
    assert error in s.llm_error


def test_use_llm_argument_overrides_settings():
    s = sm.summarize(
        sm.collect(FRESHNESS), Settings(), use_llm=True, opener=opener_returning({"response": "ok"})
    )
    assert s.source == "llm"
    s = sm.summarize(sm.collect(FRESHNESS), ENABLED, use_llm=False, opener=failing_opener)
    assert s.source == "rules" and s.llm_error is None


def test_cli_writes_report_and_exits_nonzero_on_anomalies(tmp_path, capsys):
    fresh = tmp_path / "freshness.json"
    fresh.write_text(json.dumps(FRESHNESS))
    runs = tmp_path / "runs.json"
    runs.write_text(json.dumps(RUNS))
    out = tmp_path / "summary.json"

    code = sm.main(["--freshness", str(fresh), "--runs", str(runs), "--report", str(out)], env={})
    assert code == 1
    assert "4 anomalies" in capsys.readouterr().out
    report = json.loads(out.read_text())
    assert report["source"] == "rules"
    assert len(report["anomalies"]) == 4


def test_cli_healthy_exit_zero(tmp_path, capsys):
    fresh = tmp_path / "freshness.json"
    fresh.write_text(json.dumps([{"table": "stations", "status": "ok"}]))
    assert sm.main(["--freshness", str(fresh), "--json"], env={}) == 0
    assert json.loads(capsys.readouterr().out)["healthy"] is True


def test_cli_llm_unreachable_warns_on_stderr(tmp_path, capsys):
    fresh = tmp_path / "freshness.json"
    fresh.write_text(json.dumps(FRESHNESS))
    # Port 9 (discard) on localhost: refused immediately, no real model needed.
    env = {"LLM_URL": "http://127.0.0.1:9", "LLM_TIMEOUT_S": "2"}
    assert sm.main(["--freshness", str(fresh), "--llm"], env=env) == 1
    captured = capsys.readouterr()
    assert "LLM summary unavailable" in captured.err
    assert "2 anomalies" in captured.out


def test_cli_requires_an_input():
    with pytest.raises(SystemExit):
        sm.main([], env={})
