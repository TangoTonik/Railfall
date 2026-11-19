"""railfall.transform.dbt: argument building and result reporting.

dbt's runner is replaced by a fake returning dbt-shaped result objects; one test
runs the real `dbt parse` in-process, which needs no database.
"""

from __future__ import annotations

import importlib.util
import json
from enum import StrEnum
from pathlib import Path
from types import SimpleNamespace

import pytest

from railfall.transform import dbt as tdbt

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"


class Status(StrEnum):  # mirrors dbt's str enums (RunStatus, TestStatus)
    SUCCESS = "success"
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    ERROR = "error"
    RUNTIME_ERROR = "runtime error"
    SKIPPED = "skipped"


class Resource(StrEnum):
    MODEL = "model"
    TEST = "test"
    SOURCE = "source"


def _result(uid, rtype, status, *, seconds=0.5, message=None, failures=None):
    return SimpleNamespace(
        node=SimpleNamespace(unique_id=uid, resource_type=rtype),
        status=status,
        execution_time=seconds,
        message=message,
        failures=failures,
    )


def _fake(success, results, exception=None, calls=None):
    def invoke(args):
        if calls is not None:
            calls.append(args)
        return SimpleNamespace(
            success=success, result=SimpleNamespace(results=results), exception=exception
        )

    return invoke


def test_dbt_args_points_at_the_repo_project_by_default(monkeypatch):
    monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)
    monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
    args = tdbt.dbt_args("build", select=["staging"], target="ci")
    assert args[0] == "build"
    assert args[args.index("--project-dir") + 1] == str(DBT_DIR)
    assert args[args.index("--profiles-dir") + 1] == str(DBT_DIR)
    assert args[args.index("--target") + 1] == "ci"
    assert args[args.index("--select") + 1] == "staging"


def test_selected_builds_only_run_tests_whose_parents_are_buildable():
    # Regression: with dbt's default `eager`, `--select staging` also ran the marts'
    # singular tests (they read a staging view) before the facts existed.
    args = tdbt.dbt_args("build", select=["staging"])
    assert args[-2:] == ["--indirect-selection", "buildable"]
    args = tdbt.dbt_args("test", exclude=["marts"], indirect_selection="cautious")
    assert args[-2:] == ["--indirect-selection", "cautious"]


def test_indirect_selection_is_left_alone_when_it_cannot_apply():
    assert "--indirect-selection" not in tdbt.dbt_args("build")
    assert "--indirect-selection" not in tdbt.dbt_args("run", select=["marts"])
    assert "--indirect-selection" not in tdbt.dbt_args("source freshness", select=["raw"])


def test_unknown_indirect_selection_is_rejected():
    with pytest.raises(ValueError, match="unsupported indirect selection"):
        tdbt.dbt_args("build", select=["staging"], indirect_selection="lazy")


def test_buildable_selection_keeps_marts_tests_out_of_staging():
    # Real dbt resolution of the project's graph (`dbt ls` needs no database).
    pytest.importorskip("dbt.cli.main")
    from dbt.cli.main import dbtRunner

    def tests_for(select: str) -> set[str]:
        args = tdbt.dbt_args("build", select=[select])
        res = dbtRunner().invoke(
            ["ls", *args[1:], "--resource-type", "test", "--output", "name", "--quiet"]
        )
        assert res.success, res.exception
        return set(res.result)

    staging = tests_for("staging")
    marts = tests_for("marts")
    assert "assert_fact_route_month_keeps_every_staging_row" not in staging
    assert "assert_fact_route_month_keeps_every_staging_row" in marts


def test_dirs_follow_the_environment(tmp_path):
    env = {"DBT_PROJECT_DIR": str(tmp_path / "proj")}
    assert tdbt.project_dir(env) == tmp_path / "proj"
    # profiles default to the project dir unless set on their own
    assert tdbt.profiles_dir(env) == tmp_path / "proj"
    env["DBT_PROFILES_DIR"] = str(tmp_path / "prof")
    assert tdbt.profiles_dir(env) == tmp_path / "prof"


def test_multi_word_command_is_split():
    args = tdbt.dbt_args("source freshness")
    assert args[:2] == ["source", "freshness"]


def test_docs_generate_is_a_supported_command():
    assert tdbt.dbt_args("docs generate")[:2] == ["docs", "generate"]


def test_docs_generate_also_writes_the_static_site():
    assert tdbt.dbt_args("docs generate")[-1] == "--static"
    assert "--static" not in tdbt.dbt_args("build")


def test_docs_generate_fails_on_catalog_errors():
    def invoke(args):  # `dbt docs generate` returns a CatalogArtifact
        catalog = SimpleNamespace(nodes={}, sources={}, errors=["relation marts.x not found"])
        return SimpleNamespace(success=True, result=catalog, exception=None)

    report = tdbt.run_dbt("docs generate", invoke=invoke)
    assert not report.ok
    assert report.errors == ["catalog: relation marts.x not found"]

    def clean(args):
        return SimpleNamespace(
            success=True, result=SimpleNamespace(nodes={}, errors=None), exception=None
        )

    assert tdbt.run_dbt("docs generate", invoke=clean).ok


def test_unknown_command_is_rejected():
    with pytest.raises(ValueError, match="unsupported dbt command"):
        tdbt.dbt_args("seed --full-refresh")


def test_successful_build_reports_every_node():
    calls: list[list[str]] = []
    results = [
        _result("model.railfall.stg_sncf__stations", Resource.MODEL, Status.SUCCESS),
        _result("test.railfall.not_null_x", Resource.TEST, Status.PASS, failures=0),
        _result("test.railfall.freshness_ish", Resource.TEST, Status.WARN, failures=3),
    ]
    report = tdbt.run_dbt("build", select=["staging"], invoke=_fake(True, results, calls=calls))
    assert report.ok
    assert report.errors == []
    assert report.counts == {"pass": 1, "success": 1, "warn": 1}
    assert [n.unique_id for n in report.by_type("model")] == ["model.railfall.stg_sncf__stations"]
    assert calls[0][0] == "build"


def test_source_freshness_reports_sources_and_fails_on_stale_data():
    # Freshness results carry the SourceDefinition as `node`: resource_type "source".
    fresh = _result("source.railfall.raw.stations", Resource.SOURCE, Status.PASS)
    stale = _result(
        "source.railfall.raw.weather_daily", Resource.SOURCE, Status.ERROR, message="stale"
    )
    calls: list[list[str]] = []
    report = tdbt.run_dbt("source freshness", invoke=_fake(True, [fresh], calls=calls))
    assert report.ok and [n.resource_type for n in report.nodes] == ["source"]
    assert calls[0][:2] == ["source", "freshness"]
    late = tdbt.run_dbt("source freshness", invoke=_fake(False, [fresh, stale]))
    assert not late.ok
    assert late.errors == ["source.railfall.raw.weather_daily: error (stale)"]


FAILED = [Status.FAIL, Status.ERROR, Status.RUNTIME_ERROR, Status.SKIPPED]


@pytest.mark.parametrize("status", FAILED)
def test_any_failed_node_fails_the_report(status):
    results = [
        _result("model.railfall.a", Resource.MODEL, Status.SUCCESS),
        _result("test.railfall.b", Resource.TEST, status, message="boom"),
    ]
    # dbt can report success=True with a skipped node; the report must not.
    report = tdbt.run_dbt("build", invoke=_fake(True, results))
    assert not report.ok
    assert report.errors == [f"test.railfall.b: {status.value} (boom)"]


def test_runner_exception_is_reported():
    report = tdbt.run_dbt("build", invoke=_fake(False, None, exception=RuntimeError("no profile")))
    assert not report.ok
    assert report.nodes == []
    assert report.errors == ["RuntimeError: no profile"]


def test_commands_without_results_list_still_report():
    def invoke(args):  # `dbt debug` returns a bool as its result
        return SimpleNamespace(success=True, result=True, exception=None)

    report = tdbt.run_dbt("debug", invoke=invoke)
    assert report.ok
    assert report.nodes == []


def test_cli_writes_json_report_and_exit_code(monkeypatch, tmp_path, capsys):
    results = [_result("model.railfall.a", Resource.MODEL, Status.ERROR, message="no relation")]
    monkeypatch.setattr(tdbt, "_default_invoke", _fake(False, results))
    out = tmp_path / "dbt.json"
    code = tdbt.main(["build", "--select", "staging", "--report", str(out)])
    assert code == 1
    payload = json.loads(out.read_text())
    assert payload["ok"] is False
    assert payload["counts"] == {"error": 1}
    assert payload["nodes"][0]["status"] == "error"
    assert "FAILED" in capsys.readouterr().out


@pytest.mark.skipif(importlib.util.find_spec("dbt") is None, reason="dbt not installed")
def test_real_dbt_parse_in_process(monkeypatch, tmp_path):
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    monkeypatch.setenv("DBT_TARGET_PATH", str(tmp_path / "target"))
    monkeypatch.setenv("DBT_LOG_PATH", str(tmp_path / "logs"))
    report = tdbt.run_dbt("parse")
    assert report.ok, report.errors
    assert (tmp_path / "target" / "manifest.json").exists()
