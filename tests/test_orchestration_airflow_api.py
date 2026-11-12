"""The Airflow REST client, against a scripted fake of the `/api/v1` endpoints."""

from __future__ import annotations

import json

import pytest

from railfall.orchestration.airflow_api import AirflowClient, AirflowError, main, parse_xcom

DAG = "railfall_ingest"


class FakeAirflow:
    """Answers like Airflow 2.10: the DAG appears after `parse_after` polls, the run
    walks through `states`, and every call is recorded."""

    def __init__(self, *, parse_after=0, states=("queued", "running", "success"), errors=()):
        self.parse_after = parse_after
        self.states = list(states)
        self.errors = list(errors)
        self.calls: list[tuple[str, str, dict | None]] = []
        self.paused = True
        self.runs: dict[str, dict] = {}
        self.xcoms: dict[tuple[str, int], object] = {}

    def __call__(self, method, path, body):
        self.calls.append((method, path, body))
        if method == "GET" and path == f"/dags/{DAG}":
            if self.parse_after > 0:
                self.parse_after -= 1
                return 404, {"title": "DAG not found"}
            return 200, {"dag_id": DAG, "is_paused": self.paused}
        if method == "GET" and path.startswith("/importErrors"):
            return 200, {"import_errors": self.errors, "total_entries": len(self.errors)}
        if method == "PATCH" and path.startswith(f"/dags/{DAG}?update_mask=is_paused"):
            self.paused = body["is_paused"]
            return 200, {"dag_id": DAG, "is_paused": self.paused}
        if method == "POST" and path == f"/dags/{DAG}/dagRuns":
            if self.paused:
                return 409, {"detail": "paused"}
            if body["dag_run_id"] in self.runs:
                return 409, {"detail": "DAGRun already exists"}
            self.runs[body["dag_run_id"]] = body
            return 200, {"dag_run_id": body["dag_run_id"], "state": "queued"}
        if method == "GET" and "/xcomEntries/return_value" in path:
            task_id = path.split("/taskInstances/")[1].split("/")[0]
            map_index = int(path.split("map_index=")[1].split("&")[0])
            key = (task_id, map_index)
            if key not in self.xcoms:
                return 404, {"title": "XCom entry not found"}
            return 200, {"key": "return_value", "value": self.xcoms[key]}
        if method == "GET" and path.endswith("/taskInstances?limit=100"):
            return 200, {
                "task_instances": [
                    {
                        "task_id": "verify_sncf",
                        "map_index": -1,
                        "state": "success",
                        "try_number": 1,
                        "duration": 2.0,
                    },
                    {
                        "task_id": "ingest_meteo",
                        "map_index": 1,
                        "state": "success",
                        "try_number": 2,
                        "duration": 9.5,
                    },
                    {
                        "task_id": "ingest_meteo",
                        "map_index": 0,
                        "state": "success",
                        "try_number": 1,
                        "duration": 8.0,
                    },
                ]
            }
        if method == "GET" and "/dagRuns/" in path:
            run_id = path.rsplit("/", 1)[1]
            if run_id not in self.runs:
                return 404, {"title": "DAGRun not found"}
            state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
            return 200, {"dag_run_id": run_id, "state": state}
        return 404, {"title": f"unexpected {method} {path}"}


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, s):
        self.now += s


def client(fake, clock=None):
    clock = clock or Clock()
    return AirflowClient(fake, sleep=clock.sleep, clock=clock)


def test_run_waits_for_parse_unpauses_triggers_and_awaits_success():
    fake = FakeAirflow(parse_after=2)
    report = client(fake).run(DAG, conf={"ingest_date": "2026-11-12"}, run_id="e2e-1", poll_s=1)

    assert report.ok and report.state == "success"
    assert fake.runs["e2e-1"]["conf"] == {"ingest_date": "2026-11-12"}
    methods = [m for m, _, _ in fake.calls]
    # Unpause must come before the trigger, or the run would sit queued forever.
    assert methods.index("PATCH") < methods.index("POST")
    assert sum(1 for m, p, _ in fake.calls if m == "GET" and p == f"/dags/{DAG}") == 3


def test_task_instances_are_sorted_with_mapped_indexes():
    report = client(FakeAirflow()).run(DAG, run_id="r", poll_s=1)
    assert [(t.task_id, t.map_index) for t in report.tasks] == [
        ("ingest_meteo", 0),
        ("ingest_meteo", 1),
        ("verify_sncf", -1),
    ]
    assert report.tasks[1].try_number == 2


def test_failed_run_is_reported_not_raised():
    report = client(FakeAirflow(states=("running", "failed"))).run(DAG, run_id="r", poll_s=1)
    assert not report.ok and report.state == "failed"
    assert report.to_dict()["ok"] is False


def test_run_still_running_at_timeout_is_not_ok():
    clock = Clock()
    fake = FakeAirflow(states=("running",))
    report = client(fake, clock).run(DAG, run_id="r", timeout_s=30, poll_s=10)
    assert report.state == "running" and not report.ok
    assert clock.now >= 30


def test_import_error_fails_fast_instead_of_waiting():
    fake = FakeAirflow(
        parse_after=99,
        errors=[{"filename": f"/opt/airflow/dags/{DAG}.py", "stack_trace": "SyntaxError"}],
    )
    clock = Clock()
    with pytest.raises(AirflowError, match="SyntaxError"):
        client(fake, clock).wait_for_dag(DAG, timeout_s=300)
    assert clock.now == 0


def test_import_errors_of_other_files_are_ignored_until_timeout():
    fake = FakeAirflow(
        parse_after=99, errors=[{"filename": "/opt/airflow/dags/other.py", "stack_trace": "x"}]
    )
    with pytest.raises(AirflowError, match="not parsed within 20s"):
        client(fake).wait_for_dag(DAG, timeout_s=20, poll_s=5)


def test_api_errors_surface_status_and_detail():
    def refuse(method, path, body):
        return 401, {"title": "Unauthorized"}

    with pytest.raises(AirflowError, match="HTTP 401: Unauthorized"):
        client(refuse).run(DAG)


def test_cli_writes_report_and_exit_code(tmp_path, capsys):
    out = tmp_path / "dagrun.json"
    code = main(
        [
            DAG,
            "--conf",
            '{"ingest_date": "2026-11-12"}',
            "--run-id",
            "cli",
            "--poll",
            "1",
            "--report",
            str(out),
        ],
        client=client(FakeAirflow()),
    )
    assert code == 0
    report = json.loads(out.read_text())
    assert report["ok"] and report["dag_run_id"] == "cli" and len(report["tasks"]) == 3
    assert "[  ok] railfall_ingest cli: success" in capsys.readouterr().out

    code = main([DAG, "--poll", "1"], client=client(FakeAirflow(states=("failed",))))
    assert code == 1


def test_cli_rejects_non_object_conf():
    with pytest.raises(SystemExit):
        main([DAG, "--conf", "[1]"], client=client(FakeAirflow()))


def test_xcom_results_are_attached_to_successful_tasks():
    fake = FakeAirflow()
    fake.xcoms[("ingest_meteo", 0)] = [{"dataset": "daily_75", "status": "skipped"}]
    # Without deserialize support Airflow returns the Python repr of the value.
    fake.xcoms[("ingest_meteo", 1)] = "[{'dataset': 'daily_13', 'status': 'landed', 'error': None}]"
    c = client(fake)
    report = c.with_results(c.run(DAG, run_id="r", poll_s=1))

    results = {(t.task_id, t.map_index): t.result for t in report.tasks}
    assert results[("ingest_meteo", 0)] == [{"dataset": "daily_75", "status": "skipped"}]
    assert results[("ingest_meteo", 1)] == [
        {"dataset": "daily_13", "status": "landed", "error": None}
    ]
    assert results[("verify_sncf", -1)] is None  # pushed nothing -> 404 -> None
    assert report.to_dict()["tasks"][0]["result"][0]["status"] == "skipped"


@pytest.mark.parametrize(
    ("raw", "parsed"),
    [
        ('[{"a": 1}]', [{"a": 1}]),
        ("[{'a': None, 'b': True}]", [{"a": None, "b": True}]),
        ({"a": 1}, {"a": 1}),
        ("not a value", "not a value"),
    ],
)
def test_parse_xcom_accepts_json_repr_and_native(raw, parsed):
    assert parse_xcom(raw) == parsed


def test_main_with_xcom_writes_results_in_report(tmp_path):
    fake = FakeAirflow()
    fake.xcoms[("verify_sncf", -1)] = [{"dataset": "regularite", "ok": True}]
    out = tmp_path / "report.json"
    code = main(
        [DAG, "--run-id", "r", "--poll", "1", "--xcom", "--report", str(out)], client=client(fake)
    )
    assert code == 0
    tasks = json.loads(out.read_text())["tasks"]
    assert [t["result"] for t in tasks if t["task_id"] == "verify_sncf"] == [
        [{"dataset": "regularite", "ok": True}]
    ]
