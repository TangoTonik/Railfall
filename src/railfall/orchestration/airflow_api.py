"""Trigger a DAG through Airflow's stable REST API and wait for it to finish.

Used by `make dag-run` and the e2e workflow, so the proof that a DAG works is a
real run on the real scheduler, not a `dags test` in-process shortcut:

    python -m railfall.orchestration.airflow_api railfall_ingest \\
        --conf '{"ingest_date": "2026-11-12"}' --timeout 1200 --report dagrun.json

Steps: wait until the scheduler has parsed the DAG (failing fast on an import
error for its file), unpause it, create a run, poll until the run reaches a
terminal state, then report every task instance. Exit code 0 only on `success`.

Only the standard library is used: the client must run on the CI host, which
has no Airflow installed.
"""

from __future__ import annotations

import argparse
import ast
import base64
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime

from railfall.config import Settings

TERMINAL = frozenset({"success", "failed"})

Transport = Callable[[str, str, dict | None], tuple[int, dict]]


class AirflowError(RuntimeError):
    """The API refused a call, the DAG did not parse, or the run did not succeed in time."""


@dataclass(frozen=True)
class TaskReport:
    task_id: str
    map_index: int
    state: str | None
    try_number: int
    duration_s: float | None
    result: object = None  # the task's `return_value` XCom, when fetched with --xcom


@dataclass
class RunReport:
    dag_id: str
    dag_run_id: str
    state: str
    conf: dict
    duration_s: float
    tasks: list[TaskReport] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.state == "success"

    def to_dict(self) -> dict:
        return {**asdict(self), "ok": self.ok}


def http_transport(
    base_url: str, user: str, password: str, *, timeout_s: float = 30.0
) -> Transport:
    """Basic-auth JSON transport for `/api/v1`. Returns (status, body) for any HTTP status."""
    token = base64.b64encode(f"{user}:{password}".encode()).decode()

    def call(method: str, path: str, body: dict | None) -> tuple[int, dict]:
        req = urllib.request.Request(
            f"{base_url.rstrip('/')}/api/v1{path}",
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Authorization": f"Basic {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                payload = {"detail": raw.decode(errors="replace")[:500]}
            return exc.code, payload

    return call


def parse_xcom(value: object) -> object:
    """XCom values arrive native (deserialize support on) or as a string: JSON, or
    the Python repr Airflow falls back to. Unparseable strings are returned as-is."""
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        pass
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return value


def _http_error(method: str, path: str, status: int, payload: dict) -> AirflowError:
    detail = payload.get("detail") or payload.get("title") or payload
    return AirflowError(f"{method} {path} -> HTTP {status}: {detail}")


class AirflowClient:
    def __init__(
        self,
        transport: Transport,
        *,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._call = transport
        self._sleep = sleep
        self._clock = clock

    def _ok(self, method: str, path: str, body: dict | None = None) -> dict:
        status, payload = self._call(method, path, body)
        if status >= 400:
            raise _http_error(method, path, status, payload)
        return payload

    def import_errors(self, fileloc_suffix: str) -> list[str]:
        payload = self._ok("GET", "/importErrors?limit=100")
        return [
            e.get("stack_trace", "")
            for e in payload.get("import_errors", [])
            if e.get("filename", "").endswith(fileloc_suffix)
        ]

    def wait_for_dag(self, dag_id: str, *, timeout_s: float, poll_s: float = 5.0) -> dict:
        """Wait until the scheduler has serialised the DAG; fail fast on an import error."""
        deadline = self._clock() + timeout_s
        quoted = urllib.parse.quote(dag_id, safe="")
        while True:
            status, payload = self._call("GET", f"/dags/{quoted}", None)
            if status == 200:
                return payload
            if status != 404:
                raise _http_error("GET", f"/dags/{quoted}", status, payload)
            errors = self.import_errors(f"/{dag_id}.py")
            if errors:
                raise AirflowError(f"{dag_id} failed to import:\n{errors[0]}")
            if self._clock() >= deadline:
                raise AirflowError(f"{dag_id} was not parsed within {timeout_s:.0f}s")
            self._sleep(poll_s)

    def unpause(self, dag_id: str) -> None:
        quoted = urllib.parse.quote(dag_id, safe="")
        self._ok("PATCH", f"/dags/{quoted}?update_mask=is_paused", {"is_paused": False})

    def trigger(self, dag_id: str, *, conf: dict, run_id: str) -> dict:
        quoted = urllib.parse.quote(dag_id, safe="")
        return self._ok("POST", f"/dags/{quoted}/dagRuns", {"dag_run_id": run_id, "conf": conf})

    def wait_for_run(
        self, dag_id: str, run_id: str, *, timeout_s: float, poll_s: float = 10.0
    ) -> str:
        deadline = self._clock() + timeout_s
        path = (
            f"/dags/{urllib.parse.quote(dag_id, safe='')}"
            f"/dagRuns/{urllib.parse.quote(run_id, safe='')}"
        )
        while True:
            state = self._ok("GET", path).get("state")
            if state in TERMINAL:
                return state
            if self._clock() >= deadline:
                return state or "unknown"
            self._sleep(poll_s)

    def task_instances(self, dag_id: str, run_id: str) -> list[TaskReport]:
        path = (
            f"/dags/{urllib.parse.quote(dag_id, safe='')}"
            f"/dagRuns/{urllib.parse.quote(run_id, safe='')}/taskInstances?limit=100"
        )
        tasks = [
            TaskReport(
                task_id=t["task_id"],
                map_index=t.get("map_index", -1),
                state=t.get("state"),
                try_number=t.get("try_number", 0),
                duration_s=t.get("duration"),
            )
            for t in self._ok("GET", path).get("task_instances", [])
        ]
        return sorted(tasks, key=lambda t: (t.task_id, t.map_index))

    def xcom(self, dag_id: str, run_id: str, task_id: str, map_index: int = -1) -> object:
        """The task's `return_value` XCom, or None when it pushed nothing."""
        path = (
            f"/dags/{urllib.parse.quote(dag_id, safe='')}"
            f"/dagRuns/{urllib.parse.quote(run_id, safe='')}"
            f"/taskInstances/{urllib.parse.quote(task_id, safe='')}"
            f"/xcomEntries/return_value?map_index={map_index}&deserialize=true&stringify=false"
        )
        status, payload = self._call("GET", path, None)
        if status == 404:
            return None
        if status >= 400:
            raise _http_error("GET", path, status, payload)
        return parse_xcom(payload.get("value"))

    def with_results(self, report: RunReport) -> RunReport:
        """Attach each successful task's return value to the report."""
        report.tasks = [
            replace(t, result=self.xcom(report.dag_id, report.dag_run_id, t.task_id, t.map_index))
            if t.state == "success"
            else t
            for t in report.tasks
        ]
        return report

    def run(
        self,
        dag_id: str,
        *,
        conf: dict | None = None,
        run_id: str | None = None,
        timeout_s: float = 1200.0,
        parse_timeout_s: float = 180.0,
        poll_s: float = 10.0,
    ) -> RunReport:
        """Parse-wait, unpause, trigger, await. Never raises on a failed run: check `.ok`."""
        conf = conf or {}
        run_id = run_id or f"railfall__{datetime.now(UTC):%Y%m%dT%H%M%S}"
        self.wait_for_dag(dag_id, timeout_s=parse_timeout_s, poll_s=min(poll_s, 5.0))
        self.unpause(dag_id)
        started = self._clock()
        self.trigger(dag_id, conf=conf, run_id=run_id)
        state = self.wait_for_run(dag_id, run_id, timeout_s=timeout_s, poll_s=poll_s)
        return RunReport(
            dag_id=dag_id,
            dag_run_id=run_id,
            state=state,
            conf=conf,
            duration_s=round(self._clock() - started, 1),
            tasks=self.task_instances(dag_id, run_id),
        )


def _format(report: RunReport) -> str:
    lines = [
        f"[{'ok' if report.ok else 'FAIL':>4}] {report.dag_id} {report.dag_run_id}: "
        f"{report.state} in {report.duration_s:.0f}s"
    ]
    for t in report.tasks:
        name = t.task_id if t.map_index < 0 else f"{t.task_id}[{t.map_index}]"
        lines.append(f"       {name:<28} {t.state or '-':<16} try {t.try_number}")
    return "\n".join(lines)


def main(argv: list[str] | None = None, *, client: AirflowClient | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("dag_id")
    parser.add_argument("--conf", default="{}", help="JSON run conf (overrides DAG params)")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--timeout", type=float, default=1200.0, help="seconds to await the run")
    parser.add_argument("--parse-timeout", type=float, default=180.0)
    parser.add_argument("--poll", type=float, default=10.0)
    parser.add_argument("--report", default=None, help="also write the JSON report to this file")
    parser.add_argument("--json", action="store_true", help="print the JSON report")
    parser.add_argument(
        "--xcom", action="store_true", help="add each successful task's return value to the report"
    )
    args = parser.parse_args(argv)

    try:
        conf = json.loads(args.conf)
    except json.JSONDecodeError as exc:
        parser.error(f"--conf is not valid JSON: {exc}")
    if not isinstance(conf, dict):
        parser.error("--conf must be a JSON object")

    if client is None:
        s = Settings.from_env()
        client = AirflowClient(http_transport(s.airflow_url, s.airflow_user, s.airflow_password))
    try:
        report = client.run(
            args.dag_id,
            conf=conf,
            run_id=args.run_id,
            timeout_s=args.timeout,
            parse_timeout_s=args.parse_timeout,
            poll_s=args.poll,
        )
        if args.xcom:
            report = client.with_results(report)
    except (AirflowError, urllib.error.URLError) as exc:
        print(f"[FAIL] {args.dag_id}: {exc}", file=sys.stderr)
        return 1

    payload = json.dumps(report.to_dict(), indent=2)
    if args.report:
        with open(args.report, "w") as fh:
            fh.write(payload + "\n")
    print(payload if args.json else _format(report))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
