"""Run dbt in-process and turn its results into a small, typed report.

    python -m railfall.transform.dbt build --select staging --json --report out.json

dbt is driven through `dbtRunner` (its programmatic API) rather than a
subprocess, so callers (the CLI, Airflow tasks, e2e) get per-node statuses
without parsing log lines. The project and profile directories default to the
repo's `dbt/` folder and can be moved with `DBT_PROJECT_DIR` / `DBT_PROFILES_DIR`
(the Airflow image mounts the project elsewhere).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_PROJECT_DIR = Path(__file__).resolve().parents[3] / "dbt"
COMMANDS = ("build", "run", "test", "parse", "debug", "compile", "source freshness")

# Node statuses dbt reports, per its RunStatus / TestStatus / FreshnessStatus enums.
OK_STATUSES = frozenset({"success", "pass", "warn"})
FAILED_STATUSES = frozenset({"error", "fail", "runtime error", "skipped"})


@dataclass(frozen=True)
class NodeResult:
    unique_id: str
    resource_type: str
    status: str
    seconds: float = 0.0
    message: str | None = None
    failures: int | None = None

    @property
    def ok(self) -> bool:
        return self.status in OK_STATUSES


@dataclass
class DbtReport:
    command: str
    ok: bool
    nodes: list[NodeResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def counts(self) -> dict[str, int]:
        return dict(sorted(Counter(n.status for n in self.nodes).items()))

    def by_type(self, resource_type: str) -> list[NodeResult]:
        return [n for n in self.nodes if n.resource_type == resource_type]

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["counts"] = self.counts
        return out


def project_dir(env: Mapping[str, str] | None = None) -> Path:
    env = os.environ if env is None else env
    return Path(env.get("DBT_PROJECT_DIR") or DEFAULT_PROJECT_DIR)


def profiles_dir(env: Mapping[str, str] | None = None) -> Path:
    env = os.environ if env is None else env
    return Path(env.get("DBT_PROFILES_DIR") or project_dir(env))


def _status(raw: Any) -> str:
    # dbt statuses are str enums; `.value` keeps "runtime error" instead of the enum repr.
    return str(getattr(raw, "value", raw)).lower()


def node_result(result: Any) -> NodeResult:
    """Map one dbt `RunResult` (or freshness result) to a NodeResult."""
    node = result.node
    resource_type = _status(getattr(node, "resource_type", "unknown"))
    return NodeResult(
        unique_id=node.unique_id,
        resource_type=resource_type,
        status=_status(result.status),
        seconds=round(float(getattr(result, "execution_time", 0.0) or 0.0), 3),
        message=getattr(result, "message", None),
        failures=getattr(result, "failures", None),
    )


def report_from(command: str, success: bool, results: Iterable[Any] | None, exc: Any) -> DbtReport:
    nodes = [node_result(r) for r in results or [] if getattr(r, "node", None) is not None]
    errors: list[str] = []
    if exc is not None:
        errors.append(f"{type(exc).__name__}: {exc}")
    errors += [f"{n.unique_id}: {n.status} ({n.message})" for n in nodes if not n.ok]
    return DbtReport(command=command, ok=bool(success) and not errors, nodes=nodes, errors=errors)


def dbt_args(
    command: str,
    *,
    select: Sequence[str] = (),
    exclude: Sequence[str] = (),
    target: str | None = None,
    project: Path | None = None,
    profiles: Path | None = None,
) -> list[str]:
    if command not in COMMANDS:
        raise ValueError(f"unsupported dbt command {command!r}; expected one of {COMMANDS}")
    args = command.split()
    args += ["--project-dir", str(project or project_dir())]
    args += ["--profiles-dir", str(profiles or profiles_dir())]
    if target:
        args += ["--target", target]
    if select:
        args += ["--select", *select]
    if exclude:
        args += ["--exclude", *exclude]
    return args


def _default_invoke(args: list[str]) -> Any:
    from dbt.cli.main import dbtRunner

    return dbtRunner().invoke(args)


def run_dbt(
    command: str,
    *,
    select: Sequence[str] = (),
    exclude: Sequence[str] = (),
    target: str | None = None,
    invoke: Callable[[list[str]], Any] | None = None,
) -> DbtReport:
    """Invoke dbt and report every executed node; `ok` is False on any failure."""
    args = dbt_args(command, select=select, exclude=exclude, target=target)
    res = (invoke or _default_invoke)(args)
    result = getattr(res, "result", None)
    # `debug` and `parse` return a bool / manifest instead of a results list.
    results = getattr(result, "results", None)
    return report_from(command, res.success, results, res.exception)


def _format(report: DbtReport) -> str:
    lines = [
        f"[{'  ok' if n.ok else 'FAIL'}] {n.resource_type:<8} {n.status:<13} "
        f"{n.seconds:>6.2f}s  {n.unique_id}"
        for n in report.nodes
    ]
    summary = ", ".join(f"{k}={v}" for k, v in report.counts.items()) or "no nodes"
    lines.append(f"dbt {report.command}: {'OK' if report.ok else 'FAILED'} ({summary})")
    lines += [f"  error: {e}" for e in report.errors]
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run dbt and summarise the result per node.")
    parser.add_argument("command", nargs="+", help=f"one of: {', '.join(COMMANDS)}")
    parser.add_argument("--select", "-s", nargs="+", default=[])
    parser.add_argument("--exclude", nargs="+", default=[])
    parser.add_argument("--target", default=None)
    parser.add_argument("--json", action="store_true", help="print a JSON report")
    parser.add_argument("--report", type=Path, default=None, help="also write the JSON report")
    args = parser.parse_args(argv)

    command = " ".join(args.command)
    try:
        report = run_dbt(command, select=args.select, exclude=args.exclude, target=args.target)
    except ValueError as exc:
        parser.error(str(exc))

    payload = json.dumps(report.to_dict(), indent=2, default=str)
    if args.report:
        args.report.write_text(payload + "\n")
    print(payload if args.json else _format(report))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
