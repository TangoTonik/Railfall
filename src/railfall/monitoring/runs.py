"""Run metrics: what did the recent loads do, and does the last one look normal?

Everything comes from the load audit log (`raw._load_runs`, one row per run and
table). Two views:

* **Run summaries** — one line per run: when it started, how long it took, how
  many tables it touched or failed, and the row totals (staged, inserted,
  updated, unchanged, deleted). This is the "what happened last night" answer.
* **Volume checks** — per table, the rows staged by its latest successful load
  against the median of its previous successful loads *with the same scope*.
  A full reload and a `--years 2025` reload stage very different volumes; only
  like is compared with like. A collapse (an upstream file that came back half
  empty) or a jump (duplicated partitions) is flagged; a short history is not
  judged at all.

A run is unhealthy when its latest run has a failed table or a volume anomaly.
Like the freshness checks this is plain SQL over DB-API, unit-tested on SQLite.

    python -m railfall.monitoring.runs                  # last 10 runs + checks
    python -m railfall.monitoring.runs --limit 30 --json
    python -m railfall.monitoring.runs --report runs.json
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from railfall.load.audit import LOG_TABLE
from railfall.monitoring.freshness import RAW_SCHEMA, Connection, as_datetime, qualified

COUNTS = ("staged", "inserted", "updated", "unchanged", "deleted")

# Fewer previous loads than this and the baseline is too noisy to judge.
MIN_HISTORY = 3
# Latest staged volume outside [baseline * LOW, baseline * HIGH] is an anomaly.
DEFAULT_LOW = 0.5
DEFAULT_HIGH = 2.0


@dataclass(frozen=True)
class RunSummary:
    """One load run, aggregated over its tables."""

    run_id: str
    started_at: datetime
    finished_at: datetime
    tables: int
    failed: tuple[str, ...]
    totals: Mapping[str, int]

    @property
    def ok(self) -> bool:
        return not self.failed

    @property
    def duration_s(self) -> float:
        return round((self.finished_at - self.started_at).total_seconds(), 1)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_s": self.duration_s,
            "tables": self.tables,
            "ok": self.ok,
            "failed": list(self.failed),
            **{k: self.totals[k] for k in COUNTS},
        }


@dataclass(frozen=True)
class VolumeCheck:
    """Latest successful staged volume of one table against its history."""

    table: str
    scope: str
    latest: int
    baseline: float | None
    history: int
    ratio: float | None = None
    reasons: tuple[str, ...] = field(default=())

    @property
    def ok(self) -> bool:
        return not self.reasons

    def as_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "scope": self.scope,
            "latest": self.latest,
            "baseline": self.baseline,
            "history": self.history,
            "ratio": self.ratio,
            "ok": self.ok,
            "reasons": list(self.reasons),
        }


# --------------------------------------------------------------------------- SQL


def runs_sql(schema: str | None, placeholder: str = "%s") -> str:
    """Per-run aggregates, newest first (failed tables are listed separately)."""
    sums = ", ".join(f"sum({c})" for c in COUNTS)
    return (
        f"select cast(run_id as text), min(started_at), max(finished_at), count(*), {sums}"
        f" from {qualified(LOG_TABLE, schema)}"
        f" group by run_id order by min(started_at) desc limit {placeholder}"
    )


def failed_sql(schema: str | None) -> str:
    return (
        f"select cast(run_id as text), table_name from {qualified(LOG_TABLE, schema)}"
        " where not ok order by table_name"
    )


def history_sql(schema: str | None) -> str:
    """Every successful load, newest first per table."""
    return (
        f"select table_name, scope, staged, finished_at from {qualified(LOG_TABLE, schema)}"
        " where ok order by table_name, finished_at desc"
    )


# --------------------------------------------------------------------------- queries


def _rollback(conn: Any) -> None:
    rollback = getattr(conn, "rollback", None)
    if rollback is not None:
        rollback()


def recent_runs(
    conn: Connection, limit: int = 10, *, schema: str | None = RAW_SCHEMA, placeholder: str = "%s"
) -> list[RunSummary]:
    """The `limit` most recent runs, newest first."""
    if limit < 1:
        raise ValueError("limit must be >= 1")
    try:
        rows = conn.execute(runs_sql(schema, placeholder), (limit,)).fetchall()
    except Exception:  # no audit log yet: nothing was ever loaded
        _rollback(conn)
        return []
    wanted = {r[0] for r in rows}
    failed: dict[str, list[str]] = {}
    for run_id, table in conn.execute(failed_sql(schema)).fetchall():
        if run_id in wanted:
            failed.setdefault(run_id, []).append(table)
    return [
        RunSummary(
            run_id=run_id,
            started_at=as_datetime(started),
            finished_at=as_datetime(finished),
            tables=int(tables),
            failed=tuple(failed.get(run_id, ())),
            totals={k: int(v or 0) for k, v in zip(COUNTS, sums, strict=True)},
        )
        for run_id, started, finished, tables, *sums in rows
    ]


def judge_volume(
    table: str,
    scope: str,
    latest: int,
    previous: Sequence[int],
    *,
    low: float = DEFAULT_LOW,
    high: float = DEFAULT_HIGH,
    min_history: int = MIN_HISTORY,
) -> VolumeCheck:
    """Compare `latest` with the median of `previous` (pure: no database)."""
    if not 0 < low < 1 < high:
        raise ValueError("expected 0 < low < 1 < high")
    if len(previous) < min_history:
        return VolumeCheck(table, scope, latest, None, len(previous))
    baseline = float(statistics.median(previous))
    reasons: list[str] = []
    if baseline == 0:
        ratio = None
        if latest > 0:
            reasons.append(f"staged {latest} rows where previous loads staged none")
    else:
        ratio = round(latest / baseline, 3)
        if ratio < low:
            reasons.append(f"staged {latest} rows, {ratio:.0%} of the usual {baseline:g}")
        elif ratio > high:
            reasons.append(f"staged {latest} rows, {ratio:.1f}x the usual {baseline:g}")
    return VolumeCheck(table, scope, latest, baseline, len(previous), ratio, tuple(reasons))


def volume_checks(
    conn: Connection,
    *,
    window: int = 10,
    schema: str | None = RAW_SCHEMA,
    **thresholds: float,
) -> list[VolumeCheck]:
    """Per table: latest successful load vs up to `window` earlier ones of the same scope."""
    try:
        rows = conn.execute(history_sql(schema)).fetchall()
    except Exception:
        _rollback(conn)
        return []
    latest: dict[str, tuple[str, int]] = {}
    previous: dict[str, list[int]] = {}
    for table, scope, staged, _finished in rows:  # newest first per table
        if table not in latest:
            latest[table] = (scope, int(staged))
            previous[table] = []
        elif scope == latest[table][0] and len(previous[table]) < window:
            previous[table].append(int(staged))
    return [
        judge_volume(t, scope, staged, previous[t], **thresholds)
        for t, (scope, staged) in sorted(latest.items())
    ]


# --------------------------------------------------------------------------- report


def healthy(runs: Sequence[RunSummary], volumes: Sequence[VolumeCheck]) -> bool:
    """Latest run fully succeeded and no table's volume is anomalous."""
    return bool(runs) and runs[0].ok and all(v.ok for v in volumes)


def report(runs: Sequence[RunSummary], volumes: Sequence[VolumeCheck]) -> dict[str, Any]:
    return {
        "healthy": healthy(runs, volumes),
        "runs": [r.as_dict() for r in runs],
        "volumes": [v.as_dict() for v in volumes],
    }


def format_report(runs: Sequence[RunSummary], volumes: Sequence[VolumeCheck]) -> str:
    if not runs:
        return "no load runs recorded"
    lines = ["Recent load runs (newest first):"]
    for r in runs:
        status = "ok" if r.ok else f"FAILED: {', '.join(r.failed)}"
        t = r.totals
        lines.append(
            f"  {r.started_at:%Y-%m-%d %H:%M} {r.duration_s:>7g} s  {r.tables} tables"
            f"  +{t['inserted']} ~{t['updated']} -{t['deleted']} ={t['unchanged']}  {status}"
        )
    lines.append("Staged volume of the latest successful load per table:")
    for v in volumes:
        if v.baseline is None:
            verdict = f"n/a (only {v.history} earlier {v.scope} loads)"
        else:
            verdict = "ok" if v.ok else "ANOMALY: " + "; ".join(v.reasons)
        lines.append(f"  {v.table:<24} {v.scope:<12} {v.latest:>9}  {verdict}")
    lines.append("healthy" if healthy(runs, volumes) else "UNHEALTHY")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    import psycopg

    from railfall.transform.verify import transformer_dsn

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--limit", type=int, default=10, help="how many runs to list")
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    parser.add_argument("--report", type=Path, default=None, help="write the report as JSON")
    args = parser.parse_args(argv)

    with psycopg.connect(transformer_dsn(env), connect_timeout=10) as conn:
        runs = recent_runs(conn, args.limit)
        volumes = volume_checks(conn)
    payload = json.dumps(report(runs, volumes), indent=2)
    if args.report:
        args.report.write_text(payload + "\n")
    print(payload if args.json else format_report(runs, volumes))
    return 0 if healthy(runs, volumes) else 1


if __name__ == "__main__":
    sys.exit(main())
