"""Anomaly summaries: one short message saying what is wrong with the pipeline.

Input is the JSON written by the two monitoring CLIs (`freshness --report` and
`runs --report`), so the summary can run after them without a database.

* The **digest** is rule-based and always produced: one bullet per anomaly
  (stale or empty table, failed load, abnormal staged volume), most severe
  first. It is deterministic and is what anything automated should rely on.
* The **LLM summary** is optional and off by default (`LLM_SUMMARY_ENABLED=1`).
  It asks a *local* model through the Ollama HTTP API to turn the digest into
  two or three plain sentences for a human on call. Only the anomaly facts are
  sent, never table contents, and nothing leaves the machine. When the model is
  unreachable, slow or answers nonsense, the digest is used instead: an
  optional nicety must never hide an alert.

When everything is healthy no model is called at all.

    python -m railfall.monitoring.summary --freshness freshness.json --runs runs.json
    python -m railfall.monitoring.summary --freshness freshness.json --llm --json
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from railfall.config import Settings

# Severity order: a failed or missing load outranks stale data, which outranks
# a volume that merely looks odd.
SEVERITY = {"failed": 0, "missing": 1, "empty": 2, "stale": 3, "volume": 4}

# Longer than this and the model is rambling; fall back to the digest.
MAX_LLM_CHARS = 1200

SYSTEM_PROMPT = (
    "You are the on-call assistant of a small data pipeline that loads French rail "
    "punctuality (SNCF) and weather (Météo-France) open data into a warehouse. "
    "Summarise the anomalies below for an engineer in two or three short sentences: "
    "what is broken, which tables are affected, and what to look at first. "
    "Use only the facts given. Do not invent causes, numbers or tables. "
    "No greeting, no markdown."
)

LlmOpener = Callable[[urllib.request.Request, float], Any]


@dataclass(frozen=True, order=True)
class Anomaly:
    """One thing wrong, as found by a monitoring check."""

    severity: int
    kind: str
    table: str
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "table": self.table, "detail": self.detail}

    def line(self) -> str:
        return f"[{self.kind}] {self.table}: {self.detail}"


@dataclass(frozen=True)
class Summary:
    anomalies: tuple[Anomaly, ...]
    digest: str
    text: str
    source: str  # "rules" or "llm"
    llm_error: str | None = None

    @property
    def healthy(self) -> bool:
        return not self.anomalies

    def as_dict(self) -> dict[str, Any]:
        return {
            "healthy": self.healthy,
            "source": self.source,
            "text": self.text,
            "digest": self.digest,
            "llm_error": self.llm_error,
            "anomalies": [a.as_dict() for a in self.anomalies],
        }


# --------------------------------------------------------------------------- anomalies


def _anomaly(kind: str, table: str, detail: str) -> Anomaly:
    return Anomaly(SEVERITY[kind], kind, table, detail)


def freshness_anomalies(results: Sequence[Mapping[str, Any]]) -> list[Anomaly]:
    """From `freshness --report` (a list of per-table results)."""
    found = []
    for r in results:
        status = r.get("status")
        if status == "ok":
            continue
        kind = status if status in SEVERITY else "stale"
        reasons = r.get("reasons") or [f"status {status}"]
        found.append(_anomaly(kind, str(r.get("table")), "; ".join(map(str, reasons))))
    return found


def run_anomalies(report: Mapping[str, Any]) -> list[Anomaly]:
    """From `runs --report`: failed tables in the latest run, and volume anomalies."""
    found = []
    runs = report.get("runs") or []
    if runs:
        latest = runs[0]
        for table in latest.get("failed") or []:
            found.append(
                _anomaly("failed", str(table), f"load failed in run {latest.get('run_id')}")
            )
    for v in report.get("volumes") or []:
        if not v.get("ok", True):
            found.append(_anomaly("volume", str(v.get("table")), "; ".join(v.get("reasons", []))))
    return found


def collect(
    freshness: Sequence[Mapping[str, Any]] | None = None,
    runs: Mapping[str, Any] | None = None,
) -> list[Anomaly]:
    """Every anomaly, most severe first, deduplicated."""
    found = freshness_anomalies(freshness or []) + run_anomalies(runs or {})
    return sorted(set(found))


def digest(anomalies: Sequence[Anomaly]) -> str:
    if not anomalies:
        return "All monitoring checks passed."
    tables = sorted({a.table for a in anomalies})
    head = f"{len(anomalies)} anomal{'y' if len(anomalies) == 1 else 'ies'} on {len(tables)} table"
    head += "s" if len(tables) != 1 else ""
    return "\n".join([head + ":", *(f"- {a.line()}" for a in anomalies)])


# --------------------------------------------------------------------------- LLM


class LlmError(RuntimeError):
    """The local model could not produce a usable summary."""


def _default_opener(request: urllib.request.Request, timeout: float) -> Any:
    return urllib.request.urlopen(request, timeout=timeout)  # noqa: S310 - local http URL


def build_prompt(anomalies: Sequence[Anomaly]) -> str:
    facts = json.dumps([a.as_dict() for a in anomalies], ensure_ascii=False, indent=1)
    return f"{SYSTEM_PROMPT}\n\nAnomalies (JSON):\n{facts}\n\nSummary:"


def ask_llm(prompt: str, settings: Settings, *, opener: LlmOpener = _default_opener) -> str:
    """One non-streaming call to Ollama's `/api/generate`, at temperature 0."""
    body = {
        "model": settings.llm_model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0},
    }
    request = urllib.request.Request(
        f"{settings.llm_url}/api/generate",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with opener(request, settings.llm_timeout_s) as resp:
            payload = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise LlmError(f"{settings.llm_url} unreachable: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise LlmError("model answered with invalid JSON") from exc
    text = str(payload.get("response", "")).strip() if isinstance(payload, dict) else ""
    if not text:
        raise LlmError("model returned an empty answer")
    if len(text) > MAX_LLM_CHARS:
        raise LlmError(f"model answer too long ({len(text)} chars)")
    return text


def summarize(
    anomalies: Sequence[Anomaly],
    settings: Settings | None = None,
    *,
    use_llm: bool | None = None,
    opener: LlmOpener = _default_opener,
) -> Summary:
    """Digest always; LLM wording only when enabled, needed, and working."""
    settings = settings or Settings.from_env()
    use_llm = settings.llm_summary_enabled if use_llm is None else use_llm
    anomalies = tuple(sorted(set(anomalies)))
    text = digest(anomalies)
    if not anomalies or not use_llm:
        return Summary(anomalies, text, text, "rules")
    try:
        answer = ask_llm(build_prompt(anomalies), settings, opener=opener)
    except LlmError as exc:
        return Summary(anomalies, text, text, "rules", llm_error=str(exc))
    return Summary(anomalies, text, answer, "llm")


# --------------------------------------------------------------------------- CLI


def _load(path: Path | None) -> Any:
    return None if path is None else json.loads(path.read_text())


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--freshness", type=Path, help="JSON from `freshness --report`")
    parser.add_argument("--runs", type=Path, help="JSON from `runs --report`")
    llm = parser.add_mutually_exclusive_group()
    llm.add_argument("--llm", dest="llm", action="store_true", default=None)
    llm.add_argument("--no-llm", dest="llm", action="store_false")
    parser.add_argument("--json", action="store_true", help="print the summary as JSON")
    parser.add_argument("--report", type=Path, default=None, help="write the summary as JSON")
    args = parser.parse_args(argv)
    if args.freshness is None and args.runs is None:
        parser.error("give --freshness and/or --runs")

    anomalies = collect(_load(args.freshness), _load(args.runs))
    summary = summarize(anomalies, Settings.from_env(env), use_llm=args.llm)
    payload = json.dumps(summary.as_dict(), indent=2, ensure_ascii=False)
    if args.report:
        args.report.write_text(payload + "\n")
    if summary.llm_error:
        print(f"LLM summary unavailable, using the digest: {summary.llm_error}", file=sys.stderr)
    print(payload if args.json else summary.text)
    return 0 if summary.healthy else 1


if __name__ == "__main__":
    sys.exit(main())
