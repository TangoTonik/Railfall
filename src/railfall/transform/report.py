"""Read the weather-impact marts back as a short Markdown report.

    python -m railfall.transform.report                      # Markdown on stdout
    python -m railfall.transform.report --out weather.md --json --report weather.json

The marts answer "does bad weather cost punctuality?" in three cuts; this turns
them into something a person reads in a minute:

* one headline per factor (rain, wind, frost): adverse vs normal punctuality,
  from `mart_punctuality_by_weather`;
* the dose-response per factor, band by band, from
  `mart_punctuality_by_weather_intensity`;
* the region x season cells where the factor hurts most, from
  `mart_punctuality_by_region_season`, keeping only cells with both exposures
  and enough route-months that one odd month cannot top the list.

It connects as `railfall_reader`, the role dashboards use, so a run also proves
the reader can see every mart dbt built. It only reads; the statistics (is a
gap significant?) are Day 17's job. Exit code 1 when a mart is empty.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import quote

MARTS_SCHEMA = "marts"
FACTORS = ("rain", "wind", "frost")
DEFAULT_TOP = 5
DEFAULT_MIN_ROUTE_MONTHS = 30

BINARY_SQL = """
    select weather_factor, exposure, route_months, trains_run,
           arrival_punctuality_rate, cancellation_rate, avg_delay_arrival_min,
           punctuality_gap_pts, delay_gap_min
    from marts.mart_punctuality_by_weather
    order by weather_factor, exposure
"""

INTENSITY_SQL = """
    select weather_factor, intensity_band, band_order, is_baseline, route_months,
           arrival_punctuality_rate, avg_delay_arrival_min, punctuality_gap_pts
    from marts.mart_punctuality_by_weather_intensity
    order by weather_factor, band_order
"""

# Worst cells first; ties broken on the name so the report is deterministic.
REGIONAL_SQL = """
    select region_name, season, weather_factor, route_months, years,
           arrival_punctuality_rate, punctuality_gap_pts
    from marts.mart_punctuality_by_region_season
    where exposure = 'adverse'
      and has_comparison
      and punctuality_gap_pts is not null
      and route_months >= %s
    order by punctuality_gap_pts, region_name, season, weather_factor
"""


@dataclass
class WeatherReport:
    binary: list[dict[str, Any]] = field(default_factory=list)
    intensity: list[dict[str, Any]] = field(default_factory=list)
    regional: list[dict[str, Any]] = field(default_factory=list)
    min_route_months: int = DEFAULT_MIN_ROUTE_MONTHS
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["ok"] = self.ok
        out["headlines"] = headlines(self.binary)
        return out


def reader_dsn(env: Mapping[str, str] | None = None) -> str:
    """libpq URL for the read-only role, on the same host and database as dbt."""
    env = os.environ if env is None else env
    host = env.get("DBT_HOST", "localhost")
    port = env.get("DBT_PORT") or env.get("POSTGRES_PORT") or "5432"
    user = env.get("READER_USER", "railfall_reader")
    password = env.get("READER_PASSWORD") or env.get(
        "TF_VAR_warehouse_reader_password", "reader-local"
    )
    dbname = env.get("DBT_DBNAME") or env.get("POSTGRES_DB") or "warehouse"
    return f"postgresql://{quote(user)}:{quote(password, safe='')}@{host}:{port}/{dbname}"


def _plain(value: Any) -> Any:
    """Decimals as floats, so rows serialise to JSON and format uniformly."""
    return float(value) if isinstance(value, Decimal) else value


def _rows(conn: Any, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
    cur = conn.execute(query, params)
    names = [d[0] for d in cur.description]
    return [{n: _plain(v) for n, v in zip(names, row, strict=True)} for row in cur.fetchall()]


def collect(
    conn: Any, *, min_route_months: int = DEFAULT_MIN_ROUTE_MONTHS, top: int = DEFAULT_TOP
) -> WeatherReport:
    report = WeatherReport(min_route_months=min_route_months)
    report.binary = _rows(conn, BINARY_SQL)
    report.intensity = _rows(conn, INTENSITY_SQL)
    report.regional = worst_per_factor(_rows(conn, REGIONAL_SQL, (min_route_months,)), top)
    for name, rows in (
        ("mart_punctuality_by_weather", report.binary),
        ("mart_punctuality_by_weather_intensity", report.intensity),
    ):
        if not rows:
            report.errors.append(f"{MARTS_SCHEMA}.{name} is empty")
    missing = sorted(set(FACTORS) - {r["weather_factor"] for r in report.binary})
    if report.binary and missing:
        report.errors.append(f"no weather comparison for {', '.join(missing)}")
    return report


def worst_per_factor(rows: Sequence[Mapping[str, Any]], top: int) -> list[dict[str, Any]]:
    """The `top` most negative gaps of each factor, input order kept (worst first)."""
    kept: dict[str, int] = {}
    out = []
    for row in rows:
        factor = row["weather_factor"]
        if kept.get(factor, 0) < top:
            kept[factor] = kept.get(factor, 0) + 1
            out.append(dict(row))
    return out


def _pct(rate: float | None) -> str:
    return "n/a" if rate is None else f"{100 * rate:.1f}%"


def _pts(gap: float | None) -> str:
    return "n/a" if gap is None else f"{gap:+.1f} pts"


def _minutes(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f} min"


def headlines(binary: Sequence[Mapping[str, Any]]) -> list[str]:
    """One sentence per factor present in the binary mart, in FACTORS order."""
    by_key = {(r["weather_factor"], r["exposure"]): r for r in binary}
    lines = []
    for factor in FACTORS:
        adverse = by_key.get((factor, "adverse"))
        normal = by_key.get((factor, "normal"))
        if adverse is None or normal is None:
            if adverse or normal:
                lines.append(
                    f"{factor.capitalize()}: no {'normal' if adverse else 'adverse'} "
                    "route-months to compare with."
                )
            continue
        lines.append(
            f"{factor.capitalize()}: {_pct(adverse['arrival_punctuality_rate'])} on time when it "
            f"hits vs {_pct(normal['arrival_punctuality_rate'])} otherwise "
            f"({_pts(adverse['punctuality_gap_pts'])}, "
            f"{adverse['route_months']:,} vs {normal['route_months']:,} route-months)."
        )
    return lines


def _table(header: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return lines


def render_markdown(report: WeatherReport) -> str:
    out = ["# Weather and TGV punctuality", ""]
    lines = headlines(report.binary)
    out += [f"- {line}" for line in lines] if lines else ["_No weather comparison yet._"]

    out += ["", "## Does it get worse the harder it hits?", ""]
    for factor in FACTORS:
        rows = [r for r in report.intensity if r["weather_factor"] == factor]
        if not rows:
            continue
        out += [f"**{factor.capitalize()}**", ""]
        out += _table(
            ("band", "route-months", "on time", "avg delay", "vs baseline"),
            [
                (
                    r["intensity_band"] + (" (baseline)" if r["is_baseline"] else ""),
                    f"{r['route_months']:,}",
                    _pct(r["arrival_punctuality_rate"]),
                    _minutes(r["avg_delay_arrival_min"]),
                    "—" if r["is_baseline"] else _pts(r["punctuality_gap_pts"]),
                )
                for r in rows
            ],
        )
        out.append("")

    out += [
        "## Where and when it hurts most",
        "",
        f"Region (departure end) x season cells with both exposures and at least "
        f"{report.min_route_months} adverse route-months, worst gap first.",
        "",
    ]
    if report.regional:
        out += _table(
            ("factor", "region", "season", "route-months", "on time", "vs normal"),
            [
                (
                    r["weather_factor"],
                    r["region_name"],
                    r["season"],
                    f"{r['route_months']:,}",
                    _pct(r["arrival_punctuality_rate"]),
                    _pts(r["punctuality_gap_pts"]),
                )
                for r in report.regional
            ],
        )
    else:
        out.append("_No region-season cell has enough route-months yet._")
    if report.errors:
        out += ["", "## Problems", ""] + [f"- {e}" for e in report.errors]
    return "\n".join(out) + "\n"


def _connect(dsn: str) -> Any:
    import psycopg

    return psycopg.connect(dsn, autocommit=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="railfall.transform.report", description=__doc__)
    parser.add_argument("--out", type=Path, help="write the Markdown here instead of stdout")
    parser.add_argument("--report", type=Path, help="also write the rows as JSON here")
    parser.add_argument("--json", action="store_true", help="print JSON instead of Markdown")
    parser.add_argument("--top", type=int, default=DEFAULT_TOP, help="cells per factor")
    parser.add_argument(
        "--min-route-months",
        type=int,
        default=DEFAULT_MIN_ROUTE_MONTHS,
        help="smallest adverse cell listed in the regional table",
    )
    args = parser.parse_args(argv)

    with _connect(reader_dsn()) as conn:
        report = collect(conn, min_route_months=args.min_route_months, top=args.top)

    markdown = render_markdown(report)
    if args.report:
        args.report.write_text(json.dumps(report.to_dict(), indent=2, default=str) + "\n")
    if args.out:
        args.out.write_text(markdown)
    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    elif not args.out:
        print(markdown, end="")
    for error in report.errors:
        print(f"error: {error}", file=sys.stderr)
    return 0 if report.ok else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
