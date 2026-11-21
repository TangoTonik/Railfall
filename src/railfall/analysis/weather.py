"""Answer "does bad weather significantly lower TGV punctuality?" from the marts.

    python -m railfall.analysis.weather                       # Markdown on stdout
    python -m railfall.analysis.weather --out stats.md --report stats.json

Reads the route-month panel (`railfall.analysis.panel`), then for each factor
(rain, wind, frost) runs the stratified test from `railfall.analysis.stats`:
naive pooled gap (the dashboard number), stratified gap inside route x service x
season, a within-stratum permutation p-value and a route-cluster bootstrap CI.
The three p-values are adjusted together with Benjamini-Hochberg, and the
verdict for a factor reads off the adjusted p-value and the CI:

* **lowers punctuality**: adjusted p < alpha and the whole CI below zero;
* **raises punctuality**: adjusted p < alpha and the whole CI above zero
  (unexpected, worth a look, but reported as found);
* **no significant effect**: anything else, including a test with too few
  strata to say anything.

A season-by-season table follows (`railfall.analysis.seasons`), so a
winter-only effect is not hidden by the pooled number; `--no-seasons` skips it.

It connects as `railfall_reader`, like the marts report. Exit code 1 when the
panel is empty or no factor could be tested at all, so CI fails loudly instead
of publishing an empty answer.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

from railfall.analysis.panel import FACTORS, Panel, read_panel
from railfall.analysis.seasons import DEFAULT_MIN_STRATA, SeasonCell, season_breakdown
from railfall.analysis.stats import (
    DEFAULT_BOOTSTRAP,
    DEFAULT_CONFIDENCE,
    DEFAULT_PERMUTATIONS,
    FactorTest,
    analyse_factor,
    benjamini_hochberg,
)

DEFAULT_ALPHA = 0.05
DEFAULT_SEED = 17

LOWERS = "lowers punctuality"
RAISES = "raises punctuality"
NO_EFFECT = "no significant effect"
UNTESTABLE = "not testable"


def verdict(test: FactorTest, alpha: float = DEFAULT_ALPHA) -> str:
    """Plain-words reading of one factor's test (see module docstring)."""
    if test.strata_used == 0 or math.isnan(test.gap):
        return UNTESTABLE
    p = test.p_adjusted if test.p_adjusted is not None else test.p_value
    if math.isnan(p) or p >= alpha:
        return NO_EFFECT
    if test.ci_high < 0:
        return LOWERS
    if test.ci_low > 0:
        return RAISES
    return NO_EFFECT


@dataclass
class WeatherAnalysis:
    route_months: int = 0
    routes: int = 0
    months: int = 0
    alpha: float = DEFAULT_ALPHA
    confidence: float = DEFAULT_CONFIDENCE
    seed: int | None = DEFAULT_SEED
    tests: list[FactorTest] = field(default_factory=list)
    seasons: list[SeasonCell] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def verdicts(self) -> dict[str, str]:
        return {t.factor: verdict(t, self.alpha) for t in self.tests}

    def to_dict(self) -> dict[str, Any]:
        def clean(value: Any) -> Any:
            # JSON has no NaN; an untestable factor serialises as null.
            return None if isinstance(value, float) and math.isnan(value) else value

        tests = []
        for test in self.tests:
            row = {k: clean(v) for k, v in asdict(test).items()}
            row["significant"] = test.significant
            row["verdict"] = verdict(test, self.alpha)
            tests.append(row)
        by_season = []
        for cell in self.seasons:
            row: dict[str, Any] = {
                "season": cell.season,
                "factor": cell.factor,
                "route_months": cell.route_months,
                "strata_available": cell.strata_available,
                "tested": cell.tested,
            }
            if cell.test is not None:
                for key in ("gap", "ci_low", "ci_high", "p_value", "p_adjusted"):
                    row[key] = clean(getattr(cell.test, key))
                row["verdict"] = verdict(cell.test, self.alpha)
            else:
                row["verdict"] = UNTESTABLE
            by_season.append(row)
        return {
            "route_months": self.route_months,
            "routes": self.routes,
            "months": self.months,
            "alpha": self.alpha,
            "confidence": self.confidence,
            "seed": self.seed,
            "tests": tests,
            "by_season": by_season,
            "errors": list(self.errors),
            "ok": self.ok,
        }


def analyse(
    panel: Panel,
    *,
    permutations: int = DEFAULT_PERMUTATIONS,
    resamples: int = DEFAULT_BOOTSTRAP,
    confidence: float = DEFAULT_CONFIDENCE,
    alpha: float = DEFAULT_ALPHA,
    seed: int | None = DEFAULT_SEED,
    by_season: bool = True,
    min_strata: int = DEFAULT_MIN_STRATA,
) -> WeatherAnalysis:
    """Test every factor on the panel and adjust the p-values together."""
    result = WeatherAnalysis(
        route_months=len(panel),
        routes=panel.routes,
        months=panel.months,
        alpha=alpha,
        confidence=confidence,
        seed=seed,
    )
    if not len(panel):
        result.errors.append("the route-month panel is empty (no covered route-months ran)")
        return result

    strata, clusters = panel.strata, panel.clusters
    raw = [
        analyse_factor(
            factor,
            panel.trains_on_time,
            panel.trains_run,
            panel.adverse[factor],
            strata,
            clusters,
            permutations=permutations,
            resamples=resamples,
            confidence=confidence,
            seed=seed,
        )
        for factor in FACTORS
    ]
    adjusted = benjamini_hochberg([t.p_value for t in raw])
    result.tests = [
        replace(t, p_adjusted=None if math.isnan(p) else p)
        for t, p in zip(raw, adjusted, strict=True)
    ]
    if by_season:
        result.seasons = season_breakdown(
            panel,
            permutations=permutations,
            resamples=resamples,
            confidence=confidence,
            min_strata=min_strata,
            seed=seed,
        )
    untestable = [t.factor for t in result.tests if verdict(t, alpha) == UNTESTABLE]
    if len(untestable) == len(result.tests):
        result.errors.append("no factor has a stratum with both adverse and normal months")
    return result


def _pts(gap: float) -> str:
    return "n/a" if math.isnan(gap) else f"{100 * gap:+.2f} pts"


def _p(value: float | None) -> str:
    if value is None or math.isnan(value):
        return "n/a"
    return "< 0.001" if value < 0.001 else f"{value:.3f}"


def headline(test: FactorTest, alpha: float = DEFAULT_ALPHA) -> str:
    name = test.factor.capitalize()
    reading = verdict(test, alpha)
    if reading == UNTESTABLE:
        return f"{name}: not testable (no route-season with both adverse and normal months)."
    return (
        f"{name}: {reading} — {_pts(test.gap)} within route-season "
        f"(CI {_pts(test.ci_low)} to {_pts(test.ci_high)}, "
        f"adjusted p {_p(test.p_adjusted)}); pooled gap {_pts(test.naive_gap)}."
    )


def render_markdown(result: WeatherAnalysis) -> str:
    level = f"{100 * result.confidence:g}%"
    out = [
        "# Does bad weather lower TGV punctuality?",
        "",
        f"{result.route_months:,} route-months over {result.routes} routes and "
        f"{result.months} months. Gaps are adverse minus normal on-time share, in "
        f"percentage points; negative means later trains when the weather turns.",
        "",
    ]
    out += [f"- {headline(t, result.alpha)}" for t in result.tests] or ["_Nothing tested._"]
    if result.tests:
        out += [
            "",
            "| factor | pooled gap | stratified gap | "
            f"{level} CI | p | adjusted p | strata | adverse route-months | verdict |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        out += [
            f"| {t.factor} | {_pts(t.naive_gap)} | {_pts(t.gap)} | "
            f"{_pts(t.ci_low)} to {_pts(t.ci_high)} | {_p(t.p_value)} | {_p(t.p_adjusted)} | "
            f"{t.strata_used:,} | {t.adverse_route_months:,} | {verdict(t, result.alpha)} |"
            for t in result.tests
        ]
        first = result.tests[0]
        out += [
            "",
            f"Method: CMH-weighted risk difference inside route x service x season strata; "
            f"two-sided permutation test ({first.permutations:,} within-stratum shuffles); "
            f"{level} percentile CI from {first.bootstrap:,} route-cluster bootstrap resamples; "
            f"Benjamini-Hochberg across the {len(result.tests)} factors at alpha "
            f"{result.alpha:g}; seed {result.seed}.",
        ]
    out += _render_seasons(result)
    if result.errors:
        out += ["", "## Problems", ""] + [f"- {e}" for e in result.errors]
    return "\n".join(out) + "\n"


def _render_seasons(result: WeatherAnalysis) -> list[str]:
    if not result.seasons:
        return []
    factors = list(FACTORS)
    out = [
        "",
        "## By season",
        "",
        "The same test inside each season (strata route x service). Exploratory: "
        f"p-values adjusted across all {len(result.seasons)} cells; a cell needs a few "
        "route-services with both adverse and normal months to be tested.",
        "",
        "| season | " + " | ".join(factors) + " |",
        "|---|" + "---|" * len(factors),
    ]
    for season in dict.fromkeys(c.season for c in result.seasons):
        cells = {c.factor: c for c in result.seasons if c.season == season}
        shown = []
        for factor in factors:
            cell = cells.get(factor)
            if cell is None or cell.test is None:
                shown.append("—")
                continue
            mark = " *" if verdict(cell.test, result.alpha) in (LOWERS, RAISES) else ""
            shown.append(f"{_pts(cell.test.gap)} (p {_p(cell.test.p_adjusted)}){mark}")
        out.append(f"| {season} | " + " | ".join(shown) + " |")
    out += ["", "`*` significant after adjustment, CI excluding zero; `—` too few strata."]
    return out


def _connect(dsn: str) -> Any:
    import psycopg

    return psycopg.connect(dsn, autocommit=True)


def main(argv: Sequence[str] | None = None) -> int:
    from railfall.transform.report import reader_dsn

    parser = argparse.ArgumentParser(prog="railfall.analysis.weather", description=__doc__)
    parser.add_argument("--out", type=Path, help="write the Markdown here instead of stdout")
    parser.add_argument("--report", type=Path, help="also write the results as JSON here")
    parser.add_argument("--json", action="store_true", help="print JSON instead of Markdown")
    parser.add_argument("--permutations", type=int, default=DEFAULT_PERMUTATIONS)
    parser.add_argument("--bootstrap", type=int, default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--no-seasons", action="store_true", help="skip the season-by-season breakdown"
    )
    args = parser.parse_args(argv)

    with _connect(reader_dsn()) as conn:
        panel = read_panel(conn)
    result = analyse(
        panel,
        permutations=args.permutations,
        resamples=args.bootstrap,
        alpha=args.alpha,
        seed=args.seed,
        by_season=not args.no_seasons,
    )

    markdown = render_markdown(result)
    if args.report:
        args.report.write_text(json.dumps(result.to_dict(), indent=2) + "\n")
    if args.out:
        args.out.write_text(markdown)
    if args.json:
        print(json.dumps(result.to_dict(), indent=2))
    elif not args.out:
        print(markdown, end="")
    for error in result.errors:
        print(f"error: {error}", file=sys.stderr)
    return 0 if result.ok else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
