"""Turn the weather results (JSON) into SVG charts for the write-up.

    python -m railfall.analysis.charts --stats weather-stats.json \\
        --report weather-report.json --out-dir docs/results

Inputs are the files `railfall.analysis.weather --report` and
`railfall.transform.report --report` already write, so charting needs no
database: e2e renders them from the live run, and the committed charts in
`docs/results/` can be regenerated from the JSON next to them.

Charts written (each skipped when its input section is missing or empty):

* `effects.svg`: per factor, the stratified gap (adverse minus normal on-time
  share, points) with its bootstrap CI, and the naive pooled gap beside it;
* `seasons.svg`: the same gap per season x factor, tested cells only, with CIs;
* `dose.svg`: the dose-response slope per factor with its CI;
* `intensity.svg`: on-time share per intensity band, one panel per factor (dots,
  since the axis is zoomed in on the range that matters).

A `charts.json` manifest lists what was drawn and how many data points each
chart holds, so CI can assert the charts are not empty without parsing SVG.
SVG output is deterministic (fixed hash salt, no date), so re-rendering the
same JSON leaves git clean.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

FACTORS = ("rain", "wind", "frost")
SEASON_ORDER = ("winter", "spring", "summer", "autumn")

# Categorical slots 1-3 of the reference palette (validated all-pairs in light
# mode); text and grid inks stay neutral so colour only carries the factor.
COLORS = {"rain": "#257de0", "wind": "#f47230", "frost": "#00b894"}
TEXT = "#191919"
MUTED = "#666666"
GRID = "#e5e5e5"
SURFACE = "#ffffff"


@dataclass
class Chart:
    name: str
    file: str
    title: str
    points: int


def _style() -> Any:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "svg.hashsalt": "railfall",
            "svg.fonttype": "none",  # keep text as text: smaller, searchable, diffable
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "text.color": TEXT,
            "axes.labelcolor": TEXT,
            "axes.edgecolor": MUTED,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
        }
    )
    return plt


def _num(value: Any) -> float:
    return math.nan if value is None else float(value)


def _save(plt: Any, fig: Any, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, format="svg", metadata={"Date": None}, facecolor=SURFACE)
    plt.close(fig)


def _zero_line(ax: Any, *, vertical: bool) -> None:
    if vertical:
        ax.axvline(0, color=MUTED, linewidth=1, zorder=1)
    else:
        ax.axhline(0, color=MUTED, linewidth=1, zorder=1)


def effects_chart(plt: Any, tests: Sequence[Mapping[str, Any]], path: Path) -> int:
    """Forest plot: stratified gap + CI per factor, pooled gap as a hollow marker."""
    rows = [t for t in tests if t.get("gap") is not None]
    if not rows:
        return 0
    fig, ax = plt.subplots(figsize=(7, 0.8 + 0.7 * len(rows)))
    for y, test in enumerate(reversed(rows)):
        color = COLORS.get(test["factor"], MUTED)
        gap, low, high = (100 * _num(test[k]) for k in ("gap", "ci_low", "ci_high"))
        if not math.isnan(low):
            ax.plot([low, high], [y, y], color=color, linewidth=2, solid_capstyle="round")
        ax.plot(gap, y, "o", color=color, markersize=9, markeredgecolor=SURFACE, zorder=3)
        naive = _num(test.get("naive_gap"))
        if not math.isnan(naive):
            ax.plot(100 * naive, y, "o", markerfacecolor="none", markeredgecolor=MUTED,
                    markersize=8, zorder=2)  # fmt: skip
        ax.annotate(f"{gap:+.2f} pts · {test.get('verdict', '')}", (gap, y),
                    xytext=(0, 9), textcoords="offset points", ha="center",
                    fontsize=8.5, color=TEXT)  # fmt: skip
    ax.set_yticks(range(len(rows)), [t["factor"] for t in reversed(rows)])
    ax.set_ylim(-0.6, len(rows) - 0.3)
    _zero_line(ax, vertical=True)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xlabel("on-time share, adverse minus normal month (percentage points)")
    ax.set_title("Effect of each weather factor within route × season\n"
                 "filled: stratified gap with 95% CI · hollow: naive pooled gap",
                 fontsize=10, loc="left")  # fmt: skip
    _save(plt, fig, path)
    return len(rows)


def seasons_chart(plt: Any, cells: Sequence[Mapping[str, Any]], path: Path) -> int:
    """Grouped dot-and-whisker per season, one colour per factor; untested cells omitted."""
    tested = [c for c in cells if c.get("tested") and c.get("gap") is not None]
    if not tested:
        return 0
    seasons = [s for s in SEASON_ORDER if any(c["season"] == s for c in cells)]
    seasons += sorted({c["season"] for c in cells} - set(seasons))
    offsets = {f: (i - 1) * 0.22 for i, f in enumerate(FACTORS)}
    fig, ax = plt.subplots(figsize=(7.5, 3.8))
    drawn: set[str] = set()
    for cell in tested:
        factor = cell["factor"]
        x = seasons.index(cell["season"]) + offsets.get(factor, 0.0)
        gap, low, high = (100 * _num(cell[k]) for k in ("gap", "ci_low", "ci_high"))
        color = COLORS.get(factor, MUTED)
        if not math.isnan(low):
            ax.plot([x, x], [low, high], color=color, linewidth=2, solid_capstyle="round")
        significant = cell.get("verdict") in ("lowers punctuality", "raises punctuality")
        ax.plot(x, gap, "o", markersize=8, color=color,
                markerfacecolor=color if significant else SURFACE,
                markeredgewidth=2, zorder=3,
                label=None if factor in drawn else factor)  # fmt: skip
        drawn.add(factor)
    ax.set_xticks(range(len(seasons)), seasons)
    _zero_line(ax, vertical=False)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_ylabel("gap (pts) with 95% CI")
    ax.legend(frameon=False, ncols=3, loc="upper left", bbox_to_anchor=(0, -0.1))
    ax.set_title("Gap by season (filled = significant after adjustment)",
                 fontsize=10, loc="left")  # fmt: skip
    _save(plt, fig, path)
    return len(tested)


def dose_chart(plt: Any, dose: Sequence[Mapping[str, Any]], path: Path) -> int:
    """Slope per unit of intensity, one row per factor (units differ, so labelled per row)."""
    rows = [d for d in dose if d.get("slope") is not None]
    if not rows:
        return 0
    fig, ax = plt.subplots(figsize=(7, 0.8 + 0.7 * len(rows)))
    for y, row in enumerate(reversed(rows)):
        color = COLORS.get(row["factor"], MUTED)
        slope, low, high = (100 * _num(row[k]) for k in ("slope", "ci_low", "ci_high"))
        if not math.isnan(low):
            ax.plot([low, high], [y, y], color=color, linewidth=2, solid_capstyle="round")
        ax.plot(slope, y, "o", color=color, markersize=9, markeredgecolor=SURFACE, zorder=3)
        ax.annotate(f"{slope:+.2f} pts per {row.get('unit', 'unit')} · "
                    f"{row.get('consistency', '')}", (slope, y), xytext=(0, 9),
                    textcoords="offset points", ha="center", fontsize=8.5)  # fmt: skip
    ax.set_yticks(range(len(rows)), [r["factor"] for r in reversed(rows)])
    ax.set_ylim(-0.6, len(rows) - 0.3)
    _zero_line(ax, vertical=True)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xlabel("change in on-time share per unit of intensity (percentage points)")
    ax.set_title("Dose-response: does it get worse the harder it hits?",
                 fontsize=10, loc="left")  # fmt: skip
    _save(plt, fig, path)
    return len(rows)


def intensity_chart(plt: Any, intensity: Sequence[Mapping[str, Any]], path: Path) -> int:
    """On-time share per band, small multiples sharing one y-axis; baseline hollow."""
    factors = [f for f in FACTORS if any(r["weather_factor"] == f for r in intensity)]
    if not factors:
        return 0
    fig, axes = plt.subplots(1, len(factors), figsize=(3.2 * len(factors), 3.6),
                             sharey=True, squeeze=False)  # fmt: skip
    rates = [_num(r["arrival_punctuality_rate"]) for r in intensity]
    finite = [r for r in rates if not math.isnan(r)]
    low = max(0.0, min(finite) - 0.03) if finite else 0.0
    high = min(1.0, max(finite) + 0.02) if finite else 1.0
    points = 0
    for ax, factor in zip(axes[0], factors, strict=True):
        rows = sorted((r for r in intensity if r["weather_factor"] == factor),
                      key=lambda r: r.get("band_order", 0))  # fmt: skip
        values = [100 * _num(r["arrival_punctuality_rate"]) for r in rows]
        # Dots, not bars: the axis does not start at zero, and a truncated bar lies.
        ax.plot(range(len(rows)), values, color=COLORS[factor], linewidth=2, zorder=2)
        for i, (row, value) in enumerate(zip(rows, values, strict=True)):
            base = bool(row.get("is_baseline"))
            ax.plot(i, value, "o", markersize=9, color=COLORS[factor], markeredgewidth=2,
                    markerfacecolor=SURFACE if base else COLORS[factor], zorder=3)  # fmt: skip
        for i, value in enumerate(values):
            if not math.isnan(value):
                ax.annotate(f"{value:.1f}%", (i, value), xytext=(0, 8),
                            textcoords="offset points", ha="center", fontsize=8)  # fmt: skip
        ax.set_xticks(range(len(rows)), [r["intensity_band"] for r in rows],
                      rotation=30, ha="right", fontsize=8)  # fmt: skip
        ax.set_title(factor, fontsize=10, loc="left")
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        points += len(rows)
    axes[0][0].set_ylim(100 * low, 100 * high)
    axes[0][0].set_ylabel("trains on time at arrival (%)")
    fig.suptitle("On-time share by weather intensity (hollow = baseline band)",
                 fontsize=10, x=0.01, ha="left")  # fmt: skip
    _save(plt, fig, path)
    return points


def render(
    stats: Mapping[str, Any] | None,
    report: Mapping[str, Any] | None,
    out_dir: Path,
) -> list[Chart]:
    """Draw every chart the inputs allow into `out_dir`; return what was drawn."""
    plt = _style()
    out_dir.mkdir(parents=True, exist_ok=True)
    plan = []
    if stats:
        plan += [
            ("effects", "Effect of each weather factor", effects_chart, stats.get("tests")),
            ("seasons", "Effect by season", seasons_chart, stats.get("by_season")),
            ("dose", "Dose-response slopes", dose_chart, stats.get("dose")),
        ]
    if report:
        plan.append(
            ("intensity", "On-time share by intensity", intensity_chart, report.get("intensity"))
        )
    charts = []
    for name, title, draw, data in plan:
        path = out_dir / f"{name}.svg"
        points = draw(plt, data or [], path)
        if points:
            charts.append(Chart(name=name, file=path.name, title=title, points=points))
        elif path.exists():
            path.unlink()  # a stale chart from an older run must not survive
    return charts


def _load(path: Path | None) -> dict[str, Any] | None:
    return None if path is None else json.loads(path.read_text())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="railfall.analysis.charts", description=__doc__)
    parser.add_argument("--stats", type=Path, help="weather-stats.json (analysis.weather)")
    parser.add_argument("--report", type=Path, help="weather-report.json (transform.report)")
    parser.add_argument("--out-dir", type=Path, default=Path("docs/results"))
    args = parser.parse_args(argv)
    if args.stats is None and args.report is None:
        parser.error("give --stats and/or --report")

    charts = render(_load(args.stats), _load(args.report), args.out_dir)
    manifest = {"charts": [asdict(c) for c in charts]}
    (args.out_dir / "charts.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for chart in charts:
        print(f"{args.out_dir / chart.file}: {chart.title} ({chart.points} points)")
    if not charts:
        print("error: nothing to chart in the given inputs", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
