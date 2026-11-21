"""Dose-response: does punctuality fall *further* as the weather gets worse?

The headline test (`railfall.analysis.weather`) compares adverse months with
normal ones, a yes/no exposure built from thresholds in the dbt marts. If bad
weather really costs punctuality, the cost should grow with intensity: a month
with twice the usual rain should hurt more than one just over the "wet" line.
A slope that is flat or the wrong way round is a warning that the binary gap
picks up something other than the weather.

For each factor the intensity is the worse endpoint's value from
`int_route_month_weather`:

* rain: `max_precipitation_ratio` (month rain / climate normal; the slope is
  per +1.0, i.e. one extra "normal month" of rain);
* wind: `max_strong_wind_days` (slope per extra strong-wind day);
* frost: `max_frost_days` (slope per extra frost day).

The slope is a fixed-effects, trains-weighted least-squares slope of the
on-time share on intensity, after removing each route x service x season
stratum's own mean from both, so only variation *between months of the same
route-season* counts, exactly like the stratified gap. Inference mirrors
`railfall.analysis.stats`: a permutation test shuffling intensities within
strata, and a route-cluster bootstrap CI. Factors are BH-adjusted together.

Pure numpy apart from the reader; the SQL is portable so tests use SQLite.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from typing import Any

import numpy as np
from numpy.typing import ArrayLike, NDArray

from railfall.analysis.panel import FACTORS
from railfall.analysis.stats import (
    DEFAULT_BOOTSTRAP,
    DEFAULT_CONFIDENCE,
    DEFAULT_PERMUTATIONS,
    benjamini_hochberg,
)

# Factor -> intensity column of int_route_month_weather, and the unit of the slope.
INTENSITY: dict[str, tuple[str, str]] = {
    "rain": ("max_precipitation_ratio", "x normal rain"),
    "wind": ("max_strong_wind_days", "strong-wind day"),
    "frost": ("max_frost_days", "frost day"),
}

DOSE_COLUMNS = (
    "route_id",
    "service",
    "season",
    "trains_run",
    "trains_on_time",
    *(column for column, _ in INTENSITY.values()),
)

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def dose_sql(schema: str | None = "marts") -> str:
    """Same population as `panel_sql`, with the intensity columns instead of flags."""
    if schema is not None and not _IDENTIFIER.match(schema):
        raise ValueError(f"invalid schema name: {schema!r}")
    prefix = f"{schema}." if schema else ""
    intensities = ", ".join(f"w.{column}" for column, _ in INTENSITY.values())
    return f"""
        select w.route_id, w.service, m.season,
               w.trains_run, w.trains_on_time, {intensities}
        from {prefix}int_route_month_weather as w
        inner join {prefix}dim_month as m on m.month = w.month
        where w.weather_coverage <> 'none'
          and w.trains_run > 0
          and w.trains_on_time is not null
        order by w.route_id, w.service, w.month
    """


@dataclass(frozen=True)
class DosePanel:
    route_id: NDArray[np.str_]
    strata: NDArray[np.str_]
    trains_run: NDArray[np.float64]
    trains_on_time: NDArray[np.float64]
    intensity: dict[str, NDArray[np.float64]]
    """NaN where the marts have no value for that factor."""

    def __len__(self) -> int:
        return int(self.route_id.size)


def build_dose_panel(rows: Iterable[Sequence[Any]]) -> DosePanel:
    rows = list(rows)
    width = len(DOSE_COLUMNS)
    if any(len(row) != width for row in rows):
        raise ValueError(f"every dose row needs {width} columns: {', '.join(DOSE_COLUMNS)}")
    cols = list(zip(*rows, strict=True)) if rows else [()] * width
    by_name = dict(zip(DOSE_COLUMNS, cols, strict=True))
    route = np.asarray([str(v) for v in by_name["route_id"]], dtype=np.str_)
    service = np.asarray([str(v or "") for v in by_name["service"]], dtype=np.str_)
    season = np.asarray([str(v) for v in by_name["season"]], dtype=np.str_)
    run = np.asarray(by_name["trains_run"], dtype=np.float64)
    on_time = np.asarray(by_name["trains_on_time"], dtype=np.float64)
    if np.any(on_time < 0) or np.any(on_time > run):
        raise ValueError("trains_on_time must lie between 0 and trains_run")
    strata = np.char.add(np.char.add(np.char.add(route, "|"), np.char.add(service, "|")), season)
    return DosePanel(
        route_id=route,
        strata=strata,
        trains_run=run,
        trains_on_time=on_time,
        intensity={
            factor: np.asarray(
                [math.nan if v is None else float(v) for v in by_name[column]], dtype=np.float64
            )
            for factor, (column, _) in INTENSITY.items()
        },
    )


def read_dose_panel(conn: Any, schema: str | None = "marts") -> DosePanel:
    cursor = conn.cursor()
    try:
        cursor.execute(dose_sql(schema))
        return build_dose_panel(cursor.fetchall())
    finally:
        cursor.close()


def _slope(
    share: NDArray[np.float64],
    dose: NDArray[np.float64],
    weight: NDArray[np.float64],
    codes: NDArray[np.intp],
    n_strata: int,
) -> float:
    """Weighted within-stratum slope; NaN when no stratum has varying intensity."""
    w_sum = np.bincount(codes, weights=weight, minlength=n_strata)
    safe = np.where(w_sum > 0, w_sum, 1.0)
    x_mean = np.bincount(codes, weights=weight * dose, minlength=n_strata) / safe
    y_mean = np.bincount(codes, weights=weight * share, minlength=n_strata) / safe
    x = dose - x_mean[codes]
    y = share - y_mean[codes]
    sxx = float(np.sum(weight * x * x))
    if sxx <= 1e-12 * max(1.0, float(np.sum(weight))):
        return math.nan
    return float(np.sum(weight * x * y) / sxx)


def _prepare(
    on_time: ArrayLike, run: ArrayLike, dose: ArrayLike, strata: ArrayLike, clusters: ArrayLike
) -> tuple[NDArray[np.float64], NDArray[np.float64], NDArray[np.float64], NDArray[np.intp], Any]:
    run_a = np.asarray(run, dtype=np.float64)
    on_a = np.asarray(on_time, dtype=np.float64)
    dose_a = np.asarray(dose, dtype=np.float64)
    strata_a = np.asarray(strata)
    clusters_a = np.asarray(clusters)
    if not (run_a.shape == on_a.shape == dose_a.shape == strata_a.shape == clusters_a.shape):
        raise ValueError("all inputs need one entry per route-month")
    keep = ~np.isnan(dose_a) & (run_a > 0)
    _, codes = np.unique(strata_a[keep], return_inverse=True)
    return on_a[keep] / run_a[keep], dose_a[keep], run_a[keep], codes, clusters_a[keep]


@dataclass(frozen=True)
class DoseTest:
    factor: str
    unit: str
    slope: float
    """Change in on-time share per unit of intensity (fraction, not points)."""
    ci_low: float
    ci_high: float
    p_value: float
    p_adjusted: float | None
    route_months: int
    strata_used: int
    permutations: int
    bootstrap: int


def dose_response(
    factor: str,
    on_time: ArrayLike,
    run: ArrayLike,
    dose: ArrayLike,
    strata: ArrayLike,
    clusters: ArrayLike,
    *,
    permutations: int = DEFAULT_PERMUTATIONS,
    resamples: int = DEFAULT_BOOTSTRAP,
    confidence: float = DEFAULT_CONFIDENCE,
    seed: int | None = 0,
) -> DoseTest:
    """Slope, within-stratum permutation p-value and route-cluster bootstrap CI."""
    if permutations < 1 or resamples < 1:
        raise ValueError("permutations and resamples must be >= 1")
    if not 0 < confidence < 1:
        raise ValueError("confidence must be between 0 and 1")
    unit = INTENSITY[factor][1] if factor in INTENSITY else "unit"
    share, dose_a, weight, codes, clusters_a = _prepare(on_time, run, dose, strata, clusters)
    n = int(codes.size)
    nan = math.nan
    if not n:
        return DoseTest(factor, unit, nan, nan, nan, nan, None, 0, 0, permutations, resamples)
    n_strata = int(codes.max()) + 1
    varying = [
        s for s in range(n_strata) if np.ptp(dose_a[codes == s]) > 0 and np.sum(codes == s) > 1
    ]
    observed = _slope(share, dose_a, weight, codes, n_strata)
    if math.isnan(observed):
        return DoseTest(factor, unit, nan, nan, nan, nan, None, n, 0, permutations, resamples)

    rng = np.random.default_rng(seed)
    members = [np.flatnonzero(codes == s) for s in range(n_strata)]
    shuffled = dose_a.copy()
    extreme = 0
    for _ in range(permutations):
        for idx in members:
            if idx.size > 1:
                shuffled[idx] = dose_a[rng.permutation(idx)]
        stat = _slope(share, shuffled, weight, codes, n_strata)
        if not math.isnan(stat) and abs(stat) >= abs(observed) - 1e-15:
            extreme += 1
    p_value = (extreme + 1) / (permutations + 1)

    _, cluster_codes = np.unique(clusters_a, return_inverse=True)
    n_clusters = int(cluster_codes.max()) + 1
    by_cluster = [np.flatnonzero(cluster_codes == c) for c in range(n_clusters)]
    stats = []
    for _ in range(resamples):
        drawn = rng.integers(0, n_clusters, size=n_clusters)
        rows = np.concatenate([by_cluster[c] for c in drawn])
        copy = np.repeat(np.arange(n_clusters), [by_cluster[c].size for c in drawn])
        _, boot_codes = np.unique(codes[rows] + copy * n_strata, return_inverse=True)
        stat = _slope(
            share[rows], dose_a[rows], weight[rows], boot_codes, int(boot_codes.max()) + 1
        )
        if not math.isnan(stat):
            stats.append(stat)
    tail = (1 - confidence) / 2
    low, high = np.quantile(np.asarray(stats), [tail, 1 - tail]) if stats else (nan, nan)
    return DoseTest(
        factor=factor,
        unit=unit,
        slope=observed,
        ci_low=float(low),
        ci_high=float(high),
        p_value=float(p_value),
        p_adjusted=None,
        route_months=n,
        strata_used=len(varying),
        permutations=permutations,
        bootstrap=resamples,
    )


def analyse_dose(
    panel: DosePanel,
    *,
    permutations: int = DEFAULT_PERMUTATIONS,
    resamples: int = DEFAULT_BOOTSTRAP,
    confidence: float = DEFAULT_CONFIDENCE,
    seed: int | None = 0,
) -> list[DoseTest]:
    """One dose-response test per factor, in `FACTORS` order, BH-adjusted together."""
    raw = [
        dose_response(
            factor,
            panel.trains_on_time,
            panel.trains_run,
            panel.intensity[factor],
            panel.strata,
            panel.route_id,
            permutations=permutations,
            resamples=resamples,
            confidence=confidence,
            seed=seed,
        )
        for factor in FACTORS
    ]
    adjusted = benjamini_hochberg([t.p_value for t in raw])
    return [
        replace(t, p_adjusted=None if math.isnan(p) else p)
        for t, p in zip(raw, adjusted, strict=True)
    ]
