"""Tests for "does this weather factor lower punctuality?" on route-month data.

The unit of observation is a route-month (one TGV route, one month), carrying
`trains_run` and `trains_on_time`. Two things make a naive two-proportion test
on pooled trains wrong here:

* **Trains are not independent.** Every train of a route-month shares the same
  rolling stock, timetable and incidents, so the effective sample size is the
  number of route-months, not the number of trains. A z-test on ~millions of
  trains calls every half-point gap "significant".
* **Weather is confounded with route and season.** Frost happens in winter, on
  some routes more than others, and winter timetables and Alpine routes have
  their own punctuality. Pooling all adverse months against all normal ones
  measures the route mix and the calendar as much as the weather.

So the estimate is a *stratified* risk difference: route-months are grouped
into strata (typically route x season), the on-time rate of adverse months is
compared to that of normal months inside each stratum that has both, and the
stratum gaps are averaged with Cochran-Mantel-Haenszel weights
`n_adverse * n_normal / (n_adverse + n_normal)` (trains run on each side). A
stratum with only one exposure says nothing about the effect and drops out.

Uncertainty comes from resampling, not from a distributional formula:

* a **stratified permutation test**: the adverse labels are shuffled *within*
  each stratum, which is exactly the null "inside a route-season, which months
  were adverse is unrelated to punctuality"; the p-value is two-sided with the
  usual +1 correction so it is never 0;
* a **cluster bootstrap** confidence interval: whole routes are resampled with
  replacement, so the correlation between a route's months is kept.

Several factors are tested at once, hence `benjamini_hochberg`.

Pure numpy; no database, no plotting. The reader and the report live elsewhere.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
from numpy.typing import ArrayLike, NDArray

DEFAULT_PERMUTATIONS = 2_000
DEFAULT_BOOTSTRAP = 1_000
DEFAULT_CONFIDENCE = 0.95


@dataclass(frozen=True)
class GapEstimate:
    """Stratified adverse-minus-normal on-time rate, with the strata it rests on."""

    gap: float
    """Adverse minus normal on-time share (fraction, negative = worse when adverse)."""
    strata_used: int
    """Strata with both adverse and normal route-months."""
    route_months_used: int
    """Route-months in those strata."""


@dataclass(frozen=True)
class FactorTest:
    """Everything one factor's test produced, ready to report."""

    factor: str
    naive_gap: float
    gap: float
    ci_low: float
    ci_high: float
    p_value: float
    strata_used: int
    route_months_used: int
    adverse_route_months: int
    permutations: int
    bootstrap: int
    p_adjusted: float | None = None

    @property
    def significant(self) -> bool:
        p = self.p_adjusted if self.p_adjusted is not None else self.p_value
        return p < 0.05


def _as_arrays(
    on_time: ArrayLike, run: ArrayLike, adverse: ArrayLike, strata: ArrayLike
) -> tuple[NDArray[np.float64], NDArray[np.float64], NDArray[np.bool_], NDArray[np.intp]]:
    on_time_a = np.asarray(on_time, dtype=np.float64)
    run_a = np.asarray(run, dtype=np.float64)
    adverse_a = np.asarray(adverse, dtype=bool)
    # Any hashable labels (route ids, "route|season" strings) become 0..k-1 codes.
    _, codes = np.unique(np.asarray(strata), return_inverse=True)
    sizes = {on_time_a.shape, run_a.shape, adverse_a.shape, codes.shape}
    if len(sizes) != 1 or on_time_a.ndim != 1:
        raise ValueError("on_time, run, adverse and strata must be 1-D and the same length")
    if np.any(run_a < 0) or np.any(on_time_a < 0) or np.any(on_time_a > run_a):
        raise ValueError("need 0 <= trains_on_time <= trains_run on every route-month")
    return on_time_a, run_a, adverse_a, codes.astype(np.intp)


def pooled_gap(on_time: ArrayLike, run: ArrayLike, adverse: ArrayLike) -> float:
    """Adverse minus normal on-time share, pooling all trains (the marts' number).

    NaN when either side has no trains run.
    """
    on_time_a = np.asarray(on_time, dtype=np.float64)
    run_a = np.asarray(run, dtype=np.float64)
    adverse_a = np.asarray(adverse, dtype=bool)
    run_adv, run_norm = run_a[adverse_a].sum(), run_a[~adverse_a].sum()
    if run_adv == 0 or run_norm == 0:
        return float("nan")
    return float(on_time_a[adverse_a].sum() / run_adv - on_time_a[~adverse_a].sum() / run_norm)


def _stratified_gap(
    on_time: NDArray[np.float64],
    run: NDArray[np.float64],
    adverse: NDArray[np.bool_],
    codes: NDArray[np.intp],
    n_strata: int,
) -> float:
    """CMH-weighted risk difference over strata with trains on both sides."""
    adv = adverse.astype(np.float64)
    run_adv = np.bincount(codes, weights=run * adv, minlength=n_strata)
    run_all = np.bincount(codes, weights=run, minlength=n_strata)
    ok_adv = np.bincount(codes, weights=on_time * adv, minlength=n_strata)
    ok_all = np.bincount(codes, weights=on_time, minlength=n_strata)
    run_norm = run_all - run_adv
    ok_norm = ok_all - ok_adv
    both = (run_adv > 0) & (run_norm > 0)
    if not both.any():
        return float("nan")
    weight = run_adv[both] * run_norm[both] / run_all[both]
    diff = ok_adv[both] / run_adv[both] - ok_norm[both] / run_norm[both]
    return float(np.sum(weight * diff) / np.sum(weight))


def stratified_gap(
    on_time: ArrayLike, run: ArrayLike, adverse: ArrayLike, strata: ArrayLike
) -> GapEstimate:
    """Adverse minus normal on-time share, compared inside each stratum only."""
    on_time_a, run_a, adverse_a, codes = _as_arrays(on_time, run, adverse, strata)
    n_strata = int(codes.max()) + 1 if codes.size else 0
    gap = _stratified_gap(on_time_a, run_a, adverse_a, codes, n_strata)

    run_adv = np.bincount(codes, weights=run_a * adverse_a, minlength=n_strata)
    run_norm = np.bincount(codes, weights=run_a * ~adverse_a, minlength=n_strata)
    both = (run_adv > 0) & (run_norm > 0)
    return GapEstimate(
        gap=gap,
        strata_used=int(both.sum()),
        route_months_used=int(both[codes].sum()) if codes.size else 0,
    )


def permutation_test(
    on_time: ArrayLike,
    run: ArrayLike,
    adverse: ArrayLike,
    strata: ArrayLike,
    *,
    permutations: int = DEFAULT_PERMUTATIONS,
    seed: int | None = 0,
) -> float:
    """Two-sided p-value of the stratified gap, labels shuffled within strata.

    Shuffling inside a stratum keeps how many adverse months each stratum had,
    so the null distribution has the same structure as the data. NaN when no
    stratum has both exposures.
    """
    if permutations < 1:
        raise ValueError("permutations must be >= 1")
    on_time_a, run_a, adverse_a, codes = _as_arrays(on_time, run, adverse, strata)
    if not codes.size:
        return float("nan")
    n_strata = int(codes.max()) + 1
    observed = _stratified_gap(on_time_a, run_a, adverse_a, codes, n_strata)
    if np.isnan(observed):
        return float("nan")

    rng = np.random.default_rng(seed)
    # Rows grouped by stratum (stable), so block k of `grouped_labels` holds
    # stratum k's labels. A random tie-breaker gives another grouping with the
    # same blocks in a shuffled order inside each one; writing the labels back
    # through it permutes them within strata.
    grouped_labels = adverse_a[np.argsort(codes, kind="stable")]
    extreme = 0
    shuffled = np.empty_like(adverse_a)
    tolerance = 1e-12  # the observed labelling must count as "as extreme"
    for _ in range(permutations):
        order = np.lexsort((rng.random(codes.size), codes))
        shuffled[order] = grouped_labels
        stat = _stratified_gap(on_time_a, run_a, shuffled, codes, n_strata)
        if abs(stat) >= abs(observed) - tolerance:
            extreme += 1
    return (extreme + 1) / (permutations + 1)


def cluster_bootstrap_ci(
    on_time: ArrayLike,
    run: ArrayLike,
    adverse: ArrayLike,
    strata: ArrayLike,
    clusters: ArrayLike,
    *,
    resamples: int = DEFAULT_BOOTSTRAP,
    confidence: float = DEFAULT_CONFIDENCE,
    seed: int | None = 0,
) -> tuple[float, float]:
    """Percentile CI of the stratified gap, resampling whole clusters (routes).

    A route drawn twice contributes its route-months twice, as two separate
    strata per copy, so each copy is weighted like an independent route.
    Resamples where no stratum has both exposures are dropped; NaN bounds if
    all of them are.
    """
    if resamples < 1:
        raise ValueError("resamples must be >= 1")
    if not 0 < confidence < 1:
        raise ValueError("confidence must be between 0 and 1")
    on_time_a, run_a, adverse_a, codes = _as_arrays(on_time, run, adverse, strata)
    _, cluster_codes = np.unique(np.asarray(clusters), return_inverse=True)
    if cluster_codes.shape != codes.shape:
        raise ValueError("clusters must have one label per route-month")
    if not codes.size:
        return float("nan"), float("nan")

    n_clusters = int(cluster_codes.max()) + 1
    n_strata = int(codes.max()) + 1
    members = [np.flatnonzero(cluster_codes == c) for c in range(n_clusters)]
    rng = np.random.default_rng(seed)
    stats = []
    for _ in range(resamples):
        drawn = rng.integers(0, n_clusters, size=n_clusters)
        rows = np.concatenate([members[c] for c in drawn])
        copy = np.repeat(np.arange(n_clusters), [members[c].size for c in drawn])
        boot_codes = codes[rows] + copy * n_strata
        _, boot_codes = np.unique(boot_codes, return_inverse=True)
        stat = _stratified_gap(
            on_time_a[rows], run_a[rows], adverse_a[rows], boot_codes, int(boot_codes.max()) + 1
        )
        if not np.isnan(stat):
            stats.append(stat)
    if not stats:
        return float("nan"), float("nan")
    alpha = (1 - confidence) / 2
    low, high = np.quantile(np.asarray(stats), [alpha, 1 - alpha])
    return float(low), float(high)


def benjamini_hochberg(p_values: Sequence[float]) -> list[float]:
    """False-discovery-rate adjusted p-values, in the input order. NaN stays NaN."""
    p = np.asarray(p_values, dtype=np.float64)
    out = np.full(p.shape, np.nan)
    valid = np.flatnonzero(~np.isnan(p))
    if not valid.size:
        return out.tolist()
    m = valid.size
    order = valid[np.argsort(p[valid], kind="stable")]
    scaled = p[order] * m / np.arange(1, m + 1)
    # Step-up: each adjusted p is the min of the scaled ones from its rank up.
    adjusted = np.minimum.accumulate(scaled[::-1])[::-1]
    out[order] = np.clip(adjusted, 0, 1)
    return out.tolist()


def analyse_factor(
    factor: str,
    on_time: ArrayLike,
    run: ArrayLike,
    adverse: ArrayLike,
    strata: ArrayLike,
    clusters: ArrayLike,
    *,
    permutations: int = DEFAULT_PERMUTATIONS,
    resamples: int = DEFAULT_BOOTSTRAP,
    confidence: float = DEFAULT_CONFIDENCE,
    seed: int | None = 0,
) -> FactorTest:
    """Naive gap, stratified gap, permutation p-value and bootstrap CI for one factor."""
    estimate = stratified_gap(on_time, run, adverse, strata)
    p_value = permutation_test(on_time, run, adverse, strata, permutations=permutations, seed=seed)
    low, high = cluster_bootstrap_ci(
        on_time, run, adverse, strata, clusters,
        resamples=resamples, confidence=confidence, seed=seed,
    )  # fmt: skip
    return FactorTest(
        factor=factor,
        naive_gap=pooled_gap(on_time, run, adverse),
        gap=estimate.gap,
        ci_low=low,
        ci_high=high,
        p_value=p_value,
        strata_used=estimate.strata_used,
        route_months_used=estimate.route_months_used,
        adverse_route_months=int(np.asarray(adverse, dtype=bool).sum()),
        permutations=permutations,
        bootstrap=resamples,
    )
