import math

import numpy as np
import pytest

from railfall.analysis.stats import (
    analyse_factor,
    benjamini_hochberg,
    cluster_bootstrap_ci,
    permutation_test,
    pooled_gap,
    stratified_gap,
)


def _panel(effect: float, *, routes: int = 30, months: int = 24, seed: int = 1):
    """Synthetic route-months: each route has its own base rate, winter is worse,
    and adverse months (more frequent in winter) lose `effect` of punctuality."""
    rng = np.random.default_rng(seed)
    route = np.repeat(np.arange(routes), months)
    month = np.tile(np.arange(months), routes)
    winter = np.isin(month % 12, (11, 0, 1))
    base = rng.uniform(0.75, 0.92, routes)[route] - 0.08 * winter
    adverse = rng.random(route.size) < np.where(winter, 0.6, 0.15)
    rate = np.clip(base + effect * adverse + rng.normal(0, 0.02, route.size), 0, 1)
    run = rng.integers(200, 800, route.size).astype(float)
    on_time = np.round(rate * run)
    season = np.where(winter, "winter", "other")
    strata = np.char.add(route.astype(str), np.char.add("|", season))
    return on_time, run, adverse, strata, route


def test_pooled_gap_rebuilds_rates_from_counts():
    # adverse: 80/100 on time, normal: (90 + 900)/(100 + 1000) = 0.9
    assert pooled_gap([80, 90, 900], [100, 100, 1000], [True, False, False]) == pytest.approx(-0.1)


def test_pooled_gap_is_nan_without_one_exposure():
    assert math.isnan(pooled_gap([1, 2], [2, 2], [False, False]))


def test_stratified_gap_ignores_strata_with_one_exposure():
    # stratum a: adverse 0.5 vs normal 0.9; stratum b only normal -> dropped.
    est = stratified_gap([50, 90, 10], [100, 100, 10], [True, False, False], ["a", "a", "b"])
    assert est.gap == pytest.approx(-0.4)
    assert est.strata_used == 1
    assert est.route_months_used == 2


def test_stratified_gap_weights_strata_by_cmh():
    # a: n_adv=n_norm=100 -> w=50, diff -0.2 ; b: 10 vs 10 -> w=5, diff +0.2
    est = stratified_gap(
        [70, 90, 9, 7], [100, 100, 10, 10], [True, False, True, False], ["a", "a", "b", "b"]
    )
    assert est.gap == pytest.approx((50 * -0.2 + 5 * 0.2) / 55)


def test_stratification_removes_route_mix_confounding():
    # Route x is always punctual and never adverse; route y is late and often
    # adverse. Within each route weather does nothing: pooled says it does.
    on_time = [95, 95, 70, 70, 70, 70]
    run = [100] * 6
    adverse = [False, False, True, True, True, False]
    route = ["x", "x", "y", "y", "y", "y"]
    assert pooled_gap(on_time, run, adverse) < -0.1
    assert stratified_gap(on_time, run, adverse, route).gap == pytest.approx(0)


def test_validation_rejects_inconsistent_counts():
    with pytest.raises(ValueError, match="trains_on_time"):
        stratified_gap([11], [10], [True], ["a"])
    with pytest.raises(ValueError, match="same length"):
        stratified_gap([1, 2], [10], [True], ["a"])


def test_permutation_test_finds_a_real_effect():
    on_time, run, adverse, strata, _ = _panel(effect=-0.05)
    p = permutation_test(on_time, run, adverse, strata, permutations=500, seed=3)
    assert p == pytest.approx(1 / 501)


def test_permutation_test_is_calm_without_effect():
    on_time, run, adverse, strata, _ = _panel(effect=0.0, seed=7)
    p = permutation_test(on_time, run, adverse, strata, permutations=500, seed=3)
    assert p > 0.05


def test_permutation_test_is_reproducible_with_a_seed():
    on_time, run, adverse, strata, _ = _panel(effect=-0.005)
    first = permutation_test(on_time, run, adverse, strata, permutations=200, seed=11)
    again = permutation_test(on_time, run, adverse, strata, permutations=200, seed=11)
    assert first == again


def test_permutation_test_without_comparable_strata_is_nan():
    assert math.isnan(permutation_test([1, 2], [2, 2], [True, False], ["a", "b"]))


def test_bootstrap_ci_brackets_the_true_effect():
    on_time, run, adverse, strata, route = _panel(effect=-0.04)
    low, high = cluster_bootstrap_ci(on_time, run, adverse, strata, route, resamples=300)
    assert low < -0.04 < high
    assert high < 0


def test_bootstrap_ci_rejects_bad_confidence():
    with pytest.raises(ValueError, match="confidence"):
        cluster_bootstrap_ci([1], [2], [True], ["a"], ["r"], confidence=1.5)


def test_benjamini_hochberg_matches_the_textbook_example():
    # p = 0.01, 0.04, 0.03, 0.005 with m = 4: sorted 0.005, 0.01, 0.03, 0.04
    # scaled 0.02, 0.02, 0.04, 0.04 -> already monotone.
    assert benjamini_hochberg([0.01, 0.04, 0.03, 0.005]) == pytest.approx([0.02, 0.04, 0.04, 0.02])


def test_benjamini_hochberg_keeps_nan_and_caps_at_one():
    out = benjamini_hochberg([float("nan"), 0.9, 0.8])
    assert math.isnan(out[0])
    assert out[1:] == pytest.approx([0.9, 0.9])


def test_analyse_factor_reports_naive_and_stratified_gaps():
    on_time, run, adverse, strata, route = _panel(effect=-0.03)
    result = analyse_factor(
        "frost", on_time, run, adverse, strata, route, permutations=200, resamples=200
    )
    assert result.factor == "frost"
    # Adverse months cluster in winter, so the pooled gap mixes in the winter dip.
    assert result.naive_gap < result.gap < 0
    assert result.ci_low <= result.gap <= result.ci_high
    assert result.significant
    assert result.adverse_route_months == int(np.sum(adverse))
    assert 0 < result.route_months_used <= len(run)
