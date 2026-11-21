"""Season-by-season breakdown on a synthetic panel with a winter-only effect."""

from __future__ import annotations

import numpy as np
import pytest

from railfall.analysis.panel import build_panel
from railfall.analysis.seasons import season_breakdown

SEASONS = {1: "winter", 4: "spring", 7: "summer", 10: "autumn"}


def _rows(seed: int = 5) -> list[tuple]:
    """10 routes x 3 years x 4 seasons x 3 months; rain costs 8 pts in winter only."""
    rng = np.random.default_rng(seed)
    rows = []
    for r in range(10):
        base = 0.78 + 0.012 * r
        for year in (2022, 2023, 2024):
            for start, season in SEASONS.items():
                for offset in range(3):
                    wet = bool(rng.random() < 0.45)
                    windy = bool(rng.random() < 0.3)
                    frosty = season == "winter" and bool(rng.random() < 0.5)
                    cost = 0.08 if (wet and season == "winter") else 0.0
                    rate = base - cost + rng.normal(0, 0.004)
                    rows.append(
                        (f"R{r:02d}", "National", f"{year}-{start + offset:02d}-01", season,
                         300, round(300 * rate), wet, windy, frosty)
                    )  # fmt: skip
    return rows


@pytest.fixture(scope="module")
def cells():
    return season_breakdown(build_panel(_rows()), permutations=199, resamples=199, seed=2)


def test_cells_cover_every_season_and_factor_in_order(cells):
    assert [(c.season, c.factor) for c in cells[:3]] == [
        ("winter", "rain"),
        ("winter", "wind"),
        ("winter", "frost"),
    ]
    assert [c.season for c in cells[::3]] == ["winter", "spring", "summer", "autumn"]
    assert all(c.route_months == 10 * 3 * 3 for c in cells)


def test_winter_rain_is_found_and_other_seasons_are_not(cells):
    by_key = {(c.season, c.factor): c for c in cells}
    winter_rain = by_key["winter", "rain"].test
    assert winter_rain is not None
    assert winter_rain.gap == pytest.approx(-0.08, abs=0.006)
    assert winter_rain.ci_high < 0
    assert winter_rain.p_adjusted is not None and winter_rain.p_adjusted < 0.05

    for season in ("spring", "summer", "autumn"):
        test = by_key[season, "rain"].test
        assert test is not None
        assert abs(test.gap) < 0.01
        assert test.ci_low < 0 < test.ci_high


def test_frost_outside_winter_is_not_tested(cells):
    for season in ("spring", "summer", "autumn"):
        cell = next(c for c in cells if c.season == season and c.factor == "frost")
        assert not cell.tested
        assert cell.strata_available == 0


def test_adjustment_spans_all_tested_cells(cells):
    tested = [c.test for c in cells if c.test is not None]
    assert len(tested) == 4 * 2 + 1  # rain and wind everywhere, frost in winter
    for t in tested:
        assert t.p_adjusted is not None and t.p_adjusted >= t.p_value


def test_min_strata_gates_thin_cells():
    panel = build_panel(_rows())
    cells = season_breakdown(panel, permutations=19, resamples=19, min_strata=11)
    assert not any(c.tested for c in cells)  # 10 routes can never give 11 strata
    assert all(c.strata_available <= 10 for c in cells)
    with pytest.raises(ValueError, match="min_strata"):
        season_breakdown(panel, min_strata=0)


def test_empty_panel_gives_no_cells():
    assert season_breakdown(build_panel([])) == []


def test_subset_and_season_order():
    panel = build_panel(_rows())
    winter = panel.subset(panel.season == "winter")
    assert len(winter) == 90
    assert winter.seasons() == ["winter"]
    assert set(winter.adverse) == {"rain", "wind", "frost"}
    assert all(flags.size == 90 for flags in winter.adverse.values())
    assert panel.seasons() == ["winter", "spring", "summer", "autumn"]
    with pytest.raises(ValueError, match="one entry"):
        panel.subset(np.ones(3, dtype=bool))
