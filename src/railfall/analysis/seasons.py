"""Where does the weather effect live? The same test, run one season at a time.

The headline test (`railfall.analysis.weather`) pools all seasons into one
stratified gap. That answers "does rain lower punctuality on average?" but can
hide a winter-only effect diluted by three quiet seasons, or a summer storm
effect that frost never shows. This module re-runs the identical machinery on
each season's route-months separately: strata are route x service inside the
season, clusters are still routes.

These are exploratory sub-group tests, so:

* the p-values are adjusted with Benjamini-Hochberg across **all** factor x
  season cells together (up to 12), which is stricter than per factor;
* a cell needs at least `min_strata` route-service strata with both adverse
  and normal months, otherwise it is reported as "too few strata" rather than
  given a p-value that rests on one or two routes. Frost in summer is the
  expected example: no stratum has a frosty month.

Same seed for every cell, so the whole table is reproducible.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace

import numpy as np

from railfall.analysis.panel import FACTORS, Panel
from railfall.analysis.stats import (
    DEFAULT_BOOTSTRAP,
    DEFAULT_CONFIDENCE,
    DEFAULT_PERMUTATIONS,
    FactorTest,
    analyse_factor,
    benjamini_hochberg,
    stratified_gap,
)

DEFAULT_MIN_STRATA = 3


@dataclass(frozen=True)
class SeasonCell:
    """One factor tested inside one season."""

    season: str
    factor: str
    route_months: int
    test: FactorTest | None
    """None when the season has fewer than `min_strata` usable strata."""
    strata_available: int

    @property
    def tested(self) -> bool:
        return self.test is not None


def _route_service(panel: Panel) -> np.ndarray:
    return np.char.add(np.char.add(panel.route_id, "|"), panel.service)


def season_breakdown(
    panel: Panel,
    *,
    permutations: int = DEFAULT_PERMUTATIONS,
    resamples: int = DEFAULT_BOOTSTRAP,
    confidence: float = DEFAULT_CONFIDENCE,
    min_strata: int = DEFAULT_MIN_STRATA,
    seed: int | None = 0,
) -> list[SeasonCell]:
    """Every factor x season cell, in season then factor order, BH-adjusted together."""
    if min_strata < 1:
        raise ValueError("min_strata must be >= 1")
    cells: list[SeasonCell] = []
    for season in panel.seasons():
        part = panel.subset(panel.season == season)
        strata = _route_service(part)
        for factor in FACTORS:
            adverse = part.adverse[factor]
            available = stratified_gap(
                part.trains_on_time, part.trains_run, adverse, strata
            ).strata_used
            test = None
            if available >= min_strata:
                test = analyse_factor(
                    factor,
                    part.trains_on_time,
                    part.trains_run,
                    adverse,
                    strata,
                    part.clusters,
                    permutations=permutations,
                    resamples=resamples,
                    confidence=confidence,
                    seed=seed,
                )
            cells.append(SeasonCell(season, factor, len(part), test, available))

    tested = [(i, c.test) for i, c in enumerate(cells) if c.test is not None]
    adjusted = benjamini_hochberg([test.p_value for _, test in tested])
    for (i, test), p in zip(tested, adjusted, strict=True):
        fixed = replace(test, p_adjusted=None if math.isnan(p) else p)
        cells[i] = replace(cells[i], test=fixed)
    return cells
