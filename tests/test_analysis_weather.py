"""The end-to-end weather analysis on a synthetic panel with known effects."""

from __future__ import annotations

import json
import math

import numpy as np
import pytest

from railfall.analysis import weather
from railfall.analysis.panel import build_panel
from railfall.analysis.stats import FactorTest

SEASONS = {1: "winter", 4: "spring", 7: "summer", 10: "autumn"}


def _rows(*, frost_ever: bool = True, seed: int = 3) -> list[tuple]:
    """12 routes x 4 seasons x 6 months. Rain costs 6 pts; wind and frost cost nothing."""
    rng = np.random.default_rng(seed)
    rows = []
    for r in range(12):
        base = 0.80 + 0.01 * r  # routes differ, so pooling would be confounded
        for year in (2023, 2024):
            for start, season in SEASONS.items():
                for offset in range(3):
                    month = f"{year}-{start + offset:02d}-01"
                    wet, windy = bool(rng.random() < 0.4), bool(rng.random() < 0.3)
                    frosty = frost_ever and season == "winter" and bool(rng.random() < 0.5)
                    rate = base - (0.06 if wet else 0.0) + rng.normal(0, 0.005)
                    run = 400
                    rows.append(
                        (f"R{r:02d}", "National", month, season, run,
                         round(run * min(max(rate, 0), 1)), wet, windy, frosty)
                    )  # fmt: skip
    return rows


@pytest.fixture(scope="module")
def result():
    panel = build_panel(_rows())
    return weather.analyse(panel, permutations=199, resamples=199, seed=1)


def test_finds_the_planted_rain_effect_and_nothing_else(result):
    assert result.ok
    assert result.route_months == 12 * 2 * 4 * 3
    assert result.routes == 12
    by_factor = {t.factor: t for t in result.tests}
    assert list(by_factor) == ["rain", "wind", "frost"]

    rain = by_factor["rain"]
    assert rain.gap == pytest.approx(-0.06, abs=0.005)
    assert rain.ci_high < 0
    assert rain.p_adjusted is not None and rain.p_adjusted < 0.05

    assert result.verdicts() == {
        "rain": weather.LOWERS,
        "wind": weather.NO_EFFECT,
        "frost": weather.NO_EFFECT,
    }


def test_adjusted_p_never_below_raw(result):
    for test in result.tests:
        assert test.p_adjusted is not None
        assert test.p_adjusted >= test.p_value - 1e-12


def test_same_seed_same_answer():
    panel = build_panel(_rows())
    a = weather.analyse(panel, permutations=99, resamples=99, seed=5)
    b = weather.analyse(panel, permutations=99, resamples=99, seed=5)
    assert a.to_dict() == b.to_dict()


def test_factor_never_adverse_is_untestable_not_an_error():
    panel = build_panel(_rows(frost_ever=False))
    result = weather.analyse(panel, permutations=49, resamples=49)

    assert result.ok
    assert result.verdicts()["frost"] == weather.UNTESTABLE
    frost = result.to_dict()["tests"][2]
    assert frost["gap"] is None and frost["p_adjusted"] is None
    json.dumps(result.to_dict(), allow_nan=False)  # strict JSON, no NaN leaks


def test_empty_panel_is_an_error():
    result = weather.analyse(build_panel([]))
    assert not result.ok
    assert result.tests == []
    assert "empty" in result.errors[0]
    assert "_Nothing tested._" in weather.render_markdown(result)


def _test(**overrides) -> FactorTest:
    base = dict(
        factor="rain", naive_gap=-0.03, gap=-0.02, ci_low=-0.03, ci_high=-0.01,
        p_value=0.001, strata_used=10, route_months_used=100, adverse_route_months=40,
        permutations=99, bootstrap=99, p_adjusted=0.003,
    )  # fmt: skip
    base.update(overrides)
    return FactorTest(**base)


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, weather.LOWERS),
        ({"gap": 0.02, "ci_low": 0.01, "ci_high": 0.03}, weather.RAISES),
        ({"p_adjusted": 0.2}, weather.NO_EFFECT),
        ({"ci_high": 0.005}, weather.NO_EFFECT),  # p small but CI straddles zero
        ({"strata_used": 0, "gap": math.nan}, weather.UNTESTABLE),
        ({"p_adjusted": None, "p_value": 0.04}, weather.LOWERS),
    ],
)
def test_verdict(overrides, expected):
    assert weather.verdict(_test(**overrides)) == expected


def test_markdown_reports_every_factor(result):
    text = weather.render_markdown(result)
    assert text.startswith("# Does bad weather lower TGV punctuality?")
    for factor in ("rain", "wind", "frost"):
        assert f"| {factor} |" in text
    assert "Rain: lowers punctuality" in text
    assert "95% CI" in text
    assert "Benjamini-Hochberg" in text


def test_main_writes_markdown_and_json(tmp_path, monkeypatch):
    panel = build_panel(_rows())
    monkeypatch.setattr(weather, "_connect", lambda dsn: _NullConn())
    monkeypatch.setattr(weather, "read_panel", lambda conn: panel)
    out, report = tmp_path / "stats.md", tmp_path / "stats.json"

    code = weather.main(
        ["--out", str(out), "--report", str(report), "--permutations", "49", "--bootstrap", "49"]
    )

    assert code == 0
    assert out.read_text().startswith("# Does bad weather")
    data = json.loads(report.read_text())
    assert data["ok"] and data["routes"] == 12
    assert [t["factor"] for t in data["tests"]] == ["rain", "wind", "frost"]


def test_main_fails_on_empty_panel(monkeypatch, capsys):
    monkeypatch.setattr(weather, "_connect", lambda dsn: _NullConn())
    monkeypatch.setattr(weather, "read_panel", lambda conn: build_panel([]))

    assert weather.main(["--json"]) == 1
    assert "error: the route-month panel is empty" in capsys.readouterr().err


class _NullConn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False
