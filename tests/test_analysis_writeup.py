"""The results page is generated from the weather results JSON, never typed by hand."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from railfall.analysis import writeup

FIXTURES = Path(__file__).parent / "fixtures" / "analysis"
MANIFEST = [
    {"name": "effects", "file": "effects.svg", "title": "Effect of each weather factor"},
    {"name": "intensity", "file": "intensity.svg", "title": "On-time share by intensity"},
]


@pytest.fixture
def stats() -> dict:
    return json.loads((FIXTURES / "weather-stats.json").read_text())


@pytest.fixture
def report() -> dict:
    data = json.loads((FIXTURES / "weather-report.json").read_text())
    data["headlines"] = ["Rain: 84.0% on time when it hits vs 86.0% otherwise."]
    return data


def test_answer_names_significant_factors_in_factor_order(stats):
    text = writeup.answer(stats["tests"])
    assert text == (
        "**Yes, partly**: rain and frost significantly lower punctuality; wind does not."
    )


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        ((True, True, True), "**Yes**: rain, wind and frost significantly lower punctuality."),
        (
            (False, True, False),
            "**Yes, partly**: wind significantly lowers punctuality; rain and frost do not.",
        ),
        (
            (False, False, False),
            "**No**: none of the factors shows a significant effect after correction.",
        ),
    ],
)
def test_answer_grammar(flags, expected):
    tests = [{"factor": f, "significant": s} for f, s in zip(writeup.FACTORS, flags, strict=True)]
    assert writeup.answer(tests) == expected


def test_finding_shows_points_ci_adjusted_p_and_confounding(stats):
    rain = writeup.finding(stats["tests"][0], 0.05)
    assert rain.startswith("- **Rain lowers punctuality**: -1.21 pts on-time share")
    assert "(95% CI -1.89 to -0.58)" in rain
    assert "Holm-adjusted p = 0.001," in rain
    assert "611 adverse route-months" in rain
    # 1 - (-0.0121 / -0.0187) = 35% of the naive gap was route/month mix
    assert "removes 35% of it" in rain


def test_finding_for_a_non_significant_factor(stats):
    wind = writeup.finding(stats["tests"][1], 0.05)
    assert wind.startswith("- **Wind: no significant effect** at alpha = 0.05: -0.34 pts")


def test_finding_not_testable():
    text = writeup.finding({"factor": "frost", "verdict": "not testable", "gap": None}, 0.05)
    assert "not testable" in text and "pts" not in text


@pytest.mark.parametrize(
    ("gap", "naive", "expected"),
    [
        (-0.01, -0.02, 0.5),
        (-0.02, -0.01, 0.0),
        (0.01, -0.02, None),
        (-0.01, 0, None),
        (None, -0.02, None),
    ],
)
def test_confounding_share(gap, naive, expected):
    assert writeup.confounding_share(gap, naive) == expected


def test_render_has_every_section_and_only_drawn_charts(stats, report):
    text = writeup.render(stats, report, MANIFEST, chart_link="results")
    for heading in (
        "# Results",
        "## Effect of each factor",
        "## By season",
        "## Dose-response",
        "## Descriptive view",
        "## Caveats",
    ):
        assert heading in text
    assert "![Effect of each weather factor](results/effects.svg)" in text
    assert "seasons.svg" not in text and "dose.svg" not in text  # not in the manifest
    assert "Not testable (no comparable months): summer frost." in text
    assert "| winter | rain | -1.50 pts | -2.70 to -0.40 | 0.020 | lowers punctuality |" in text
    assert "| rain | dry (baseline) | 86.0% | - | 900 |" in text
    assert "- Rain: 84.0% on time" in text
    assert "1,840 route-months (46 routes x 40 months)" in text


def test_render_is_deterministic(stats, report):
    assert writeup.render(stats, report, MANIFEST) == writeup.render(stats, report, MANIFEST)


def test_render_stats_only_skips_descriptive_section(stats):
    text = writeup.render(stats, None)
    assert "## Descriptive view" not in text and "## Caveats" in text


def test_cli_writes_then_checks(tmp_path, stats):
    charts_dir = tmp_path / "docs" / "results"
    charts_dir.mkdir(parents=True)
    (charts_dir / "charts.json").write_text(json.dumps({"charts": MANIFEST}))
    out = tmp_path / "docs" / "RESULTS.md"
    args = [
        "--stats",
        str(FIXTURES / "weather-stats.json"),
        "--report",
        str(FIXTURES / "weather-report.json"),
        "--charts",
        str(charts_dir / "charts.json"),
        "--out",
        str(out),
    ]

    assert writeup.main([*args, "--summary", str(tmp_path / "s.json")]) == 0
    assert "](results/effects.svg)" in out.read_text()
    assert json.loads((tmp_path / "s.json").read_text()) == {
        "factors": ["rain", "wind", "frost"],
        "significant": ["rain", "frost"],
        "charts": ["effects", "intensity"],
    }
    assert writeup.main([*args, "--check"]) == 0

    out.write_text(out.read_text() + "hand edit\n")
    assert writeup.main([*args, "--check"]) == 1


def test_cli_fails_when_the_tests_reported_errors(tmp_path, stats):
    stats["ok"], stats["errors"] = False, ["frost: no strata"]
    path = tmp_path / "stats.json"
    path.write_text(json.dumps(stats))
    out = tmp_path / "R.md"
    assert writeup.main(["--stats", str(path), "--out", str(out)]) == 1
    assert "- frost: no strata" in out.read_text()


def test_cli_needs_an_input():
    with pytest.raises(SystemExit):
        writeup.main([])
