"""SVG charts from the weather results JSON (fixtures shaped like the real outputs)."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

pytest.importorskip("matplotlib")

from railfall.analysis import charts  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures" / "analysis"


@pytest.fixture
def stats() -> dict:
    return json.loads((FIXTURES / "weather-stats.json").read_text())


@pytest.fixture
def report() -> dict:
    return json.loads((FIXTURES / "weather-report.json").read_text())


def test_renders_all_four_charts(tmp_path, stats, report):
    drawn = charts.render(stats, report, tmp_path)
    assert [c.name for c in drawn] == ["effects", "seasons", "dose", "intensity"]
    for chart in drawn:
        svg = (tmp_path / chart.file).read_text()
        assert svg.lstrip().startswith("<?xml") and "<svg" in svg


def test_points_count_only_drawable_rows(tmp_path, stats, report):
    drawn = {c.name: c.points for c in charts.render(stats, report, tmp_path)}
    # every factor tested; the untestable summer-frost cell and the null frost
    # slope are left out rather than drawn at zero
    assert drawn == {"effects": 3, "seasons": 5, "dose": 2, "intensity": 9}


def test_text_stays_text_and_labels_the_findings(tmp_path, stats, report):
    charts.render(stats, report, tmp_path)
    effects = (tmp_path / "effects.svg").read_text()
    assert "-1.21 pts" in effects and "lowers punctuality" in effects
    intensity = (tmp_path / "intensity.svg").read_text()
    assert "very wet" in intensity and "86.0%" in intensity


def test_output_is_deterministic(tmp_path, stats, report):
    first, second = tmp_path / "a", tmp_path / "b"
    charts.render(stats, report, first)
    charts.render(stats, report, second)
    for name in ("effects", "seasons", "dose", "intensity"):
        assert (first / f"{name}.svg").read_bytes() == (second / f"{name}.svg").read_bytes()


def test_stats_only_skips_intensity(tmp_path, stats):
    assert [c.name for c in charts.render(stats, None, tmp_path)] == ["effects", "seasons", "dose"]


def test_empty_section_removes_a_stale_chart(tmp_path, stats, report):
    charts.render(stats, report, tmp_path)
    stats["dose"] = []
    drawn = charts.render(stats, report, tmp_path)
    assert "dose" not in {c.name for c in drawn}
    assert not (tmp_path / "dose.svg").exists()


def test_cli_writes_manifest(tmp_path):
    out = tmp_path / "results"
    code = charts.main(
        [
            "--stats", str(FIXTURES / "weather-stats.json"),
            "--report", str(FIXTURES / "weather-report.json"),
            "--out-dir", str(out),
        ]
    )  # fmt: skip
    assert code == 0
    manifest = json.loads((out / "charts.json").read_text())
    assert [c["file"] for c in manifest["charts"]] == [
        "effects.svg",
        "seasons.svg",
        "dose.svg",
        "intensity.svg",
    ]
    assert all(c["points"] > 0 for c in manifest["charts"])


def test_cli_fails_when_nothing_to_chart(tmp_path):
    empty = tmp_path / "empty.json"
    shutil.copy(FIXTURES / "weather-stats.json", empty)
    data = json.loads(empty.read_text())
    data.update(tests=[], by_season=[], dose=[])
    empty.write_text(json.dumps(data))
    assert charts.main(["--stats", str(empty), "--out-dir", str(tmp_path / "out")]) == 1


def test_cli_needs_an_input():
    with pytest.raises(SystemExit):
        charts.main([])
