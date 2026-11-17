"""railfall.transform.report: the weather-impact marts rendered from fake rows."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from railfall.transform import report as tr

BINARY_COLS = (
    "weather_factor", "exposure", "route_months", "trains_run", "arrival_punctuality_rate",
    "cancellation_rate", "avg_delay_arrival_min", "punctuality_gap_pts", "delay_gap_min",
)  # fmt: skip
BINARY = [
    ("frost", "adverse", 120, 52_000, Decimal("0.8012"), Decimal("0.031"), Decimal("41.2"),
     Decimal("-6.35"), Decimal("4.10")),
    ("frost", "normal", 2_480, 1_210_000, Decimal("0.8647"), Decimal("0.012"), Decimal("37.1"),
     None, None),
    ("rain", "adverse", 610, 280_000, Decimal("0.8423"), Decimal("0.015"), Decimal("38.9"),
     Decimal("-2.21"), Decimal("1.60")),
    ("rain", "normal", 1_990, 982_000, Decimal("0.8644"), Decimal("0.013"), Decimal("37.3"),
     None, None),
    ("wind", "adverse", 300, 140_000, Decimal("0.8551"), Decimal("0.019"), Decimal("38.0"),
     Decimal("-0.92"), Decimal("0.70")),
    ("wind", "normal", 2_300, 1_122_000, Decimal("0.8643"), Decimal("0.013"), Decimal("37.3"),
     None, None),
]  # fmt: skip

INTENSITY_COLS = (
    "weather_factor", "intensity_band", "band_order", "is_baseline", "route_months",
    "arrival_punctuality_rate", "avg_delay_arrival_min", "punctuality_gap_pts",
)  # fmt: skip
INTENSITY = [
    ("frost", "none", 1, True, 1_900, Decimal("0.8701"), Decimal("36.8"), None),
    ("frost", "light", 2, False, 500, Decimal("0.8550"), Decimal("37.9"), Decimal("-1.51")),
    ("frost", "hard", 4, False, 110, Decimal("0.8030"), Decimal("41.0"), Decimal("-6.71")),
    ("rain", "normal", 2, True, 1_700, Decimal("0.8650"), Decimal("37.2"), None),
    ("rain", "extreme", 5, False, 40, Decimal("0.8100"), Decimal("40.5"), Decimal("-5.50")),
]  # fmt: skip

REGIONAL_COLS = (
    "region_name", "season", "weather_factor", "route_months", "years",
    "arrival_punctuality_rate", "punctuality_gap_pts",
)  # fmt: skip
REGIONAL = [
    ("Grand Est", "winter", "frost", 60, 3, Decimal("0.7710"), Decimal("-9.80")),
    ("Bretagne", "autumn", "rain", 45, 3, Decimal("0.8205"), Decimal("-4.40")),
    ("Occitanie", "winter", "frost", 35, 2, Decimal("0.8100"), Decimal("-3.10")),
    ("Normandie", "winter", "frost", 31, 2, Decimal("0.8300"), Decimal("-2.00")),
]  # fmt: skip


class FakeCursor:
    def __init__(self, cols, rows):
        self.description = [(c,) for c in cols]
        self._rows = rows

    def fetchall(self):
        return list(self._rows)


class FakeConn:
    def __init__(self, binary=BINARY, intensity=INTENSITY, regional=REGIONAL):
        self.tables = {
            "mart_punctuality_by_weather\n": (BINARY_COLS, binary),
            "mart_punctuality_by_weather_intensity": (INTENSITY_COLS, intensity),
            "mart_punctuality_by_region_season": (REGIONAL_COLS, regional),
        }
        self.params: list = []

    def execute(self, query, params=None):
        self.params.append(params)
        for marker, (cols, rows) in self.tables.items():
            if marker in query:
                return FakeCursor(cols, rows)
        raise AssertionError(f"unexpected query: {query}")


def test_reader_dsn_uses_the_reader_role_on_the_dbt_database():
    dsn = tr.reader_dsn(
        {"DBT_HOST": "postgres", "POSTGRES_PORT": "5433", "POSTGRES_DB": "wh",
         "TF_VAR_warehouse_reader_password": "p@ss/word"}
    )  # fmt: skip
    assert dsn == "postgresql://railfall_reader:p%40ss%2Fword@postgres:5433/wh"
    assert tr.reader_dsn({}).startswith("postgresql://railfall_reader:reader-local@localhost:5432/")


def test_collect_reads_all_three_marts_as_plain_values():
    conn = FakeConn()
    report = tr.collect(conn, min_route_months=30, top=5)
    assert report.ok
    assert len(report.binary) == 6 and len(report.intensity) == 5 and len(report.regional) == 4
    assert isinstance(report.binary[0]["arrival_punctuality_rate"], float)
    assert conn.params[-1] == (30,)
    json.dumps(report.to_dict())  # serialisable without a default=


def test_worst_per_factor_keeps_the_first_n_of_each_factor():
    rows = [dict(zip(REGIONAL_COLS, r, strict=True)) for r in REGIONAL]
    kept = tr.worst_per_factor(rows, top=2)
    assert [(r["weather_factor"], r["region_name"]) for r in kept] == [
        ("frost", "Grand Est"),
        ("rain", "Bretagne"),
        ("frost", "Occitanie"),
    ]


def test_headlines_compare_adverse_with_normal_in_factor_order():
    report = tr.collect(FakeConn())
    lines = tr.headlines(report.binary)
    assert [line.split(":")[0] for line in lines] == ["Rain", "Wind", "Frost"]
    assert lines[2] == (
        "Frost: 80.1% on time when it hits vs 86.5% otherwise "
        "(-6.3 pts, 120 vs 2,480 route-months)."
    )


def test_headline_says_when_one_exposure_is_missing():
    lines = tr.headlines([{"weather_factor": "frost", "exposure": "normal"}])
    assert lines == ["Frost: no adverse route-months to compare with."]


def test_markdown_has_headlines_dose_response_and_regional_tables():
    md = tr.render_markdown(tr.collect(FakeConn(), min_route_months=30))
    assert md.startswith("# Weather and TGV punctuality\n")
    assert "| none (baseline) | 1,900 | 87.0% | 36.8 min | — |" in md
    assert "| hard | 110 | 80.3% | 41.0 min | -6.7 pts |" in md
    # Wind has no intensity rows here, so no empty table is rendered for it.
    assert "**Wind**" not in md
    assert "at least 30 adverse route-months" in md
    assert "| frost | Grand Est | winter | 60 | 77.1% | -9.8 pts |" in md
    assert "## Problems" not in md


def test_empty_marts_are_errors_and_listed_in_the_report():
    report = tr.collect(FakeConn(binary=[], intensity=[], regional=[]))
    assert not report.ok
    assert report.errors == [
        "marts.mart_punctuality_by_weather is empty",
        "marts.mart_punctuality_by_weather_intensity is empty",
    ]
    md = tr.render_markdown(report)
    assert "_No weather comparison yet._" in md
    assert "_No region-season cell has enough route-months yet._" in md
    assert "- marts.mart_punctuality_by_weather is empty" in md


def test_a_missing_factor_is_an_error():
    report = tr.collect(FakeConn(binary=[r for r in BINARY if r[0] != "wind"]))
    assert report.errors == ["no weather comparison for wind"]


@pytest.fixture
def fake_connect(monkeypatch):
    def install(conn):
        class Ctx:
            def __enter__(self):
                return conn

            def __exit__(self, *exc):
                return False

        monkeypatch.setattr(tr, "_connect", lambda dsn: Ctx())

    return install


def test_main_writes_markdown_and_json(tmp_path, fake_connect, capsys):
    fake_connect(FakeConn())
    out, rep = tmp_path / "weather.md", tmp_path / "weather.json"
    assert tr.main(["--out", str(out), "--report", str(rep), "--top", "1"]) == 0
    assert capsys.readouterr().out == ""
    assert out.read_text().startswith("# Weather and TGV punctuality")
    data = json.loads(rep.read_text())
    assert data["ok"] is True
    assert [r["region_name"] for r in data["regional"]] == ["Grand Est", "Bretagne"]
    assert len(data["headlines"]) == 3


def test_main_fails_on_empty_marts(fake_connect, capsys):
    fake_connect(FakeConn(binary=[], intensity=[], regional=[]))
    assert tr.main([]) == 1
    captured = capsys.readouterr()
    assert "# Weather and TGV punctuality" in captured.out
    assert "error: marts.mart_punctuality_by_weather is empty" in captured.err
