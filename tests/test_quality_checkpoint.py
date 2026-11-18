"""The GX checkpoint runs the raw suites end to end on a real (SQLite) database.

SQLite has no schemas, so tables are created unqualified and `schema=None`;
everything else (datasource, table assets, suites, checkpoint, result parsing)
is the code path the DAG runs against Postgres.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from railfall.quality import checkpoint
from railfall.quality.suites import SUITES, between, not_empty, not_null, unique_key

pytest.importorskip("great_expectations")

from quality_db import STATIONS, WEATHER_MONTHLY, sqlite_db  # noqa: E402


def _db(tmp_path, stations=STATIONS, weather=WEATHER_MONTHLY) -> str:
    return sqlite_db(tmp_path, stations, weather)


def _run(url, tables=("stations", "weather_monthly"), **kwargs):
    return {r.table: r for r in checkpoint.run(url, tables, schema=None, **kwargs)}


def test_clean_tables_pass_every_expectation(tmp_path):
    reports = _run(_db(tmp_path))
    assert set(reports) == {"stations", "weather_monthly"}
    for table, report in reports.items():
        assert report.ok, report.failures
        assert report.evaluated == len(SUITES[table])


def test_broken_rows_are_reported_per_expectation(tmp_path):
    stations = [*STATIONS, STATIONS[0], ("12345", "Nowhere", "NOWHERE", "XX", 48.0, 2.0)]
    weather = [
        *WEATHER_MONTHLY[:2],
        # More heavy-rain days than rain days, and a -80 °C month.
        ("2A", "2024-01-01", 2024, 2, 92.5, 2.0, 3.0, 9.4, -80.0, 0.0, 5.0, 25.0, 6.0),
    ]
    reports = _run(_db(tmp_path, stations, weather))

    stations_failed = {(f.expectation, f.column) for f in reports["stations"].failures}
    assert not reports["stations"].ok
    assert ("expect_column_values_to_be_unique", "uic_code") in stations_failed
    assert ("expect_column_values_to_match_regex", "uic_code") in stations_failed
    assert ("expect_column_values_to_match_regex", "department") in stations_failed

    weather_failed = {(f.expectation, f.column) for f in reports["weather_monthly"].failures}
    assert ("expect_column_values_to_be_between", "temp_min_c") in weather_failed
    assert ("expect_column_pair_values_a_to_be_greater_than_b", "rain_days") in weather_failed
    failures = reports["weather_monthly"].failures
    (minimum,) = [f for f in failures if f.column == "temp_min_c"]
    assert minimum.unexpected_count == 1


def test_empty_table_fails_the_row_count(tmp_path):
    reports = _run(_db(tmp_path, stations=[]), tables=("stations",))
    assert [f.expectation for f in reports["stations"].failures] == [
        "expect_table_row_count_to_be_between"
    ]


def test_custom_suites_and_missing_column_surface_as_failures(tmp_path):
    suites = {"stations": (not_empty(), not_null("uic_code"), between("altitude", 0, 4000))}
    reports = _run(_db(tmp_path), tables=("stations",), suites=suites)
    (failure,) = reports["stations"].failures
    assert failure.column == "altitude"
    assert reports["stations"].evaluated == 3


def test_unknown_table_is_refused_before_connecting():
    with pytest.raises(ValueError, match="no expectation suite for: nope"):
        checkpoint.run("sqlite:///unused.db", ["nope"])


def test_unique_key_uses_the_compound_form_for_several_columns():
    assert unique_key("a").kind == "ExpectColumnValuesToBeUnique"
    assert unique_key("a", "b").kind == "ExpectCompoundColumnsToBeUnique"


@pytest.mark.parametrize(
    ("dsn", "url"),
    [
        ("postgresql://u:p@h:5432/db", "postgresql+psycopg2://u:p@h:5432/db"),
        ("postgres://u@h/db", "postgresql+psycopg2://u@h/db"),
        ("sqlite:///x.db", "sqlite:///x.db"),
    ],
)
def test_sqlalchemy_url(dsn, url):
    assert checkpoint.sqlalchemy_url(dsn) == url


def test_format_lists_failures_under_their_table():
    report = checkpoint.TableReport(
        "stations",
        ok=False,
        evaluated=7,
        failures=[checkpoint.Failure("expect_column_values_to_be_unique", "uic_code", 2)],
    )
    text = checkpoint._format([report])
    assert text.splitlines()[0].split() == ["stations", "FAILED", "6/7"]
    assert "expect_column_values_to_be_unique on uic_code: unexpected=2" in text


def test_main_writes_the_report_and_exits_on_failures(tmp_path, capsys):
    url = _db(tmp_path)
    report = tmp_path / "quality.json"
    code = checkpoint.main(["--url", url, "--table", "stations", "--report", str(report)])
    assert code == 0
    payload = json.loads(report.read_text())
    assert [r["table"] for r in payload] == ["stations"]
    assert payload[0]["ok"] and payload[0]["evaluated"] == len(SUITES["stations"])
    assert capsys.readouterr().out.split()[:2] == ["stations", "ok"]

    broken = _db(tmp_path / "broken", stations=[*STATIONS, STATIONS[0]])
    assert checkpoint.main(["--url", broken, "--table", "stations", "--json"]) == 1
    printed = json.loads(capsys.readouterr().out)
    assert not printed[0]["ok"] and printed[0]["failures"]


def test_a_suite_can_validate_a_copy_under_another_table_name(tmp_path):
    url = _db(tmp_path)
    path = url.removeprefix("sqlite:///")
    with sqlite3.connect(path) as conn:
        conn.execute("create table stations_copy as select * from stations")
        conn.execute("update stations_copy set department = 'XX'")
    reports = _run(url, tables=("stations",), table_names={"stations": "stations_copy"})
    assert not reports["stations"].ok
    assert {f.column for f in reports["stations"].failures} == {"department"}
    # The original table is untouched and still passes under the same suite.
    assert _run(url, tables=("stations",))["stations"].ok
