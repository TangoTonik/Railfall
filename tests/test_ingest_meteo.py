"""Unit tests for railfall.ingest.meteo against data.gouv.fr / Météo-France fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from railfall.ingest.meteo import (
    DepartmentFile,
    ResolveError,
    dataset_url,
    normalize_department,
    parse_catalogue,
    select_files,
    station_prefix,
)

FIXTURES = Path(__file__).parent / "fixtures" / "meteo"


def _catalogue() -> list[DepartmentFile]:
    return parse_catalogue((FIXTURES / "catalogue_sample.json").read_bytes())


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("75", "75"), (6, "06"), ("6", "06"), (" 2a ", "2A"), ("2B", "2B"), ("974", "974")],
)
def test_department_codes_are_normalized(raw, expected) -> None:
    assert normalize_department(raw) == expected


@pytest.mark.parametrize("raw", ["20", "00", "96", "977", "ABC", "", "7501"])
def test_invalid_department_codes_are_rejected(raw) -> None:
    with pytest.raises(ValueError, match="not a French department code"):
        normalize_department(raw)


def test_station_prefix_maps_corsica_to_20() -> None:
    assert station_prefix("2A") == "20"
    assert station_prefix("2b") == "20"
    assert station_prefix(6) == "06"
    assert station_prefix("971") == "971"


def test_dataset_url_targets_the_datagouv_api() -> None:
    assert dataset_url() == (
        "https://www.data.gouv.fr/api/1/datasets/donnees-climatologiques-de-base-quotidiennes/"
    )


def test_catalogue_keeps_only_rr_t_vent_files() -> None:
    files = _catalogue()
    assert [(f.department, f.period) for f in files] == [
        ("13", "previous"),
        ("13", "latest"),
        ("2A", "previous"),
        ("2A", "latest"),
        ("75", "previous"),
        ("75", "latest"),
    ]
    assert all(f.url.endswith("_RR-T-Vent.csv.gz") for f in files)


def test_catalogue_prefers_the_newest_file_during_rollover() -> None:
    latest_13 = next(f for f in _catalogue() if (f.department, f.period) == ("13", "latest"))
    assert (latest_13.first_year, latest_13.last_year) == (2025, 2026)
    assert latest_13.name == "daily_13_latest"


@pytest.mark.parametrize(
    ("payload", "message"),
    [(b"<html>502</html>", "not valid JSON"), (b'{"message": "Not found"}', "no 'resources'")],
)
def test_catalogue_rejects_unexpected_answers(payload: bytes, message: str) -> None:
    with pytest.raises(ResolveError, match=message):
        parse_catalogue(payload)


def test_select_files_normalizes_and_deduplicates_departments() -> None:
    selected = select_files(_catalogue(), ["75", "2a", 75], periods=["latest"])
    assert [f.name for f in selected] == ["daily_75_latest", "daily_2A_latest"]


def test_select_files_defaults_to_both_periods() -> None:
    selected = select_files(_catalogue(), ["13"])
    assert [f.period for f in selected] == ["previous", "latest"]


def test_select_files_reports_every_missing_file() -> None:
    with pytest.raises(ResolveError, match="69/previous, 69/latest"):
        select_files(_catalogue(), ["75", "69"])


def test_select_files_rejects_unknown_periods() -> None:
    with pytest.raises(ValueError, match="unknown period"):
        select_files(_catalogue(), ["75"], periods=["archive"])
