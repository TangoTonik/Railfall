"""Unit tests for railfall.ingest.sncf against small, realistic export fixtures."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from railfall.ingest.sncf import (
    DATASETS,
    PUNCTUALITY,
    STATIONS,
    SchemaError,
    validate_export,
)

FIXTURES = Path(__file__).parent / "fixtures" / "sncf"


def _load(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def test_export_urls_point_at_the_opendatasoft_json_export() -> None:
    assert PUNCTUALITY.export_url() == (
        "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/"
        "regularite-mensuelle-tgv-aqst/exports/json"
    )
    assert STATIONS.export_url("csv").endswith("/gares-de-voyageurs/exports/csv")
    assert set(DATASETS) == {"punctuality", "stations"}


@pytest.mark.parametrize(
    ("dataset", "fixture", "rows"),
    [(PUNCTUALITY, "punctuality_sample.json", 4), (STATIONS, "stations_sample.json", 5)],
)
def test_fixtures_satisfy_the_contract(dataset, fixture, rows) -> None:
    result = validate_export(dataset, _load(fixture), min_rows=1)
    assert result.dataset == dataset.name
    assert result.rows == rows
    assert set(dataset.required) <= set(result.fields)


def test_real_floor_applies_by_default() -> None:
    with pytest.raises(SchemaError, match="4 rows, expected at least 5000"):
        validate_export(PUNCTUALITY, _load("punctuality_sample.json"))


def test_empty_export_is_rejected_even_with_no_floor() -> None:
    with pytest.raises(SchemaError, match="0 rows"):
        validate_export(STATIONS, b"[]", min_rows=0)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"<html>maintenance</html>", "not valid JSON"),
        (b'{"error_code": "ODSQLError"}', "expected a JSON array, got dict"),
        (b"[1]", "row 0: not an object"),
    ],
)
def test_garbage_payloads_are_rejected(payload: bytes, message: str) -> None:
    with pytest.raises(SchemaError, match=message):
        validate_export(PUNCTUALITY, payload, min_rows=1)


def test_missing_and_malformed_fields_are_reported_with_row_numbers() -> None:
    records = json.loads(_load("punctuality_sample.json"))
    del records[1]["gare_arrivee"]
    records[2]["date"] = "2024-13"
    records[3]["nb_annulation"] = -1
    with pytest.raises(SchemaError) as excinfo:
        validate_export(PUNCTUALITY, json.dumps(records).encode(), min_rows=1)
    msg = str(excinfo.value)
    assert "row 1: missing 'gare_arrivee'" in msg
    assert "row 2: bad date='2024-13'" in msg
    assert "row 3: bad nb_annulation=-1" in msg


def test_boolean_is_not_a_count() -> None:
    records = json.loads(_load("punctuality_sample.json"))
    records[0]["nb_train_prevu"] = True
    with pytest.raises(SchemaError, match="row 0: bad nb_train_prevu=True"):
        validate_export(PUNCTUALITY, json.dumps(records).encode(), min_rows=1)


def test_station_without_coordinates_is_allowed_but_bad_point_is_not() -> None:
    records = json.loads(_load("stations_sample.json"))
    records[0]["position_geographique"] = {"lat": "48.8"}
    with pytest.raises(SchemaError, match="row 0: bad position_geographique"):
        validate_export(STATIONS, json.dumps(records).encode(), min_rows=1)


def test_negative_average_delay_is_valid_in_bronze() -> None:
    # Regression: the real AQST export has routes whose mean arrival delay is
    # negative (trains early on average), e.g. -30.5125 and -40.10925926.
    records = json.loads(_load("punctuality_sample.json"))
    records[0]["retard_moyen_arrivee"] = -30.5125
    records[1]["retard_moyen_arrivee"] = None
    result = validate_export(PUNCTUALITY, json.dumps(records).encode(), min_rows=1)
    assert result.rows == len(records)


@pytest.mark.parametrize("value", [True, "12.5", float("nan"), float("inf")])
def test_average_delay_must_be_a_finite_number(value) -> None:
    records = json.loads(_load("punctuality_sample.json"))
    records[0]["retard_moyen_arrivee"] = value
    with pytest.raises(SchemaError, match="row 0: bad retard_moyen_arrivee"):
        validate_export(PUNCTUALITY, json.dumps(records).encode(), min_rows=1)
