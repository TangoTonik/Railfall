"""Unit tests for the stations bronze -> silver transform (local Spark)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from railfall.spark import stations as s

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parent / "fixtures" / "sncf" / "stations_sample.json"


def _records() -> list[dict]:
    return json.loads(FIXTURE.read_text())


def _split(spark, tmp_path, records: list[dict]) -> s.Split:
    path = tmp_path / "stations.json"
    path.write_text(json.dumps(records, ensure_ascii=False))
    return s.to_silver(s.read_bronze(spark, str(path)))


def test_fixture_becomes_typed_silver(spark, tmp_path) -> None:
    split = _split(spark, tmp_path, _records())
    rows = {r.uic_code: r for r in split.valid.collect()}

    assert split.rejected.count() == 0
    assert len(rows) == 5
    pld = rows["87686006"]
    assert pld.department == "75"
    assert pld.name_key == "PARIS GARE DE LYON"
    assert pld.latitude == pytest.approx(48.8443)
    assert dict(split.valid.dtypes)["longitude"] == "double"
    # No coordinates is fine: the department still links it to the weather.
    assert rows["87743146"].latitude is None
    assert rows["87743146"].department == "01"


def test_name_key_folds_accents_and_punctuation(spark, tmp_path) -> None:
    rec = {**_records()[0], "nom": " Saint-Étienne  Châteaucreux ", "codes_uic": "87726000"}
    row = _split(spark, tmp_path, [rec]).valid.first()

    assert row.name_key == "SAINT ETIENNE CHATEAUCREUX"
    assert row.station_name == "Saint-Étienne  Châteaucreux"


@pytest.mark.parametrize(
    ("insee", "department"), [("2A004", "2A"), ("2b033", "2B"), ("97411", "974")]
)
def test_department_handles_corsica_and_overseas(spark, tmp_path, insee, department) -> None:
    rec = {**_records()[0], "codeinsee": insee}
    assert _split(spark, tmp_path, [rec]).valid.first().department == department


def test_multiple_uic_codes_become_rows_and_duplicates_collapse(spark, tmp_path) -> None:
    first, other = _records()[0], _records()[1]
    shared = {**first, "codes_uic": "87686006; 87686667"}
    no_coords = {**first, "position_geographique": None}
    valid = _split(spark, tmp_path, [no_coords, shared, other]).valid

    codes = sorted(r.uic_code for r in valid.collect())
    assert codes == ["87686006", "87686667", "87751008"]
    kept = valid.where("uic_code = '87686006'").first()
    assert kept.latitude is not None


def test_unusable_rows_are_rejected_with_a_reason(spark, tmp_path) -> None:
    base = _records()[0]
    bad = [
        {**base, "nom": "  "},
        {**base, "codes_uic": "1234"},
        {**base, "codes_uic": None},
        {**base, "codeinsee": "7511"},
    ]
    split = _split(spark, tmp_path, bad)

    reasons = sorted(r.reject_reason for r in split.rejected.collect())
    assert reasons == ["bad_insee_code", "bad_uic_code", "bad_uic_code", "missing_name"]
    assert split.valid.count() == 0


def test_out_of_range_coordinates_are_nulled(spark, tmp_path) -> None:
    rec = {**_records()[0], "position_geographique": {"lon": 2.37, "lat": 148.8}}
    row = _split(spark, tmp_path, [rec]).valid.first()
    assert row.latitude is None and row.longitude is None
