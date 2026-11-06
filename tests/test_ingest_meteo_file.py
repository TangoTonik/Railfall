"""Unit tests for the Météo-France csv.gz contract (railfall.ingest.meteo.validate_file)."""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from railfall.ingest.meteo import REQUIRED_COLUMNS, SchemaError, validate_file

FIXTURE = Path(__file__).parent / "fixtures" / "meteo" / "Q_75_latest-2025-2026_RR-T-Vent.csv.gz"


def _lines() -> list[str]:
    return gzip.decompress(FIXTURE.read_bytes()).decode().splitlines()


def _pack(lines: list[str]) -> bytes:
    return gzip.compress(("\n".join(lines) + "\n").encode())


def _set(line: str, header: str, column: str, value: str) -> str:
    cells = line.split(";")
    cells[header.split(";").index(column)] = value
    return ";".join(cells)


def test_fixture_satisfies_the_contract() -> None:
    result = validate_file("75", FIXTURE.read_bytes(), min_rows=1)
    assert result.department == "75"
    assert result.rows == 30
    assert result.stations == 3
    assert (result.first_day, result.last_day) == ("20250101", "20250110")
    assert set(REQUIRED_COLUMNS) <= set(result.columns)
    assert len(result.columns) == 58


def test_real_floor_applies_by_default() -> None:
    with pytest.raises(SchemaError, match="30 rows, expected at least 1000"):
        validate_file("75", FIXTURE.read_bytes())


def test_header_only_file_is_rejected_even_with_no_floor() -> None:
    with pytest.raises(SchemaError, match="0 rows"):
        validate_file("75", _pack(_lines()[:1]), min_rows=0)


def test_rain_gauge_only_stations_with_empty_measures_are_valid() -> None:
    # PARIS-LUXEMBOURG only measures RR: TN/TX/FFM/... are empty, not an error.
    rows = [line for line in _lines() if line.startswith("75107005")]
    assert rows and all(";;;" in row for row in rows)
    assert validate_file("75", _pack([_lines()[0], *rows]), min_rows=1).stations == 1


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"<html>maintenance</html>", "not a complete gzip file"),
        (gzip.compress(b"NUM_POSTE;x\n")[:-6], "not a complete gzip file"),
        (gzip.compress(b"NUM_POSTE,NOM_USUEL,LAT\n1,2,3\n"), "missing columns"),
    ],
)
def test_garbage_payloads_are_rejected(payload: bytes, message: str) -> None:
    with pytest.raises(SchemaError, match=message):
        validate_file("75", payload, min_rows=1)


def test_file_from_another_department_is_rejected() -> None:
    with pytest.raises(SchemaError, match="NUM_POSTE='75114001' not in 13"):
        validate_file("13", FIXTURE.read_bytes(), min_rows=1)


def test_corsican_files_accept_20_station_ids() -> None:
    header, *rows = _lines()
    rows = [_set(row, header, "NUM_POSTE", "20004002") for row in rows]
    assert validate_file("2A", _pack([header, *rows]), min_rows=1).stations == 1


def test_bad_values_are_reported_with_line_numbers() -> None:
    header, *rows = _lines()
    rows[0] = _set(rows[0], header, "AAAAMMJJ", "2025-01-01")
    rows[1] = _set(rows[1], header, "RR", "n/a")
    rows[2] = _set(rows[2], header, "LAT", "")
    rows[3] = rows[3].rsplit(";", 5)[0]
    with pytest.raises(SchemaError) as excinfo:
        validate_file("75", _pack([header, *rows]), min_rows=1)
    msg = str(excinfo.value)
    assert "line 2: bad AAAAMMJJ='2025-01-01'" in msg
    assert "line 3: bad RR='n/a'" in msg
    assert "line 4: bad LAT=''" in msg
    assert "line 5: 53 fields, expected 58" in msg


def test_error_report_is_capped() -> None:
    header, *rows = _lines()
    rows = [_set(row, header, "RR", "x") for row in rows]
    with pytest.raises(SchemaError) as excinfo:
        validate_file("75", _pack([header, *rows]), min_rows=1)
    assert str(excinfo.value).count("bad RR") == 10
