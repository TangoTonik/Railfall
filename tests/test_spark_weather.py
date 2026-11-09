"""Unit tests for the Météo-France bronze -> silver transform (local Spark)."""

from __future__ import annotations

import csv
import gzip
import io
import shutil
from datetime import date
from pathlib import Path

import pytest

from railfall.spark import weather as w

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parent / "fixtures" / "meteo" / "Q_75_latest-2025-2026_RR-T-Vent.csv.gz"


def _fixture_rows() -> tuple[list[str], list[list[str]]]:
    text = gzip.decompress(FIXTURE.read_bytes()).decode()
    header, *rows = list(csv.reader(io.StringIO(text), delimiter=";"))
    return header, rows


def _land(root: Path, dataset: str, ingest_date: str, header, rows) -> None:
    """Write rows where bronze would put them for `dataset` and `ingest_date`."""
    folder = root / "bronze" / "meteo" / dataset / f"ingest_date={ingest_date}"
    folder.mkdir(parents=True, exist_ok=True)
    buffer = io.StringIO()
    csv.writer(buffer, delimiter=";", lineterminator="\n").writerows([header, *rows])
    (folder / f"{dataset}.csv.gz").write_bytes(gzip.compress(buffer.getvalue().encode()))


def _set(header: list[str], row: list[str], **values: str) -> list[str]:
    row = list(row)
    for column, value in values.items():
        row[header.index(column)] = value
    return row


def _silver(spark, root: Path) -> w.Split:
    return w.to_silver(w.read_bronze(spark, w.bronze_glob(str(root))))


def test_fixture_becomes_typed_station_days(spark, tmp_path) -> None:
    folder = tmp_path / "bronze/meteo/daily_75_latest/ingest_date=2026-11-06"
    folder.mkdir(parents=True)
    shutil.copy(FIXTURE, folder / "daily_75_latest.csv.gz")
    header, rows = _fixture_rows()

    split = _silver(spark, tmp_path)
    valid = split.valid

    assert split.rejected.count() == 0
    assert valid.count() == len(rows)
    types = dict(valid.dtypes)
    assert types["day"] == "date"
    assert types["precipitation_mm"] == "double"
    assert types["station_id"] == "string"
    first = valid.where("station_id = '75114001' AND day = '2025-01-02'").first()
    assert first.department == "75"
    assert first.period == "latest"
    assert first.ingest_date == date(2026, 11, 6)
    assert first.month == date(2025, 1, 1)
    assert first.precipitation_mm == 1.6
    assert (first.temp_min_c, first.temp_max_c) == (5.8, 12.2)
    # Rain-only stations have empty temperature columns: null, not 0.
    luxembourg = valid.where("station_id = '75107005'").first()
    assert luxembourg.temp_mean_c is None


def test_doubtful_and_impossible_values_are_nulled_one_by_one(spark, tmp_path) -> None:
    header, rows = _fixture_rows()
    base = rows[0]
    doubtful = _set(header, base, AAAAMMJJ="20250201", RR="12.0", QRR="2")
    negative = _set(header, base, AAAAMMJJ="20250202", RR="-3", FFM="250")
    inverted = _set(header, base, AAAAMMJJ="20250203", TN="9.0", TX="4.0")
    _land(tmp_path, "daily_75_latest", "2026-11-06", header, [doubtful, negative, inverted])

    days = {r.day.day: r for r in _silver(spark, tmp_path).valid.collect()}

    assert days[1].precipitation_mm is None and days[1].temp_max_c is not None
    assert days[2].precipitation_mm is None and days[2].wind_mean_ms is None
    assert days[2].temp_min_c is not None
    assert (days[3].temp_min_c, days[3].temp_max_c) == (None, None)
    assert days[3].temp_mean_c is not None


def test_latest_wins_over_previous_and_newest_ingest_wins(spark, tmp_path) -> None:
    header, rows = _fixture_rows()
    row = rows[0]
    _land(tmp_path, "daily_75_previous", "2026-11-06", header, [_set(header, row, RR="1")])
    _land(tmp_path, "daily_75_latest", "2026-11-05", header, [_set(header, row, RR="2")])
    _land(tmp_path, "daily_75_latest", "2026-11-06", header, [_set(header, row, RR="3")])

    valid = _silver(spark, tmp_path).valid

    assert valid.count() == 1
    kept = valid.first()
    assert (kept.precipitation_mm, kept.period, kept.ingest_date) == (
        3.0,
        "latest",
        date(2026, 11, 6),
    )


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"AAAAMMJJ": "20251301"}, "bad_day"),
        ({"NUM_POSTE": "13055001"}, "station_outside_department"),
        ({"NUM_POSTE": "7511"}, "bad_station"),
    ],
)
def test_untrustworthy_keys_are_quarantined(spark, tmp_path, change, reason) -> None:
    header, rows = _fixture_rows()
    _land(
        tmp_path,
        "daily_75_latest",
        "2026-11-06",
        header,
        [rows[0], _set(header, rows[1], **change)],
    )

    split = _silver(spark, tmp_path)

    assert split.valid.count() == 1
    assert [r.reject_reason for r in split.rejected.collect()] == [reason]


def test_corsican_stations_keep_their_20_prefix(spark, tmp_path) -> None:
    header, rows = _fixture_rows()
    corsica = _set(header, rows[0], NUM_POSTE="20004002", NOM_USUEL="AJACCIO")
    _land(tmp_path, "daily_2A_latest", "2026-11-06", header, [corsica])

    split = _silver(spark, tmp_path)

    assert split.rejected.count() == 0
    assert split.valid.first().department == "2A"


def test_missing_required_column_fails_loudly(spark, tmp_path) -> None:
    header, rows = _fixture_rows()
    drop = header.index("FXY")
    _land(
        tmp_path,
        "daily_75_latest",
        "2026-11-06",
        [c for i, c in enumerate(header) if i != drop],
        [[v for i, v in enumerate(r) if i != drop] for r in rows[:2]],
    )
    with pytest.raises(ValueError, match="FXY"):
        w.read_bronze(spark, w.bronze_glob(str(tmp_path)))
