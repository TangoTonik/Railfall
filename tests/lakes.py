"""Local lakes (bronze files under a temp dir) shared by the silver job tests."""

from __future__ import annotations

import calendar
import gzip
import shutil
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"


def local_lake(root: Path, day: str = "2026-11-09") -> None:
    files = {
        f"bronze/sncf/punctuality/ingest_date={day}/punctuality.json": FIXTURES
        / "sncf"
        / "punctuality_sample.json",
        f"bronze/sncf/stations/ingest_date={day}/stations.json": FIXTURES
        / "sncf"
        / "stations_sample.json",
        f"bronze/meteo/daily_75_latest/ingest_date={day}/daily_75_latest.csv.gz": FIXTURES
        / "meteo"
        / "Q_75_latest-2025-2026_RR-T-Vent.csv.gz",
    }
    for key, src in files.items():
        dst = root / key
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dst)


METEO_HEADER = (
    "NUM_POSTE;NOM_USUEL;LAT;LON;ALTI;AAAAMMJJ;RR;QRR;TN;QTN;TX;QTX;TM;QTM;FFM;QFFM;FXY;QFXY"
)


def full_months(root: Path, department: str, station: str, months: list[tuple[int, int]]) -> None:
    """Bronze Météo-France file covering every day of `months` for one station.

    The shared fixture only holds ten days of January 2025, too few for a
    department-month; the derived tables need fully covered months that overlap
    the punctuality fixture (January / February 2024).
    """
    lines = [METEO_HEADER]
    for year, month in months:
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            rain = 12.0 if day % 7 == 0 else (2.0 if day % 3 == 0 else 0.0)
            tmin = -1.5 if day % 5 == 0 else 3.0
            lines.append(
                f"{station};TEST;48.8;2.3;50;{year}{month:02d}{day:02d};"
                f"{rain};1;{tmin};1;9.0;1;5.0;1;4.2;1;11.5;1"
            )
    key = f"bronze/meteo/daily_{department}_previous/ingest_date=2026-11-09/daily.csv.gz"
    dst = root / key
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode()))


def derived_lake(root: Path) -> None:
    local_lake(root)
    full_months(root, "75", "75114001", [(2024, 1), (2024, 2)])
    full_months(root, "13", "13055001", [(2024, 1)])
