"""Météo-France daily climatological data (BASE/QUOT), one file set per department.

Météo-France publishes the "Données climatologiques de base - quotidiennes" on
data.gouv.fr as gzipped, semicolon-separated CSV files, one per department and
period, e.g.::

    Q_75_previous-1950-2024_RR-T-Vent.csv.gz   # long history, republished yearly
    Q_75_latest-2025-2026_RR-T-Vent.csv.gz     # current + previous year, refreshed daily

The period boundaries in the file names move every January, so URLs are never
hard-coded: they are resolved from the data.gouv.fr dataset API, which lists every
resource with its URL. We only use the `RR-T-Vent` files (precipitation,
temperature, wind); the `autres-parametres` files are out of scope.

As for SNCF, bronze keeps the payload byte-for-byte; this module checks that a
downloaded file is the department table we asked for before it is landed.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import math
import re
import zlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from railfall.ingest.http import build_url

DATAGOUV_API = "https://www.data.gouv.fr/api/1"
DATASET_SLUG = "donnees-climatologiques-de-base-quotidiennes"

PERIODS = ("previous", "latest")

_FILE = re.compile(
    r"Q_(?P<dept>\d{2}|2[AB]|97\d)_(?P<period>previous|latest)-(?P<start>\d{4})-(?P<end>\d{4})"
    r"_RR-T-Vent\.csv\.gz$"
)
_DEPT = re.compile(r"^(0[1-9]|1\d|2[1-9]|[3-8]\d|9[0-5]|2[AB]|97[1-6])$")


class SchemaError(ValueError):
    """The file does not look like the department table we asked for."""


class ResolveError(LookupError):
    """The data.gouv.fr catalogue has no file for the requested department/period."""


def normalize_department(code: str | int) -> str:
    """Return the canonical INSEE department code ('6' -> '06', '2a' -> '2A').

    Corsica has no '20' department: it is split into 2A and 2B.
    """
    raw = str(code).strip().upper()
    if raw.isdigit() and len(raw) == 1:
        raw = f"0{raw}"
    if not _DEPT.match(raw):
        raise ValueError(f"not a French department code: {code!r}")
    return raw


def station_prefix(department: str) -> str:
    """The prefix every NUM_POSTE of `department` starts with.

    Station ids are 8 digits: the department number (3 digits overseas), then a
    commune and a station number. Corsican stations still use '20'.
    """
    department = normalize_department(department)
    return "20" if department in {"2A", "2B"} else department


def dataset_url() -> str:
    return build_url(f"{DATAGOUV_API}/datasets/{DATASET_SLUG}/")


@dataclass(frozen=True)
class DepartmentFile:
    department: str
    period: str  # "previous" | "latest"
    first_year: int
    last_year: int
    url: str

    @property
    def name(self) -> str:
        """Bronze dataset name: one per department and period (e.g. 'daily_75_latest')."""
        return f"daily_{self.department}_{self.period}"


def parse_catalogue(payload: bytes | Mapping[str, Any]) -> list[DepartmentFile]:
    """Extract every `RR-T-Vent` department file from a data.gouv.fr dataset document."""
    if isinstance(payload, bytes | str):
        try:
            payload = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ResolveError(f"data.gouv.fr answer is not valid JSON ({exc})") from exc
    resources = payload.get("resources") if isinstance(payload, Mapping) else None
    if not isinstance(resources, list):
        raise ResolveError("data.gouv.fr dataset document has no 'resources' list")

    files: dict[tuple[str, str], DepartmentFile] = {}
    for resource in resources:
        url = resource.get("url") if isinstance(resource, Mapping) else None
        if not isinstance(url, str):
            continue
        match = _FILE.search(url)
        if not match:
            continue
        found = DepartmentFile(
            department=match["dept"],
            period=match["period"],
            first_year=int(match["start"]),
            last_year=int(match["end"]),
            url=url,
        )
        key = (found.department, found.period)
        # During the yearly rollover both the old and new file can be listed for a
        # short while: keep the one that reaches furthest.
        if key not in files or found.last_year > files[key].last_year:
            files[key] = found
    return sorted(files.values(), key=lambda f: (f.department, PERIODS.index(f.period)))


def select_files(
    catalogue: Iterable[DepartmentFile],
    departments: Iterable[str | int],
    periods: Iterable[str] = PERIODS,
) -> list[DepartmentFile]:
    """Pick the files for `departments` x `periods`; fail if any is missing."""
    wanted_periods = tuple(periods)
    for period in wanted_periods:
        if period not in PERIODS:
            raise ValueError(f"unknown period {period!r}, expected one of {PERIODS}")
    index = {(f.department, f.period): f for f in catalogue}
    selected: list[DepartmentFile] = []
    missing: list[str] = []
    for dept in dict.fromkeys(normalize_department(d) for d in departments):
        for period in wanted_periods:
            if (dept, period) in index:
                selected.append(index[(dept, period)])
            else:
                missing.append(f"{dept}/{period}")
    if missing:
        raise ResolveError("no RR-T-Vent file on data.gouv.fr for " + ", ".join(missing))
    return selected


# Columns downstream jobs rely on. The real files carry ~58 columns (every value
# has a Q* quality flag next to it); extra columns are fine, missing ones are not.
REQUIRED_COLUMNS = (
    "NUM_POSTE",
    "NOM_USUEL",
    "LAT",
    "LON",
    "ALTI",
    "AAAAMMJJ",
    "RR",  # precipitation, mm
    "TN",  # min temperature, °C
    "TX",  # max temperature, °C
    "TM",  # mean temperature, °C
    "FFM",  # mean wind speed at 10 m, m/s
    "FXY",  # max hourly-mean wind speed, m/s
)
MEASURES = ("RR", "TN", "TX", "TM", "FFM", "FXY")

# A department "latest" file is ~20-60 stations x 365-730 days; anything far below
# this is a truncated download or an empty placeholder.
MIN_ROWS = 1_000

_DAY = re.compile(r"^(19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])$")


def _is_number(value: str, *, empty_ok: bool = True) -> bool:
    if value == "":
        return empty_ok
    try:
        return math.isfinite(float(value))
    except ValueError:
        return False


@dataclass(frozen=True)
class ValidatedFile:
    department: str
    rows: int
    stations: int
    first_day: str
    last_day: str
    columns: tuple[str, ...]


def _gunzip(payload: bytes, name: str) -> bytes:
    try:
        return gzip.decompress(payload)
    except (OSError, EOFError, zlib.error) as exc:
        raise SchemaError(f"{name}: not a complete gzip file ({exc})") from exc


def validate_file(department: str, payload: bytes, *, min_rows: int | None = None) -> ValidatedFile:
    """Check a department `csv.gz` against the contract; raise SchemaError if it fails.

    `min_rows` overrides the floor (unit tests use small fixtures).
    """
    department = normalize_department(department)
    name = f"meteo/{department}"
    text = _gunzip(payload, name).decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text), delimiter=";")
    header = [column.strip() for column in next(reader, [])]
    missing = [column for column in REQUIRED_COLUMNS if column not in header]
    if missing:
        raise SchemaError(f"{name}: missing columns {missing} (header: {header[:8]}...)")
    pos = {column: header.index(column) for column in REQUIRED_COLUMNS}
    prefix = station_prefix(department)

    problems: list[str] = []
    stations: set[str] = set()
    rows = 0
    first_day = last_day = ""
    for line_no, row in enumerate(reader, start=2):
        if not row:
            continue
        rows += 1
        if len(row) < len(header):
            problems.append(f"line {line_no}: {len(row)} fields, expected {len(header)}")
        else:
            station, day = row[pos["NUM_POSTE"]].strip(), row[pos["AAAAMMJJ"]].strip()
            if not (station.isdigit() and len(station) == 8 and station.startswith(prefix)):
                problems.append(f"line {line_no}: NUM_POSTE={station!r} not in {department}")
            if not _DAY.match(day):
                problems.append(f"line {line_no}: bad AAAAMMJJ={day!r}")
            else:
                first_day = min(first_day or day, day)
                last_day = max(last_day, day)
            for column in ("LAT", "LON", "ALTI"):
                if not _is_number(row[pos[column]].strip(), empty_ok=False):
                    problems.append(f"line {line_no}: bad {column}={row[pos[column]]!r}")
            for column in MEASURES:
                if not _is_number(row[pos[column]].strip()):
                    problems.append(f"line {line_no}: bad {column}={row[pos[column]]!r}")
            stations.add(station)
        if len(problems) >= 10:
            raise SchemaError(f"{name}: " + "; ".join(problems[:10]))
    if problems:
        raise SchemaError(f"{name}: " + "; ".join(problems))

    floor = MIN_ROWS if min_rows is None else min_rows
    if rows < max(floor, 1):
        raise SchemaError(f"{name}: {rows} rows, expected at least {floor}")
    return ValidatedFile(
        department=department,
        rows=rows,
        stations=len(stations),
        first_day=first_day,
        last_day=last_day,
        columns=tuple(header),
    )
