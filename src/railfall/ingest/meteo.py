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

import json
import re
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
