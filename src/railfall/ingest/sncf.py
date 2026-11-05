"""SNCF open-data datasets (ressources.data.sncf.com, an Opendatasoft portal).

Both datasets are small (a few thousand rows), so we pull the complete export in
one request from the Explore API v2.1 `exports/json` endpoint rather than paging
through `records` (capped at 100 rows per call and 10k offset).

Bronze keeps the payload byte-for-byte; this module only checks that what came
back is the dataset we think it is (a non-empty JSON array whose records carry
the fields downstream jobs rely on) before it is landed.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from railfall.ingest.http import build_url

PORTAL = "https://ressources.data.sncf.com/api/explore/v2.1"

_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


class SchemaError(ValueError):
    """The export does not look like the dataset we asked for."""


def _is_month(value: Any) -> bool:
    return isinstance(value, str) and bool(_MONTH.match(value))


def _is_count(value: Any) -> bool:
    # bool is an int subclass: `true` in a count column is a broken export, not 1.
    if isinstance(value, bool):
        return False
    return value is None or (isinstance(value, int | float) and value >= 0)


def _is_text(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _is_point(value: Any) -> bool:
    return (
        isinstance(value, Mapping)
        and isinstance(value.get("lat"), int | float)
        and isinstance(value.get("lon"), int | float)
    )


@dataclass(frozen=True)
class Dataset:
    """An SNCF dataset and the contract its records must honour."""

    name: str
    dataset_id: str
    # field -> check; a field missing from a record is always an error.
    required: Mapping[str, Callable[[Any], bool]]
    # A sanity floor: a real export far below this is truncated or wrong.
    min_rows: int

    def export_url(self, fmt: str = "json") -> str:
        return build_url(f"{PORTAL}/catalog/datasets/{self.dataset_id}/exports/{fmt}")


PUNCTUALITY = Dataset(
    name="punctuality",
    dataset_id="regularite-mensuelle-tgv-aqst",
    required={
        "date": _is_month,
        "service": _is_text,
        "gare_depart": _is_text,
        "gare_arrivee": _is_text,
        "nb_train_prevu": _is_count,
        "nb_annulation": _is_count,
        "nb_train_retard_arrivee": _is_count,
        "retard_moyen_arrivee": _is_count,
    },
    # ~130 routes x 12 months x 7+ years of history.
    min_rows=5_000,
)

STATIONS = Dataset(
    name="stations",
    dataset_id="gares-de-voyageurs",
    required={
        "nom": _is_text,
        "codes_uic": _is_text,
        "position_geographique": lambda v: v is None or _is_point(v),
    },
    # ~2,800 passenger stations in France.
    min_rows=2_000,
)

DATASETS: dict[str, Dataset] = {d.name: d for d in (PUNCTUALITY, STATIONS)}


@dataclass(frozen=True)
class ValidatedExport:
    dataset: str
    rows: int
    fields: tuple[str, ...]


def validate_export(
    dataset: Dataset, payload: bytes, *, min_rows: int | None = None
) -> ValidatedExport:
    """Check a JSON export against the dataset contract; raise SchemaError if it fails.

    `min_rows` overrides the dataset's floor (unit tests use small fixtures).
    """
    try:
        records = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SchemaError(f"{dataset.name}: export is not valid JSON ({exc})") from exc
    if not isinstance(records, list):
        raise SchemaError(f"{dataset.name}: expected a JSON array, got {type(records).__name__}")

    floor = dataset.min_rows if min_rows is None else min_rows
    if len(records) < max(floor, 1):
        raise SchemaError(f"{dataset.name}: {len(records)} rows, expected at least {floor}")

    problems: list[str] = []
    fields: set[str] = set()
    for i, record in enumerate(records):
        if not isinstance(record, Mapping):
            problems.append(f"row {i}: not an object")
            continue
        fields.update(record)
        for name, check in dataset.required.items():
            if name not in record:
                problems.append(f"row {i}: missing {name!r}")
            elif not check(record[name]):
                problems.append(f"row {i}: bad {name}={record[name]!r}")
        if len(problems) >= 10:
            break
    if problems:
        raise SchemaError(f"{dataset.name}: " + "; ".join(problems[:10]))
    return ValidatedExport(dataset=dataset.name, rows=len(records), fields=tuple(sorted(fields)))
