"""Read bronze back and prove it is what the ingestion claims it wrote.

    python -m railfall.ingest.verify sncf                 # today's partition (UTC)
    python -m railfall.ingest.verify sncf --date 2026-11-05 --json
    python -m railfall.ingest.verify sncf --list          # every bronze key, all days

For every dataset of the source, the day's partition must hold exactly the
payload and its manifest — no stray objects, no second version — and:

* the payload's SHA-256 matches both the manifest and the object metadata;
* the payload still passes the dataset contract, with the *real* row floor
  (not the relaxed one unit tests use);
* the row count in the manifest is the row count of the payload.

The e2e workflow runs this against MinIO after the real download, so the
assertions are made on what is actually stored, not on what the ingest step
printed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime

from railfall.ingest.bronze import BRONZE_PREFIX, SHA_METADATA_KEY, bronze_prefix
from railfall.ingest.sncf import DATASETS, Dataset, SchemaError, validate_export

SOURCES: dict[str, dict[str, Dataset]] = {"sncf": DATASETS}


@dataclass(frozen=True)
class Check:
    dataset: str
    ok: bool
    rows: int = 0
    key: str = ""
    problems: list[str] = field(default_factory=list)


def _list_keys(client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return sorted(keys)


def list_source(client, bucket: str, source: str) -> list[str]:
    """Every bronze key of a source, across all ingest days."""
    if source not in SOURCES:
        raise ValueError(f"unknown source: {source!r}")
    return _list_keys(client, bucket, f"{BRONZE_PREFIX}/{source}")


def verify_dataset(
    client,
    bucket: str,
    source: str,
    dataset: Dataset,
    ingest_date: date,
    *,
    min_rows: int | None = None,
) -> Check:
    prefix = bronze_prefix(source, dataset.name, ingest_date)
    key = f"{prefix}/{dataset.name}.json"
    manifest_key = f"{prefix}/_manifest.json"

    keys = _list_keys(client, bucket, prefix)
    if not keys:
        return Check(dataset.name, ok=False, key=key, problems=[f"nothing under {prefix}/"])
    expected = sorted([key, manifest_key])
    problems: list[str] = []
    if keys != expected:
        extra = sorted(set(keys) - set(expected))
        missing = sorted(set(expected) - set(keys))
        if extra:
            problems.append(f"unexpected objects: {', '.join(extra)}")
        if missing:
            problems.append(f"missing objects: {', '.join(missing)}")
        return Check(dataset.name, ok=False, key=key, problems=problems)

    obj = client.get_object(Bucket=bucket, Key=key)
    payload = obj["Body"].read()
    manifest = json.loads(client.get_object(Bucket=bucket, Key=manifest_key)["Body"].read())
    sha = hashlib.sha256(payload).hexdigest()

    if manifest.get("sha256") != sha:
        problems.append(f"manifest sha256 {manifest.get('sha256')!r} != payload {sha}")
    if obj.get("Metadata", {}).get(SHA_METADATA_KEY) != sha:
        problems.append("object metadata sha256 does not match payload")
    if manifest.get("size_bytes") != len(payload):
        problems.append(f"manifest size {manifest.get('size_bytes')} != payload {len(payload)}")
    if manifest.get("ingest_date") != ingest_date.isoformat():
        problems.append(f"manifest ingest_date {manifest.get('ingest_date')!r} is wrong")

    rows = 0
    try:
        export = validate_export(dataset, payload, min_rows=min_rows)
    except SchemaError as exc:
        problems.append(str(exc))
    else:
        rows = export.rows
        if manifest.get("rows") != rows:
            problems.append(f"manifest rows {manifest.get('rows')} != payload rows {rows}")

    return Check(dataset.name, ok=not problems, rows=rows, key=key, problems=problems)


def verify_source(
    client,
    bucket: str,
    source: str,
    ingest_date: date,
    *,
    names: Iterable[str] | None = None,
    min_rows: int | None = None,
) -> list[Check]:
    if source not in SOURCES:
        raise ValueError(f"unknown source: {source!r}")
    datasets = SOURCES[source]
    selected = list(names) if names else list(datasets)
    unknown = sorted(set(selected) - set(datasets))
    if unknown:
        raise ValueError(f"unknown {source} dataset(s): {', '.join(unknown)}")
    return [
        verify_dataset(client, bucket, source, datasets[name], ingest_date, min_rows=min_rows)
        for name in selected
    ]


def _format(check: Check) -> str:
    if check.ok:
        return f"[  ok] {check.dataset:<12} {check.rows:>7} rows  {check.key}"
    return f"[FAIL] {check.dataset:<12} " + "; ".join(check.problems)


def main(argv: Sequence[str] | None = None) -> int:
    from railfall.config import Settings
    from railfall.ingest.run import lake_client

    parser = argparse.ArgumentParser(description="Verify a bronze partition against its contract.")
    parser.add_argument("source", choices=sorted(SOURCES))
    parser.add_argument("--dataset", action="append", help="only this dataset (repeatable)")
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="ingest_date partition to check (YYYY-MM-DD); default: today (UTC)",
    )
    parser.add_argument("--json", action="store_true", help="print a JSON summary")
    parser.add_argument(
        "--list", action="store_true", help="only list the source's bronze keys, one per line"
    )
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    if args.list:
        for key in list_source(lake_client(settings), settings.lake_bucket, args.source):
            print(key)
        return 0
    checks = verify_source(
        lake_client(settings),
        settings.lake_bucket,
        args.source,
        args.date or datetime.now(UTC).date(),
        names=args.dataset,
    )
    if args.json:
        print(json.dumps([asdict(c) for c in checks], indent=2))
    else:
        for c in checks:
            print(_format(c))
    return 0 if all(c.ok for c in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
