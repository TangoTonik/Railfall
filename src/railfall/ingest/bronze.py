"""Land raw source payloads in the bronze layer of the lake, idempotently.

Layout (one object per dataset per ingestion day, plus a manifest next to it):

    bronze/<source>/<dataset>/ingest_date=YYYY-MM-DD/<dataset>.<ext>
    bronze/<source>/<dataset>/ingest_date=YYYY-MM-DD/_manifest.json

The payload is stored byte-for-byte. Its SHA-256 is written both as object
metadata and in the manifest, so a re-run on the same day that downloads the same
bytes is a no-op (`skipped`), and a re-run that gets different bytes (the source
was republished) replaces that day's object instead of adding a second copy.
Either way a day never holds more than one version of a dataset.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime

BRONZE_PREFIX = "bronze"
SHA_METADATA_KEY = "sha256"

_CONTENT_TYPES = {"json": "application/json", "csv": "text/csv"}


@dataclass(frozen=True)
class Landing:
    """Where a payload went and what happened to it."""

    key: str
    manifest_key: str
    sha256: str
    size_bytes: int
    rows: int
    status: str  # "written" | "skipped" | "replaced"


def bronze_prefix(source: str, dataset: str, ingest_date: date) -> str:
    for part, value in (("source", source), ("dataset", dataset)):
        if not value or "/" in value or value.startswith("."):
            raise ValueError(f"invalid {part} name for a lake key: {value!r}")
    return f"{BRONZE_PREFIX}/{source}/{dataset}/ingest_date={ingest_date.isoformat()}"


def _existing_sha(client, bucket: str, key: str) -> str | None:
    from botocore.exceptions import ClientError

    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    return head.get("Metadata", {}).get(SHA_METADATA_KEY)


def land(
    client,
    bucket: str,
    *,
    source: str,
    dataset: str,
    payload: bytes,
    rows: int,
    source_url: str,
    ingest_date: date | None = None,
    fmt: str = "json",
    now: datetime | None = None,
) -> Landing:
    """Write `payload` to bronze unless that day already holds the same bytes."""
    now = now or datetime.now(UTC)
    ingest_date = ingest_date or now.date()
    prefix = bronze_prefix(source, dataset, ingest_date)
    key = f"{prefix}/{dataset}.{fmt}"
    manifest_key = f"{prefix}/_manifest.json"
    sha = hashlib.sha256(payload).hexdigest()

    previous = _existing_sha(client, bucket, key)
    if previous == sha:
        status = "skipped"
    else:
        status = "written" if previous is None else "replaced"
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            ContentType=_CONTENT_TYPES.get(fmt, "application/octet-stream"),
            Metadata={SHA_METADATA_KEY: sha, "rows": str(rows)},
        )

    landing = Landing(
        key=key,
        manifest_key=manifest_key,
        sha256=sha,
        size_bytes=len(payload),
        rows=rows,
        status=status,
    )
    if status != "skipped":
        manifest = {
            **asdict(landing),
            "source": source,
            "dataset": dataset,
            "source_url": source_url,
            "ingest_date": ingest_date.isoformat(),
            "landed_at": now.isoformat(timespec="seconds"),
        }
        manifest.pop("status")
        client.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2, sort_keys=True).encode(),
            ContentType="application/json",
        )
    return landing


def read_manifest(client, bucket: str, manifest_key: str) -> dict:
    body = client.get_object(Bucket=bucket, Key=manifest_key)["Body"].read()
    return json.loads(body)
