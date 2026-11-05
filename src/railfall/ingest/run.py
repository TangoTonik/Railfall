"""Ingestion entry point: download -> validate -> land in bronze.

    python -m railfall.ingest.run sncf                    # every SNCF dataset
    python -m railfall.ingest.run sncf --dataset stations
    python -m railfall.ingest.run sncf --json             # machine-readable summary

Each dataset is handled on its own: one failing download or schema check does
not stop the others, but makes the command exit non-zero. Nothing reaches
bronze unless it passed validation, so bronze never holds a half-download or a
portal error page.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime

from railfall.config import Settings
from railfall.ingest.bronze import land
from railfall.ingest.http import Download, DownloadError, fetch
from railfall.ingest.sncf import DATASETS, Dataset, SchemaError, validate_export

SOURCE = "sncf"

Fetcher = Callable[[str], Download]


@dataclass(frozen=True)
class DatasetResult:
    dataset: str
    ok: bool
    status: str  # landing status, or "failed"
    rows: int = 0
    key: str = ""
    sha256: str = ""
    error: str = ""


def ingest_dataset(
    client,
    bucket: str,
    dataset: Dataset,
    *,
    fetcher: Fetcher,
    ingest_date: date,
    now: datetime,
    min_rows: int | None = None,
) -> DatasetResult:
    url = dataset.export_url()
    try:
        download = fetcher(url)
        export = validate_export(dataset, download.content, min_rows=min_rows)
    except (DownloadError, SchemaError) as exc:
        return DatasetResult(dataset=dataset.name, ok=False, status="failed", error=str(exc))
    landing = land(
        client,
        bucket,
        source=SOURCE,
        dataset=dataset.name,
        payload=download.content,
        rows=export.rows,
        source_url=url,
        ingest_date=ingest_date,
        now=now,
    )
    return DatasetResult(
        dataset=dataset.name,
        ok=True,
        status=landing.status,
        rows=landing.rows,
        key=landing.key,
        sha256=landing.sha256,
    )


def ingest_sncf(
    client,
    bucket: str,
    *,
    fetcher: Fetcher,
    names: Iterable[str] | None = None,
    ingest_date: date | None = None,
    now: datetime | None = None,
    min_rows: int | None = None,
) -> list[DatasetResult]:
    now = now or datetime.now(UTC)
    ingest_date = ingest_date or now.date()
    selected = list(names) if names else list(DATASETS)
    unknown = sorted(set(selected) - set(DATASETS))
    if unknown:
        raise ValueError(f"unknown SNCF dataset(s): {', '.join(unknown)}")
    return [
        ingest_dataset(
            client,
            bucket,
            DATASETS[name],
            fetcher=fetcher,
            ingest_date=ingest_date,
            now=now,
            min_rows=min_rows,
        )
        for name in selected
    ]


def lake_client(settings: Settings):
    import boto3
    from botocore.config import Config

    # Exports are a few MB: allow a longer read than the health checks do.
    return boto3.client(
        "s3",
        endpoint_url=settings.lake_endpoint_url,
        aws_access_key_id=settings.lake_access_key,
        aws_secret_access_key=settings.lake_secret_key,
        region_name="us-east-1",
        config=Config(connect_timeout=5, read_timeout=60, retries={"max_attempts": 3}),
    )


def _format(result: DatasetResult) -> str:
    if not result.ok:
        return f"[FAIL] {result.dataset:<12} {result.error}"
    return f"[  ok] {result.dataset:<12} {result.status:<8} {result.rows:>7} rows -> {result.key}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest open data into the bronze layer.")
    parser.add_argument("source", choices=[SOURCE], help="data source to ingest")
    parser.add_argument(
        "--dataset",
        action="append",
        choices=sorted(DATASETS),
        help="only this dataset (repeatable); default: all",
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="ingest_date partition to write (YYYY-MM-DD); default: today (UTC)",
    )
    parser.add_argument("--json", action="store_true", help="print a JSON summary")
    args = parser.parse_args(argv)

    settings = Settings.from_env()

    def fetcher(url: str) -> Download:
        return fetch(url, timeout_s=settings.http_timeout_s, max_retries=settings.http_max_retries)

    results = ingest_sncf(
        lake_client(settings),
        settings.lake_bucket,
        fetcher=fetcher,
        names=args.dataset,
        ingest_date=args.date,
    )
    if args.json:
        print(json.dumps([asdict(r) for r in results], indent=2))
    else:
        for r in results:
            print(_format(r))
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
