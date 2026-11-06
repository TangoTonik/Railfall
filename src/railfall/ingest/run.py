"""Ingestion entry point: download -> validate -> land in bronze.

    python -m railfall.ingest.run sncf                    # every SNCF dataset
    python -m railfall.ingest.run sncf --dataset stations
    python -m railfall.ingest.run sncf --json             # machine-readable summary
    python -m railfall.ingest.run meteo                   # METEO_DEPARTMENTS, latest period
    python -m railfall.ingest.run meteo --department 33 --period previous

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
from railfall.ingest import meteo
from railfall.ingest.bronze import land
from railfall.ingest.http import Download, DownloadError, fetch
from railfall.ingest.sncf import DATASETS, Dataset, SchemaError, validate_export

SOURCE = "sncf"
METEO_SOURCE = "meteo"
# "latest" (current + previous year, a few MB per department) is what the
# pipeline needs day to day; "previous" (1950 onwards) is a one-off backfill.
METEO_DEFAULT_PERIODS = ("latest",)

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


def ingest_meteo_file(
    client,
    bucket: str,
    file: meteo.DepartmentFile,
    *,
    fetcher: Fetcher,
    ingest_date: date,
    now: datetime,
    min_rows: int | None = None,
) -> DatasetResult:
    try:
        download = fetcher(file.url)
        validated = meteo.validate_file(file.department, download.content, min_rows=min_rows)
    except (DownloadError, meteo.SchemaError) as exc:
        return DatasetResult(dataset=file.name, ok=False, status="failed", error=str(exc))
    landing = land(
        client,
        bucket,
        source=METEO_SOURCE,
        dataset=file.name,
        payload=download.content,
        rows=validated.rows,
        source_url=file.url,
        ingest_date=ingest_date,
        fmt="csv.gz",
        now=now,
    )
    return DatasetResult(
        dataset=file.name,
        ok=True,
        status=landing.status,
        rows=landing.rows,
        key=landing.key,
        sha256=landing.sha256,
    )


def ingest_meteo(
    client,
    bucket: str,
    *,
    fetcher: Fetcher,
    departments: Iterable[str | int],
    periods: Iterable[str] = METEO_DEFAULT_PERIODS,
    ingest_date: date | None = None,
    now: datetime | None = None,
    min_rows: int | None = None,
) -> list[DatasetResult]:
    """Resolve the department files from data.gouv.fr, then land each one in bronze.

    Bad department codes or periods raise before any download. A department
    missing from the catalogue, or a file that fails its contract, is reported
    as failed without stopping the others.
    """
    now = now or datetime.now(UTC)
    ingest_date = ingest_date or now.date()
    wanted = list(dict.fromkeys(meteo.normalize_department(d) for d in departments))
    if not wanted:
        raise ValueError("no Météo-France department requested")
    periods = tuple(periods)
    for period in periods:
        if period not in meteo.PERIODS:
            raise ValueError(f"unknown period {period!r}, expected one of {meteo.PERIODS}")

    try:
        catalogue = meteo.parse_catalogue(fetcher(meteo.dataset_url()).content)
    except (DownloadError, meteo.ResolveError) as exc:
        return [DatasetResult(dataset="catalogue", ok=False, status="failed", error=str(exc))]

    results: list[DatasetResult] = []
    for department in wanted:
        try:
            files = meteo.select_files(catalogue, [department], periods)
        except meteo.ResolveError as exc:
            results.append(
                DatasetResult(
                    dataset=f"daily_{department}", ok=False, status="failed", error=str(exc)
                )
            )
            continue
        for file in files:
            results.append(
                ingest_meteo_file(
                    client,
                    bucket,
                    file,
                    fetcher=fetcher,
                    ingest_date=ingest_date,
                    now=now,
                    min_rows=min_rows,
                )
            )
    return results


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
        return f"[FAIL] {result.dataset:<16} {result.error}"
    return f"[  ok] {result.dataset:<16} {result.status:<8} {result.rows:>7} rows -> {result.key}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest open data into the bronze layer.")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="ingest_date partition to write (YYYY-MM-DD); default: today (UTC)",
    )
    common.add_argument("--json", action="store_true", help="print a JSON summary")
    sources = parser.add_subparsers(dest="source", required=True, metavar="source")

    sncf = sources.add_parser(SOURCE, parents=[common], help="SNCF open data exports")
    sncf.add_argument(
        "--dataset",
        action="append",
        choices=sorted(DATASETS),
        help="only this dataset (repeatable); default: all",
    )
    weather = sources.add_parser(
        METEO_SOURCE, parents=[common], help="Météo-France daily data per department"
    )
    weather.add_argument(
        "--department",
        action="append",
        help="INSEE department code (repeatable); default: METEO_DEPARTMENTS",
    )
    weather.add_argument(
        "--period",
        action="append",
        choices=meteo.PERIODS,
        help=f"file period (repeatable); default: {', '.join(METEO_DEFAULT_PERIODS)}",
    )
    args = parser.parse_args(argv)

    settings = Settings.from_env()

    def fetcher(url: str) -> Download:
        return fetch(url, timeout_s=settings.http_timeout_s, max_retries=settings.http_max_retries)

    client = lake_client(settings)
    if args.source == METEO_SOURCE:
        try:
            results = ingest_meteo(
                client,
                settings.lake_bucket,
                fetcher=fetcher,
                departments=args.department or settings.meteo_departments,
                periods=args.period or METEO_DEFAULT_PERIODS,
                ingest_date=args.date,
            )
        except ValueError as exc:
            parser.error(str(exc))
    else:
        results = ingest_sncf(
            client,
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
