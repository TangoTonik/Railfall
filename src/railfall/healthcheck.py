"""Health checks for the local platform (Postgres warehouse + MinIO lake).

Used by the `e2e` workflow right after `docker compose up`, and handy locally:

    python -m railfall.healthcheck --wait 120

Exit code 0 means every service answered; 1 means at least one did not.
"""

from __future__ import annotations

import argparse
import sys
import time
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from railfall.config import Settings


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str


def check_postgres(settings: Settings) -> CheckResult:
    """Open a connection and run a trivial query against the warehouse."""
    import psycopg

    try:
        with psycopg.connect(settings.warehouse_dsn, connect_timeout=5) as conn:
            row = conn.execute("select version()").fetchone()
    except psycopg.Error as exc:
        return CheckResult("postgres", False, _first_line(exc))
    version = row[0].split(" on ", 1)[0] if row else "unknown"
    return CheckResult("postgres", True, version)


def check_minio_live(settings: Settings) -> CheckResult:
    """Hit MinIO's unauthenticated liveness endpoint."""
    url = settings.lake_endpoint_url.rstrip("/") + "/minio/health/live"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310 - fixed scheme
            status = resp.status
    except OSError as exc:
        return CheckResult("minio", False, _first_line(exc))
    return CheckResult("minio", status == 200, f"HTTP {status}")


def check_lake_bucket(settings: Settings) -> CheckResult:
    """Check the lake bucket exists and the configured credentials can reach it."""
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError

    client = boto3.client(
        "s3",
        endpoint_url=settings.lake_endpoint_url,
        aws_access_key_id=settings.lake_access_key,
        aws_secret_access_key=settings.lake_secret_key,
        region_name="us-east-1",
        config=Config(connect_timeout=5, read_timeout=5, retries={"max_attempts": 1}),
    )
    try:
        client.head_bucket(Bucket=settings.lake_bucket)
    except (BotoCoreError, ClientError) as exc:
        return CheckResult("lake-bucket", False, _first_line(exc))
    return CheckResult("lake-bucket", True, settings.lake_bucket)


DEFAULT_CHECKS: tuple[Callable[[Settings], CheckResult], ...] = (
    check_postgres,
    check_minio_live,
    check_lake_bucket,
)


def run_checks(
    settings: Settings,
    checks: Sequence[Callable[[Settings], CheckResult]] = DEFAULT_CHECKS,
    *,
    wait_s: float = 0.0,
    interval_s: float = 3.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> list[CheckResult]:
    """Run every check, retrying the failing ones until they pass or `wait_s` elapses."""
    deadline = clock() + wait_s
    results = {c: c(settings) for c in checks}
    while not all(r.ok for r in results.values()) and clock() < deadline:
        sleep(interval_s)
        for check, result in results.items():
            if not result.ok:
                results[check] = check(settings)
    return list(results.values())


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip() or type(exc).__name__
    return text.splitlines()[0]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check the Railfall platform services.")
    parser.add_argument(
        "--wait", type=float, default=0.0, help="seconds to keep retrying failing checks"
    )
    parser.add_argument("--interval", type=float, default=3.0, help="seconds between retries")
    args = parser.parse_args(argv)

    results = run_checks(Settings.from_env(), wait_s=args.wait, interval_s=args.interval)
    for r in results:
        print(f"[{'ok' if r.ok else 'FAIL':>4}] {r.name:<12} {r.detail}")
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
