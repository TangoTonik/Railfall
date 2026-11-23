"""Retry policy: transient errors are retried, permanent ones fail the task at once."""

from __future__ import annotations

import inspect
import urllib.error
from datetime import timedelta

import psycopg
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

from railfall.ingest.http import DownloadError, fetch
from railfall.orchestration import retry
from railfall.orchestration.ingest import TaskFailed, _raise_on_failures


def test_task_defaults_back_off_exponentially_and_cap_the_wait() -> None:
    args = retry.task_defaults()
    assert args["retries"] == 3
    assert args["retry_exponential_backoff"] is True
    assert args["retry_delay"] == timedelta(minutes=2)
    assert args["max_retry_delay"] == timedelta(minutes=20)
    assert args["execution_timeout"] == timedelta(minutes=30)
    assert retry.task_defaults(retries=0)["retries"] == 0


@pytest.mark.parametrize(
    "exc",
    [
        ConnectionResetError("reset by peer"),
        TimeoutError("read timed out"),
        urllib.error.URLError("temporary failure in name resolution"),
        psycopg.OperationalError("connection refused"),
        TaskFailed("ingest sncf: 1 dataset(s) failed"),
        DownloadError("GET x failed after 4 attempts: HTTP 503", retryable=True),
        RuntimeError("something nobody classified"),
    ],
)
def test_transient_errors(exc: Exception) -> None:
    assert retry.is_transient(exc)


@pytest.mark.parametrize(
    "exc",
    [
        TaskFailed("verify silver: 1 dataset(s) failed", retryable=False),
        DownloadError("GET x -> HTTP 404 Not Found"),
        ValueError("unknown silver tables: ['nope']"),
        KeyError("ingest_date"),
    ],
)
def test_permanent_errors(exc: Exception) -> None:
    assert not retry.is_transient(exc)


def test_unknown_errors_are_judged_by_their_cause() -> None:
    try:
        try:
            raise ValueError("bad column")
        except ValueError as inner:
            raise RuntimeError("spark job failed") from inner
    except RuntimeError as outer:
        assert not retry.is_transient(outer)


def test_explicit_flag_wins_over_the_type() -> None:
    exc = ConnectionError("refused")
    exc.retryable = False  # type: ignore[attr-defined]
    assert not retry.is_transient(exc)


def test_verification_failures_are_not_retryable() -> None:
    with pytest.raises(TaskFailed) as excinfo:
        _raise_on_failures("verify sncf", ["stations: 0 rows"], retryable=False)
    assert excinfo.value.retryable is False
    with pytest.raises(TaskFailed) as excinfo:
        _raise_on_failures("ingest sncf", ["stations: HTTP 503"])
    assert excinfo.value.retryable is True


def test_download_error_retryable_only_when_attempts_ran_out() -> None:
    class _Opener:
        def __init__(self, code: int) -> None:
            self.code = code

        def __call__(self, request, timeout):
            raise urllib.error.HTTPError(request.full_url, self.code, "boom", {}, None)

    with pytest.raises(DownloadError) as excinfo:
        fetch("https://e.org", max_retries=1, sleep=lambda _s: None, opener=_Opener(503))
    assert excinfo.value.retryable is True
    with pytest.raises(DownloadError) as excinfo:
        fetch("https://e.org", max_retries=1, sleep=lambda _s: None, opener=_Opener(404))
    assert excinfo.value.retryable is False


def test_fail_fast_passes_results_and_transient_errors_through() -> None:
    @retry.fail_fast
    def ok(ingest_date: str, *, department: str = "75") -> str:
        return f"{ingest_date}/{department}"

    @retry.fail_fast
    def flaky() -> None:
        raise ConnectionResetError("reset")

    assert ok("2026-11-23", department="13") == "2026-11-23/13"
    assert list(inspect.signature(ok).parameters) == ["ingest_date", "department"]
    assert ok.__name__ == "ok"
    with pytest.raises(ConnectionResetError):
        flaky()


def test_fail_fast_turns_permanent_errors_into_a_no_retry_failure() -> None:
    @retry.fail_fast
    def gate() -> None:
        raise TaskFailed("check quality: 1 dataset(s) failed: stations", retryable=False)

    expected = retry._fail_fast_type()
    with pytest.raises(expected, match="TaskFailed: check quality") as excinfo:
        gate()
    assert isinstance(excinfo.value.__cause__, TaskFailed)


def test_fail_fast_type_is_airflows_when_installed() -> None:
    pytest.importorskip("airflow", reason="Airflow is not installed")
    from airflow.exceptions import AirflowFailException

    assert retry._fail_fast_type() is AirflowFailException


class _Connector:
    def __init__(self, failures: list[Exception]) -> None:
        self.failures = list(failures)
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, dsn: str, **kwargs):
        self.calls.append((dsn, kwargs))
        if self.failures:
            raise self.failures.pop(0)
        return "conn"


def test_connect_retries_operational_errors_with_backoff() -> None:
    connector = _Connector([psycopg.OperationalError("starting up")] * 2)
    sleeps: list[float] = []
    conn = retry.connect(
        "postgresql://x", backoff_s=1.5, sleep=sleeps.append, connector=connector, autocommit=True
    )
    assert conn == "conn"
    assert sleeps == [1.5, 3.0]
    assert connector.calls[0][1] == {"autocommit": True, "connect_timeout": 10}


def test_connect_gives_up_after_its_attempts() -> None:
    connector = _Connector([psycopg.OperationalError("refused")] * 5)
    sleeps: list[float] = []
    with pytest.raises(psycopg.OperationalError, match="refused"):
        retry.connect("postgresql://x", attempts=3, sleep=sleeps.append, connector=connector)
    assert len(connector.calls) == 3
    assert sleeps == [2.0, 4.0]


def test_connect_does_not_wait_on_bad_credentials() -> None:
    connector = _Connector(
        [psycopg.OperationalError('password authentication failed for user "rail"')]
    )
    with pytest.raises(psycopg.OperationalError):
        retry.connect("postgresql://x", sleep=pytest.fail, connector=connector)
    assert len(connector.calls) == 1


def test_connect_rejects_zero_attempts() -> None:
    with pytest.raises(ValueError):
        retry.connect("postgresql://x", attempts=0, connector=_Connector([]))


def _s3_error(code: str, status: int) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "PutObject",
    )


@pytest.mark.parametrize(
    "exc",
    [
        EndpointConnectionError(endpoint_url="http://minio:9000/railfall-lake"),
        ReadTimeoutError(endpoint_url="http://minio:9000/railfall-lake"),
        _s3_error("SlowDown", 503),
        _s3_error("InternalError", 500),
        _s3_error("XMinioServerNotInitialized", 503),
    ],
)
def test_lake_outages_are_transient(exc: Exception) -> None:
    """MinIO restarting or overloaded: the next try lands the same object."""
    assert retry.is_transient(exc)


@pytest.mark.parametrize(
    "exc",
    [
        _s3_error("AccessDenied", 403),
        _s3_error("NoSuchBucket", 404),
        _s3_error("InvalidAccessKeyId", 403),
    ],
)
def test_refused_lake_requests_are_permanent(exc: Exception) -> None:
    """A policy or a missing bucket does not change between retries."""
    assert not retry.is_transient(exc)


def test_fail_fast_does_not_retry_a_denied_lake_write() -> None:
    @retry.fail_fast
    def land() -> None:
        raise _s3_error("AccessDenied", 403)

    with pytest.raises(Exception) as info:
        land()
    assert type(info.value).__name__ in {"AirflowFailException", "PermanentFailure"}
    assert "AccessDenied" in str(info.value)
