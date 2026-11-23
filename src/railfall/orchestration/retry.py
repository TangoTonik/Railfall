"""Retry policy for the DAGs: retry what time can fix, fail fast on what it cannot.

Airflow retries every failed task the same way by default. That is right for a
portal answering 503 or a Postgres restarting under us, and wrong for a
contract violation: re-reading the same bronze file or the same `raw.*` table
gives the same answer three times, 5 + 10 + 20 minutes later, and the run only
turns red once nobody is looking any more.

So failures are sorted in two:

* **transient** (network, timeouts, connection refused, a download that ran
  out of attempts, a step that may land differently next time) are retried
  with the shared exponential backoff of `task_defaults()`;
* **permanent** (a verification or quality gate that found bad data, a
  non-retryable HTTP status, a programming error such as `ValueError`) fail
  the task at once, through `AirflowFailException` which skips the remaining
  retries.

Anything unknown is treated as transient: an extra retry costs minutes, a
wrongly skipped one costs a night of data.

`connect()` gives the warehouse connections the same patience on their own:
a few quick in-task attempts before the error reaches Airflow at all.
"""

from __future__ import annotations

import functools
import logging
import time
import urllib.error
from collections.abc import Callable
from datetime import timedelta
from typing import Any, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")

log = logging.getLogger(__name__)

# Errors raised by bad input or bad code: a retry replays them identically.
PERMANENT_TYPES: tuple[type[BaseException], ...] = (
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    NotImplementedError,
)
# Errors raised by the network or a service that is momentarily unavailable.
TRANSIENT_TYPES: tuple[type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    urllib.error.URLError,
)


def task_defaults(
    *,
    retries: int = 3,
    retry_delay: timedelta = timedelta(minutes=2),
    max_retry_delay: timedelta = timedelta(minutes=20),
    execution_timeout: timedelta = timedelta(minutes=30),
) -> dict[str, Any]:
    """The `default_args` shared by every Railfall DAG.

    Three retries 2, 4 and 8 minutes apart (capped at 20) ride out a portal
    redeploy or a Postgres restart, and still finish well before the next
    daily run.
    """
    return {
        "owner": "railfall",
        "retries": retries,
        "retry_delay": retry_delay,
        "retry_exponential_backoff": True,
        "max_retry_delay": max_retry_delay,
        "execution_timeout": execution_timeout,
    }


class PermanentFailure(RuntimeError):
    """Raised instead of `AirflowFailException` where Airflow is not installed."""


def _operational_errors() -> tuple[type[BaseException], ...]:
    try:
        import psycopg
    except ImportError:  # pragma: no cover - psycopg is a runtime dependency
        return ()
    return (psycopg.OperationalError,)


def is_transient(exc: BaseException) -> bool:
    """Would retrying the step that raised `exc` plausibly succeed?"""
    flag = getattr(exc, "retryable", None)
    if isinstance(flag, bool):  # TaskFailed, DownloadError: the raiser knows best
        return flag
    if isinstance(exc, TRANSIENT_TYPES + _operational_errors()):
        return True
    if isinstance(exc, PERMANENT_TYPES):
        return False
    cause = exc.__cause__
    if cause is not None and cause is not exc:
        return is_transient(cause)
    return True


def _fail_fast_type() -> type[BaseException]:
    try:
        from airflow.exceptions import AirflowFailException
    except ImportError:
        return PermanentFailure
    return AirflowFailException


def fail_fast(fn: Callable[P, R]) -> Callable[P, R]:
    """Decorate a task callable so permanent errors skip Airflow's retries.

    Transient errors propagate unchanged and are retried as configured. The
    wrapper keeps the signature (`functools.wraps`), so TaskFlow still sees the
    task's real parameters.
    """

    @functools.wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if is_transient(exc):
                raise
            log.error("permanent failure, not retrying: %s: %s", type(exc).__name__, exc)
            raise _fail_fast_type()(f"{type(exc).__name__}: {exc}") from exc

    return wrapper


def connect(
    dsn: str,
    *,
    attempts: int = 4,
    backoff_s: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
    connector: Callable[..., Any] | None = None,
    **kwargs: Any,
):
    """`psycopg.connect` with a few quick retries on `OperationalError`.

    Waits `backoff_s * 2**n` between attempts (2 + 4 + 8 s by default): enough
    for a Postgres that is restarting or still recovering, short enough that a
    database that is really down reaches Airflow's own retries quickly.
    Authentication and other non-operational errors are raised at once.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    if connector is None:
        import psycopg

        connector = psycopg.connect
    errors = _operational_errors() or (ConnectionError,)
    kwargs.setdefault("connect_timeout", 10)
    for attempt in range(attempts):
        try:
            return connector(dsn, **kwargs)
        except errors as exc:
            if attempt == attempts - 1 or _is_auth_error(exc):
                raise
            wait = backoff_s * 2**attempt
            log.warning(
                "warehouse connection failed (attempt %d/%d), retrying in %.0fs: %s",
                attempt + 1,
                attempts,
                wait,
                exc,
            )
            sleep(wait)
    raise AssertionError("unreachable")  # pragma: no cover


def _is_auth_error(exc: BaseException) -> bool:
    # libpq reports bad credentials as an OperationalError too; waiting does
    # not fix a wrong password, so give up straight away.
    text = str(exc).lower()
    return "password authentication failed" in text or ("role" in text and "does not exist" in text)
