"""Small HTTP download helper with timeouts and retries (stdlib only).

Open-data portals are flaky in boring ways: 429 rate limits, 502/503 during
redeploys, connections reset half-way through a large export. Those are retried
with exponential backoff; anything else (404, 400, ...) is a bug in our request
and fails immediately.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from railfall import __version__

RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
USER_AGENT = f"railfall/{__version__} (+https://github.com/TangoTonik/Railfall)"


class DownloadError(RuntimeError):
    """A download failed for good (non-retryable status, or retries exhausted)."""


@dataclass(frozen=True)
class Download:
    url: str
    status: int
    content: bytes
    content_type: str


Opener = Callable[[urllib.request.Request, float], object]


def _default_opener(request: urllib.request.Request, timeout: float):
    return urllib.request.urlopen(request, timeout=timeout)  # noqa: S310 - https URLs only


def build_url(base: str, params: Mapping[str, str | int] | None = None) -> str:
    if not params:
        return base
    return f"{base}?{urllib.parse.urlencode(params)}"


def fetch(
    url: str,
    *,
    timeout_s: float = 30.0,
    max_retries: int = 3,
    backoff_s: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    opener: Opener = _default_opener,
) -> Download:
    """GET `url` and return the whole body.

    Makes at most `max_retries + 1` attempts. Waits `backoff_s * 2**n` between
    attempts, or the server's `Retry-After` (in seconds) when it sends one.
    """
    if not url.startswith(("https://", "http://")):
        raise ValueError(f"refusing non-HTTP URL: {url!r}")
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_error = "no attempt made"
    for attempt in range(max_retries + 1):
        retry_after: float | None = None
        try:
            with opener(request, timeout_s) as resp:  # type: ignore[attr-defined]
                body = resp.read()
                return Download(
                    url=url,
                    status=resp.status,
                    content=body,
                    content_type=resp.headers.get("Content-Type", ""),
                )
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_STATUS:
                raise DownloadError(f"GET {url} -> HTTP {exc.code} {exc.reason}") from exc
            last_error = f"HTTP {exc.code} {exc.reason}"
            retry_after = _retry_after(exc.headers.get("Retry-After") if exc.headers else None)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last_error = f"{type(exc).__name__}: {getattr(exc, 'reason', exc)}"
        if attempt < max_retries:
            sleep(retry_after if retry_after is not None else backoff_s * 2**attempt)
    raise DownloadError(f"GET {url} failed after {max_retries + 1} attempts: {last_error}")


def _retry_after(value: str | None, cap_s: float = 60.0) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:  # HTTP-date form: not worth parsing, fall back to backoff
        return None
    return min(max(seconds, 0.0), cap_s)
