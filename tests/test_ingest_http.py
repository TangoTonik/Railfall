"""Unit tests for railfall.ingest.http. No network: the opener is faked."""

from __future__ import annotations

import io
import urllib.error
from email.message import Message

import pytest

from railfall.ingest.http import USER_AGENT, DownloadError, build_url, fetch


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200, content_type: str = "application/json"):
        self._body = body
        self.status = status
        self.headers = Message()
        self.headers["Content-Type"] = content_type

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def _http_error(code: int, retry_after: str | None = None) -> urllib.error.HTTPError:
    headers = Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return urllib.error.HTTPError("https://x", code, "err", headers, io.BytesIO(b""))


class ScriptedOpener:
    """Replays a list of outcomes: exceptions are raised, bytes are returned."""

    def __init__(self, *outcomes: object) -> None:
        self.outcomes = list(outcomes)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append((request, timeout))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return FakeResponse(outcome)


def test_fetch_returns_body_and_sends_user_agent() -> None:
    opener = ScriptedOpener(b"[1, 2]")
    result = fetch("https://example.org/a.json", timeout_s=7, opener=opener)
    assert result.content == b"[1, 2]"
    assert result.status == 200
    assert result.content_type == "application/json"
    request, timeout = opener.requests[0]
    assert timeout == 7
    assert request.get_header("User-agent") == USER_AGENT


def test_fetch_retries_transient_errors_with_exponential_backoff() -> None:
    sleeps: list[float] = []
    opener = ScriptedOpener(_http_error(503), urllib.error.URLError("reset"), TimeoutError(), b"ok")
    result = fetch(
        "https://e.org", max_retries=3, backoff_s=0.5, sleep=sleeps.append, opener=opener
    )
    assert result.content == b"ok"
    assert sleeps == [0.5, 1.0, 2.0]


def test_fetch_honours_retry_after_and_caps_it() -> None:
    sleeps: list[float] = []
    opener = ScriptedOpener(_http_error(429, "5"), _http_error(429, "3600"), b"ok")
    fetch("https://e.org", sleep=sleeps.append, opener=opener)
    assert sleeps == [5.0, 60.0]


def test_fetch_fails_fast_on_client_errors() -> None:
    sleeps: list[float] = []
    opener = ScriptedOpener(_http_error(404))
    with pytest.raises(DownloadError, match="HTTP 404"):
        fetch("https://e.org", sleep=sleeps.append, opener=opener)
    assert sleeps == []
    assert len(opener.requests) == 1


def test_fetch_gives_up_after_max_retries() -> None:
    opener = ScriptedOpener(*[_http_error(502)] * 3)
    with pytest.raises(DownloadError, match="after 3 attempts: HTTP 502"):
        fetch("https://e.org", max_retries=2, sleep=lambda _s: None, opener=opener)


def test_fetch_rejects_non_http_urls() -> None:
    with pytest.raises(ValueError, match="non-HTTP"):
        fetch("file:///etc/passwd")


def test_build_url_encodes_params() -> None:
    assert build_url("https://e.org/x") == "https://e.org/x"
    assert (
        build_url("https://e.org/x", {"where": "date >= '2024-01'", "limit": -1})
        == "https://e.org/x?where=date+%3E%3D+%272024-01%27&limit=-1"
    )
