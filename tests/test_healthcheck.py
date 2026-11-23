"""Unit tests for railfall.healthcheck. No Docker needed: services are faked or unreachable."""

from __future__ import annotations

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from railfall import healthcheck
from railfall.config import Settings
from railfall.healthcheck import CheckResult, run_checks


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _flaky(name: str, fails: int):
    calls = {"n": 0}

    def check(_settings: Settings) -> CheckResult:
        calls["n"] += 1
        return CheckResult(name, calls["n"] > fails, f"call {calls['n']}")

    return check, calls


def test_run_checks_retries_only_failing_checks():
    clock = FakeClock()
    healthy, healthy_calls = _flaky("pg", fails=0)
    slow, slow_calls = _flaky("minio", fails=2)

    results = run_checks(
        Settings(), [healthy, slow], wait_s=30, interval_s=3, sleep=clock.sleep, clock=clock
    )

    assert all(r.ok for r in results)
    assert healthy_calls["n"] == 1
    assert slow_calls["n"] == 3
    assert clock.now == 6


def test_run_checks_gives_up_after_wait():
    clock = FakeClock()
    never, calls = _flaky("pg", fails=10**6)

    results = run_checks(
        Settings(), [never], wait_s=10, interval_s=3, sleep=clock.sleep, clock=clock
    )

    assert [r.ok for r in results] == [False]
    assert calls["n"] == 5  # t=0, 3, 6, 9, 12
    assert clock.now >= 10


def test_run_checks_without_wait_runs_once():
    never, calls = _flaky("pg", fails=1)
    results = run_checks(Settings(), [never], sleep=lambda _: pytest.fail("must not sleep"))
    assert not results[0].ok
    assert calls["n"] == 1


@pytest.fixture
def live_endpoint():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib API
            self.send_response(200 if self.path == "/minio/health/live" else 404)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/"
    server.shutdown()


def test_minio_live_ok(live_endpoint):
    result = healthcheck.check_minio_live(Settings(lake_endpoint_url=live_endpoint))
    assert result == CheckResult("minio", True, "HTTP 200")


def test_minio_live_unreachable():
    settings = Settings(lake_endpoint_url=f"http://127.0.0.1:{_free_port()}")
    result = healthcheck.check_minio_live(settings)
    assert result.name == "minio" and not result.ok and result.detail


def test_postgres_unreachable_is_reported_not_raised():
    dsn = f"postgresql://rail:rail@127.0.0.1:{_free_port()}/warehouse"
    result = healthcheck.check_postgres(Settings(warehouse_dsn=dsn))
    assert result.name == "postgres" and not result.ok
    assert "\n" not in result.detail


def test_lake_bucket_unreachable_is_reported_not_raised():
    settings = Settings(lake_endpoint_url=f"http://127.0.0.1:{_free_port()}")
    result = healthcheck.check_lake_bucket(settings)
    assert result.name == "lake-bucket" and not result.ok


@pytest.mark.parametrize(("ok", "code"), [(True, 0), (False, 1)])
def test_main_exit_code(monkeypatch, capsys, ok, code):
    monkeypatch.setattr(
        healthcheck, "run_checks", lambda *a, **k: [CheckResult("pg", ok, "detail")]
    )
    assert healthcheck.main([]) == code
    assert "pg" in capsys.readouterr().out


@pytest.fixture
def fake_lake(monkeypatch):
    """In-memory S3 (moto) with the lake bucket created."""
    moto = pytest.importorskip("moto")
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    with moto.mock_aws():
        # No endpoint override: moto only intercepts the default AWS endpoints.
        settings = Settings(lake_endpoint_url=None)  # type: ignore[arg-type]
        client = healthcheck._s3_client(settings)
        client.create_bucket(Bucket=settings.lake_bucket)
        yield settings, client


def test_lake_roundtrip_leaves_nothing_behind(fake_lake):
    settings, client = fake_lake
    result = healthcheck.check_lake_roundtrip(settings)
    assert result.ok, result.detail
    listed = client.list_objects_v2(Bucket=settings.lake_bucket, Prefix=healthcheck.SMOKE_PREFIX)
    assert listed["KeyCount"] == 0


def test_lake_roundtrip_missing_bucket(fake_lake):
    settings, _ = fake_lake
    missing = Settings(lake_endpoint_url=None, lake_bucket="no-such-bucket")  # type: ignore[arg-type]
    result = healthcheck.check_lake_roundtrip(missing)
    assert result.name == "lake-write" and not result.ok


def test_warehouse_write_unreachable_is_reported_not_raised():
    dsn = f"postgresql://rail:rail@127.0.0.1:{_free_port()}/warehouse"
    result = healthcheck.check_warehouse_write(Settings(warehouse_dsn=dsn))
    assert result.name == "warehouse-write" and not result.ok


@pytest.fixture
def json_endpoint():
    """Serve canned JSON bodies by path: `routes[path] = (status, body)`."""
    routes: dict[str, tuple[int, object]] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib API
            status, body = routes.get(self.path, (404, {"error": "not found"}))
            payload = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", routes
    server.shutdown()


def _airflow_health(scheduler: str, metadatabase: str = "healthy") -> dict:
    return {
        "metadatabase": {"status": metadatabase},
        "scheduler": {"status": scheduler, "latest_scheduler_heartbeat": "2026-11-23T10:00:00"},
        "triggerer": {"status": None},
    }


def test_airflow_healthy(json_endpoint):
    url, routes = json_endpoint
    routes["/health"] = (200, _airflow_health("healthy"))
    result = healthcheck.check_airflow(Settings(airflow_url=url))
    assert result == CheckResult("airflow", True, "metadatabase healthy, scheduler healthy")


def test_airflow_dead_scheduler_fails_even_with_http_200(json_endpoint):
    url, routes = json_endpoint
    routes["/health"] = (200, _airflow_health("unhealthy"))
    result = healthcheck.check_airflow(Settings(airflow_url=url))
    assert not result.ok and "scheduler unhealthy" in result.detail


def test_airflow_non_json_body_fails(json_endpoint):
    url, routes = json_endpoint
    routes["/health"] = (200, b"<html>booting</html>")
    result = healthcheck.check_airflow(Settings(airflow_url=url))
    assert not result.ok and "unexpected body" in result.detail


def test_airflow_missing_component_fails(json_endpoint):
    url, routes = json_endpoint
    routes["/health"] = (200, {"metadatabase": {"status": "healthy"}})
    result = healthcheck.check_airflow(Settings(airflow_url=url))
    assert not result.ok and "scheduler missing" in result.detail


def test_airflow_unreachable():
    result = healthcheck.check_airflow(Settings(airflow_url=f"http://127.0.0.1:{_free_port()}"))
    assert result.name == "airflow" and not result.ok and result.detail


def test_metabase_ok(json_endpoint):
    url, routes = json_endpoint
    routes["/api/health"] = (200, {"status": "ok"})
    result = healthcheck.check_metabase(Settings(metabase_url=url))
    assert result == CheckResult("metabase", True, "HTTP 200, ok")


def test_metabase_still_migrating_is_not_ok(json_endpoint):
    """Metabase answers 503 {"status": "initializing"} until its migrations finish."""
    url, routes = json_endpoint
    routes["/api/health"] = (503, {"status": "initializing", "progress": 0.4})
    result = healthcheck.check_metabase(Settings(metabase_url=url))
    assert result.name == "metabase" and not result.ok and "503" in result.detail


def test_metabase_url_follows_the_port():
    assert Settings.from_env({"METABASE_PORT": "3100"}).metabase_url == "http://localhost:3100"
    env = {"METABASE_URL": "http://bi.local:3000/"}
    assert Settings.from_env(env).metabase_url == "http://bi.local:3000"


@pytest.mark.parametrize(
    ("flag", "expected"),
    [([], 3), (["--write"], 5), (["--all"], 5), (["--all", "--write"], 7)],
)
def test_main_flags_add_checks(monkeypatch, flag, expected):
    seen = {}

    def fake_run(settings, checks, **kwargs):
        seen["checks"] = checks
        return [CheckResult("x", True, "")]

    monkeypatch.setattr(healthcheck, "run_checks", fake_run)
    assert healthcheck.main(flag) == 0
    assert len(seen["checks"]) == expected
