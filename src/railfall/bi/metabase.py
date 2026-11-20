"""Bootstrap a fresh Metabase through its REST API, idempotently.

Used by `make dashboards` and the e2e workflow, so Metabase never needs a click
in the setup wizard:

    python -m railfall.bi.metabase bootstrap --report metabase.json
    python -m railfall.bi.metabase check --manifest dbt/target/manifest.json

Steps: wait for `/api/health`, create the admin account with the one-time
setup token when nobody has done it yet (otherwise log in), register the
warehouse as `railfall_reader` restricted to the `marts` schema (created once,
updated in place on later runs), sync its schema and wait until every expected
mart is visible to Metabase. Finally the dashboard specs under `bi/dashboards/`
are provisioned and every card is run (see `railfall.bi.provision`).

`check` validates the specs without calling Metabase at all and, given a dbt
manifest, that every table they read is one dbt builds in `marts`.

Only the standard library is used, like the Airflow client: it runs on the CI
host next to the stack.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from railfall.bi.dashboards import DashboardSpec

# Marts the dashboards read. The bootstrap waits until Metabase has synced them all.
EXPECTED_TABLES: tuple[str, ...] = (
    "fact_route_month",
    "mart_punctuality_by_weather",
    "mart_punctuality_by_weather_intensity",
    "mart_punctuality_by_region_season",
)

# Transport(method, path, body, session) -> (status, payload). Path is under /api.
Transport = Callable[[str, str, dict | None, str | None], tuple[int, object]]


class MetabaseError(RuntimeError):
    """Metabase is unreachable, refused a call, or never synced the marts."""


@dataclass(frozen=True)
class MetabaseSettings:
    url: str = "http://localhost:3000"
    admin_email: str = "admin@railfall.local"
    admin_password: str = field(default="railfall-metabase1", repr=False)
    # The warehouse as Metabase sees it: from inside the compose network.
    warehouse_name: str = "Railfall warehouse"
    warehouse_host: str = "postgres"
    warehouse_port: int = 5432
    warehouse_db: str = "warehouse"
    reader_user: str = "railfall_reader"
    reader_password: str = field(default="reader-local", repr=False)
    schema: str = "marts"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> MetabaseSettings:
        env = os.environ if env is None else env
        d = cls()
        port = env.get("METABASE_PORT", "3000")
        return cls(
            url=env.get("METABASE_URL", f"http://localhost:{port}").rstrip("/"),
            admin_email=env.get("METABASE_ADMIN_EMAIL", d.admin_email),
            admin_password=env.get("METABASE_ADMIN_PASSWORD", d.admin_password),
            warehouse_name=env.get("METABASE_WAREHOUSE_NAME", d.warehouse_name),
            warehouse_host=env.get("METABASE_WAREHOUSE_HOST", d.warehouse_host),
            warehouse_port=int(env.get("METABASE_WAREHOUSE_PORT", d.warehouse_port)),
            warehouse_db=env.get("POSTGRES_DB", d.warehouse_db),
            reader_user=env.get("READER_USER", d.reader_user),
            reader_password=env.get("READER_PASSWORD")
            or env.get("TF_VAR_warehouse_reader_password", d.reader_password),
        )


def http_transport(base_url: str, *, timeout_s: float = 60.0) -> Transport:
    """JSON transport for `/api`; sends the session header when given one."""

    def call(method: str, path: str, body: dict | None, session: str | None) -> tuple[int, object]:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if session:
            headers["X-Metabase-Session"] = session
        req = urllib.request.Request(
            f"{base_url.rstrip('/')}/api{path}",
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers,
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                raw = resp.read()
                status = resp.status
        except urllib.error.HTTPError as exc:
            raw, status = exc.read(), exc.code
        if not raw:
            return status, {}
        try:
            return status, json.loads(raw)
        except json.JSONDecodeError:
            # Metabase answers some errors (and a few successes) as plain text.
            return status, {"message": raw.decode(errors="replace")[:500]}

    return call


@dataclass
class BootstrapReport:
    url: str
    created_admin: bool
    database_id: int
    database_created: bool
    tables: list[str]
    missing: list[str]
    dashboards: list[dict] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.missing and all(d["ok"] for d in self.dashboards)

    def to_dict(self) -> dict:
        return {**asdict(self), "ok": self.ok}


class MetabaseClient:
    def __init__(
        self,
        transport: Transport,
        *,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._call = transport
        self._sleep = sleep
        self._clock = clock
        self.session: str | None = None

    def _ok(self, method: str, path: str, body: dict | None = None) -> object:
        status, payload = self._call(method, path, body, self.session)
        if status >= 400:
            detail = payload.get("message") if isinstance(payload, dict) else None
            raise MetabaseError(f"{method} {path} -> HTTP {status}: {detail or payload}")
        return payload

    def api(self, method: str, path: str, body: dict | None = None) -> object:
        """Authenticated call under /api; raises MetabaseError on HTTP >= 400."""
        return self._ok(method, path, body)

    def wait_healthy(self, *, timeout_s: float = 300.0, poll_s: float = 5.0) -> None:
        deadline = self._clock() + timeout_s
        last: object = None
        while True:
            try:
                status, payload = self._call("GET", "/health", None, None)
                if status == 200 and isinstance(payload, dict) and payload.get("status") == "ok":
                    return
                last = payload
            except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
                last = exc
            if self._clock() >= deadline:
                raise MetabaseError(f"Metabase not healthy after {timeout_s:.0f}s: {last}")
            self._sleep(poll_s)

    def login(self, email: str, password: str) -> bool:
        """Log in, creating the admin first on a fresh instance. True if it was created."""
        props = self._ok("GET", "/session/properties")
        assert isinstance(props, dict)
        if not props.get("has-user-setup"):
            token = props.get("setup-token")
            if not token:
                raise MetabaseError("fresh Metabase but no setup-token in session properties")
            payload = self._ok(
                "POST",
                "/setup",
                {
                    "token": token,
                    "user": {
                        "first_name": "Railfall",
                        "last_name": "Admin",
                        "email": email,
                        "password": password,
                        "site_name": "Railfall",
                    },
                    "prefs": {
                        "site_name": "Railfall",
                        "site_locale": "en",
                        "allow_tracking": False,
                    },
                },
            )
            assert isinstance(payload, dict)
            self.session = payload["id"]
            return True
        payload = self._ok("POST", "/session", {"username": email, "password": password})
        assert isinstance(payload, dict)
        self.session = payload["id"]
        return False

    def databases(self) -> list[dict]:
        payload = self._ok("GET", "/database")
        # Since 0.40 the list is wrapped in {"data": [...], "total": n}.
        if isinstance(payload, dict):
            payload = payload.get("data", [])
        return list(payload)  # type: ignore[arg-type]

    def ensure_database(self, s: MetabaseSettings) -> tuple[int, bool]:
        """Register the warehouse (or refresh its connection details). (id, created)."""
        details = {
            "host": s.warehouse_host,
            "port": s.warehouse_port,
            "dbname": s.warehouse_db,
            "user": s.reader_user,
            "password": s.reader_password,
            "ssl": False,
            "schema-filters-type": "inclusion",
            "schema-filters-patterns": s.schema,
        }
        body = {
            "engine": "postgres",
            "name": s.warehouse_name,
            "details": details,
            "is_full_sync": True,
            "auto_run_queries": True,
        }
        for db in self.databases():
            if db.get("name") == s.warehouse_name and db.get("engine") == "postgres":
                self._ok("PUT", f"/database/{db['id']}", body)
                return int(db["id"]), False
        created = self._ok("POST", "/database", body)
        assert isinstance(created, dict)
        return int(created["id"]), True

    def tables(self, database_id: int, schema: str) -> list[str]:
        meta = self._ok("GET", f"/database/{database_id}/metadata?include_hidden=true")
        assert isinstance(meta, dict)
        return sorted(t["name"] for t in meta.get("tables", []) if t.get("schema") == schema)

    def sync_and_wait(
        self,
        database_id: int,
        schema: str,
        expected: Iterable[str],
        *,
        timeout_s: float = 300.0,
        poll_s: float = 5.0,
    ) -> tuple[list[str], list[str]]:
        """Trigger a schema sync, then poll until every expected table is known."""
        expected = list(expected)
        self._ok("POST", f"/database/{database_id}/sync_schema")
        deadline = self._clock() + timeout_s
        while True:
            seen = self.tables(database_id, schema)
            missing = [t for t in expected if t not in seen]
            if not missing or self._clock() >= deadline:
                return seen, missing
            self._sleep(poll_s)

    def bootstrap(
        self,
        s: MetabaseSettings,
        *,
        expected: Iterable[str] = EXPECTED_TABLES,
        dashboards: Sequence[DashboardSpec] = (),
        health_timeout_s: float = 300.0,
        sync_timeout_s: float = 300.0,
        poll_s: float = 5.0,
    ) -> BootstrapReport:
        self.wait_healthy(timeout_s=health_timeout_s, poll_s=poll_s)
        created_admin = self.login(s.admin_email, s.admin_password)
        db_id, db_created = self.ensure_database(s)
        seen, missing = self.sync_and_wait(
            db_id, s.schema, expected, timeout_s=sync_timeout_s, poll_s=poll_s
        )
        report = BootstrapReport(
            url=s.url,
            created_admin=created_admin,
            database_id=db_id,
            database_created=db_created,
            tables=seen,
            missing=missing,
        )
        # Dashboards on missing marts would only fail card by card; skip them.
        if dashboards and not missing:
            from railfall.bi.provision import DashboardProvisioner

            provisioner = DashboardProvisioner(self, base_url=s.url)
            report.dashboards = [d.to_dict() for d in provisioner.provision_all(dashboards, db_id)]
        return report


def _format(r: BootstrapReport) -> str:
    return "\n".join(
        [
            f"[{'ok' if r.ok else 'FAIL':>4}] Metabase {r.url}",
            f"       admin {'created' if r.created_admin else 'existing'}, "
            f"warehouse db #{r.database_id} {'created' if r.database_created else 'updated'}",
            f"       {len(r.tables)} tables synced"
            + (f", missing: {', '.join(r.missing)}" if r.missing else ""),
            *(_format_dashboard(d) for d in r.dashboards),
        ]
    )


def _format_dashboard(d: dict) -> str:
    line = (
        f"       dashboard {d['name']!r} -> {d['url']}: {len(d['cards'])} cards "
        f"({len(d['created'])} created, {len(d['updated'])} updated, "
        f"{len(d['archived'])} archived)"
    )
    problems = [f"{k} failed: {e}" for k, e in d["failed"].items()]
    problems += [f"{k} returned no rows" for k in d["empty"]]
    return "\n".join([line, *(f"         ! {p}" for p in problems)])


def _check(root: Path, manifest: Path | None) -> int:
    """Offline: specs are valid and, with a manifest, only read marts dbt builds."""
    from railfall.bi.dashboards import (
        DashboardSpecError,
        check_manifest,
        load_dashboards,
        manifest_tables,
    )

    try:
        specs = load_dashboards(root, known_tables=EXPECTED_TABLES)
        built = manifest_tables(manifest) if manifest else None
    except DashboardSpecError as exc:
        print(f"[FAIL] dashboard specs: {exc}", file=sys.stderr)
        return 1
    problems = check_manifest(specs, built) if built is not None else {}
    unsynced = sorted(set(EXPECTED_TABLES) - built) if built is not None else []
    for spec in specs:
        print(f"[  ok] {spec.slug}: {spec.name!r}, {len(spec.cards)} cards")
    for card, missing in problems.items():
        print(f"[FAIL] {card} reads {', '.join(missing)}, not built by dbt", file=sys.stderr)
    for table in unsynced:
        print(f"[FAIL] bootstrap waits for {table}, not built by dbt", file=sys.stderr)
    if built is not None and not problems and not unsynced:
        print(f"[  ok] every card reads one of the {len(built)} relations dbt builds in marts")
    return 1 if problems or unsynced else 0


def main(argv: list[str] | None = None, *, client: MetabaseClient | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["bootstrap", "check"])
    parser.add_argument("--timeout", type=float, default=300.0, help="health and sync timeout (s)")
    parser.add_argument("--poll", type=float, default=5.0)
    parser.add_argument("--report", default=None, help="also write the JSON report to this file")
    parser.add_argument(
        "--dashboards",
        type=Path,
        default=Path("bi/dashboards"),
        help="directory of dashboard specs to provision",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="check: also require every card's tables in this dbt manifest.json",
    )
    parser.add_argument(
        "--skip-dashboards", action="store_true", help="only set up admin and warehouse"
    )
    args = parser.parse_args(argv)

    from railfall.bi.dashboards import DashboardSpecError, load_dashboards

    if args.command == "check":
        return _check(args.dashboards, args.manifest)

    specs: list[DashboardSpec] = []
    if not args.skip_dashboards:
        try:
            specs = load_dashboards(args.dashboards, known_tables=EXPECTED_TABLES)
        except DashboardSpecError as exc:
            print(f"[FAIL] dashboard specs: {exc}", file=sys.stderr)
            return 1

    settings = MetabaseSettings.from_env()
    client = client or MetabaseClient(http_transport(settings.url))
    try:
        report = client.bootstrap(
            settings,
            dashboards=specs,
            health_timeout_s=args.timeout,
            sync_timeout_s=args.timeout,
            poll_s=args.poll,
        )
    except (MetabaseError, urllib.error.URLError) as exc:
        print(f"[FAIL] Metabase bootstrap: {exc}", file=sys.stderr)
        return 1
    if args.report:
        with open(args.report, "w") as fh:
            fh.write(json.dumps(report.to_dict(), indent=2) + "\n")
    print(_format(report))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
