"""A scripted fake of the Metabase 0.52 `/api` endpoints the bootstrap and the
dashboard provisioner use. Shared by the `test_bi_*` modules."""

from __future__ import annotations

from railfall.bi.metabase import EXPECTED_TABLES


class FakeMetabase:
    """Answers like Metabase 0.52: health turns ok after `boot_polls`, setup works once
    with the token, and synced tables appear `sync_polls` metadata calls after a sync."""

    def __init__(self, *, boot_polls=0, sync_polls=1, tables=EXPECTED_TABLES):
        self.boot_polls = boot_polls
        self.sync_polls = sync_polls
        self.warehouse_tables = list(tables)
        self.users: dict[str, str] = {}
        self.dbs: list[dict] = [{"id": 1, "name": "Sample Database", "engine": "h2"}]
        self.synced: set[int] = set()
        self.calls: list[tuple[str, str, dict | None, str | None]] = []
        self.collections: dict[int, dict] = {}
        self.cards: dict[int, dict] = {}
        self.dashboards: dict[int, dict] = {}
        self.dashcard_puts = 0
        self.rows_per_card = 3
        self.broken_sql: tuple[str, ...] = ()
        self.empty_tables: tuple[str, ...] = ()
        self._ids = 100

    def __call__(self, method, path, body, session):
        self.calls.append((method, path, body, session))
        if path == "/health":
            if self.boot_polls > 0:
                self.boot_polls -= 1
                return 503, {"status": "initializing", "progress": 0.4}
            return 200, {"status": "ok"}
        if path == "/session/properties":
            props = {"has-user-setup": bool(self.users)}
            if not self.users:
                props["setup-token"] = "tok-123"
            return 200, props
        if method == "POST" and path == "/setup":
            if self.users or body["token"] != "tok-123":
                return 403, {"message": "The /api/setup route can only be used to create the first"}
            self.users[body["user"]["email"]] = body["user"]["password"]
            return 200, {"id": "sess-setup"}
        if method == "POST" and path == "/session":
            if self.users.get(body["username"]) != body["password"]:
                return 401, {"errors": {"password": "did not match stored password"}}
            return 200, {"id": "sess-login"}
        if session is None:
            return 401, "Unauthenticated"
        if method == "GET" and path == "/database":
            return 200, {"data": self.dbs, "total": len(self.dbs)}
        if method == "POST" and path == "/database":
            db = {"id": len(self.dbs) + 1, **body}
            self.dbs.append(db)
            return 200, db
        if method == "PUT" and path.startswith("/database/"):
            db_id = int(path.rsplit("/", 1)[1])
            db = next(d for d in self.dbs if d["id"] == db_id)
            db.update(body)
            return 200, db
        if method == "POST" and path.endswith("/sync_schema"):
            self.synced.add(int(path.split("/")[2]))
            return 200, {"status": "ok"}
        if method == "GET" and "/metadata" in path:
            db_id = int(path.split("/")[2])
            if db_id not in self.synced or self.sync_polls > 0:
                self.sync_polls -= 1
                return 200, {"id": db_id, "tables": []}
            tables = [{"schema": "marts", "name": t} for t in self.warehouse_tables]
            tables.append({"schema": "staging", "name": "stg_sncf__stations"})
            return 200, {"id": db_id, "tables": tables}
        return self._content(method, path, body)

    # -- collections, questions and dashboards -------------------------------

    def _content(self, method, path, body):
        bare, _, query = path.partition("?")
        parts = bare.strip("/").split("/")
        if bare == "/collection" and method == "GET":
            root = {"id": "root", "name": "Our analytics"}
            return 200, [root, *self.collections.values()]
        if bare == "/collection" and method == "POST":
            c = {
                "id": self._next(),
                "name": body["name"],
                "location": "/",
                "archived": False,
                "personal_owner_id": None,
            }
            self.collections[c["id"]] = c
            return 200, c
        if parts[0] == "collection" and parts[-1] == "items":
            coll = int(parts[1])
            model = query.split("models=")[1]
            store = self.cards if model == "card" else self.dashboards
            items = [
                {"id": o["id"], "name": o["name"], "model": model}
                for o in store.values()
                if o["collection_id"] == coll and not o.get("archived")
            ]
            return 200, {"data": items, "total": len(items)}
        if bare == "/card" and method == "POST":
            card = {"id": self._next(), **body}
            self.cards[card["id"]] = card
            return 200, card
        if parts[0] == "card" and len(parts) == 2 and method == "PUT":
            self.cards[int(parts[1])].update(body)
            return 200, self.cards[int(parts[1])]
        if parts[0] == "card" and parts[-1] == "query" and method == "POST":
            card = self.cards[int(parts[1])]
            sql = card["dataset_query"]["native"]["query"]
            if any(bad in sql for bad in self.broken_sql):
                return 202, {"status": "failed", "error": "ERROR: column does not exist"}
            rows = 0 if any(e in sql for e in self.empty_tables) else self.rows_per_card
            return 202, {"status": "completed", "row_count": rows, "data": {"rows": []}}
        if bare == "/dashboard" and method == "POST":
            dash = {"id": self._next(), "dashcards": [], **body}
            self.dashboards[dash["id"]] = dash
            return 200, dash
        if parts[0] == "dashboard" and len(parts) == 2:
            dash = self.dashboards[int(parts[1])]
            if method == "GET":
                return 200, dash
            dashcards = []
            for dc in body.pop("dashcards", []):
                if dc["id"] < 0:
                    dc = {**dc, "id": self._next()}
                dashcards.append(dc)
            dash.update(body, dashcards=dashcards)
            self.dashcard_puts += 1
            return 200, dash
        return 404, {"message": f"unscripted {method} {path}"}

    def _next(self) -> int:
        self._ids += 1
        return self._ids
