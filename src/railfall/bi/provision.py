"""Provision the dashboard specs into Metabase, idempotently, then run every card.

For each spec (see `railfall.bi.dashboards`):

1. find or create the top-level collection named in the spec;
2. create or update one native-SQL question per card, matched by name inside
   that collection; questions in the collection that no spec card claims any
   more (a renamed or removed card) are archived, not deleted;
3. find or create the dashboard and replace its cards with the spec layout,
   reusing the existing dashcard ids so a re-run is a no-op for Metabase;
4. run every question through `/api/card/:id/query` and record its row count,
   so a mart that is empty or a query that no longer compiles fails the run
   instead of showing an empty tile to the first person who opens it.

The spec is the source of truth: edits made in the UI to provisioned questions
and layouts are overwritten on the next run.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass, field

from railfall.bi.dashboards import CardSpec, DashboardSpec
from railfall.bi.metabase import MetabaseClient, MetabaseError


@dataclass
class DashboardReport:
    name: str
    dashboard_id: int
    collection_id: int
    url: str
    dashboard_created: bool
    cards: dict[str, int] = field(default_factory=dict)
    created: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    archived: list[str] = field(default_factory=list)
    row_counts: dict[str, int] = field(default_factory=dict)
    failed: dict[str, str] = field(default_factory=dict)

    @property
    def empty(self) -> list[str]:
        return sorted(k for k, n in self.row_counts.items() if n == 0)

    @property
    def ok(self) -> bool:
        return not self.failed and not self.empty

    def to_dict(self) -> dict:
        return {**asdict(self), "empty": self.empty, "ok": self.ok}


def _items(payload: object) -> list[dict]:
    # Collection items come wrapped as {"data": [...], "total": n} since 0.40.
    if isinstance(payload, dict):
        payload = payload.get("data", [])
    return list(payload)  # type: ignore[arg-type]


def card_body(card: CardSpec, database_id: int, collection_id: int) -> dict:
    return {
        "name": card.name,
        "description": card.description or None,
        "display": card.display,
        "collection_id": collection_id,
        "visualization_settings": card.visualization_settings,
        "dataset_query": {
            "type": "native",
            "database": database_id,
            "native": {"query": card.sql, "template-tags": {}},
        },
    }


class DashboardProvisioner:
    def __init__(self, client: MetabaseClient, *, base_url: str) -> None:
        self.client = client
        self.base_url = base_url.rstrip("/")

    def ensure_collection(self, name: str) -> int:
        for c in _items(self.client.api("GET", "/collection")):
            if (
                c.get("name") == name
                and not c.get("archived")
                and c.get("personal_owner_id") is None
                and c.get("location", "/") == "/"
                and isinstance(c.get("id"), int)
            ):
                return c["id"]
        created = self.client.api("POST", "/collection", {"name": name, "parent_id": None})
        assert isinstance(created, dict)
        return int(created["id"])

    def _collection_items(self, collection_id: int, model: str) -> list[dict]:
        items = _items(self.client.api("GET", f"/collection/{collection_id}/items?models={model}"))
        return [i for i in items if i.get("model", model) == model]

    def sync_cards(
        self, spec: DashboardSpec, database_id: int, collection_id: int, report: DashboardReport
    ) -> None:
        existing = {i["name"]: int(i["id"]) for i in self._collection_items(collection_id, "card")}
        for card in spec.cards:
            body = card_body(card, database_id, collection_id)
            if card.name in existing:
                card_id = existing.pop(card.name)
                self.client.api("PUT", f"/card/{card_id}", body)
                report.updated.append(card.key)
            else:
                created = self.client.api("POST", "/card", body)
                assert isinstance(created, dict)
                card_id = int(created["id"])
                report.created.append(card.key)
            report.cards[card.key] = card_id
        # Whatever is left in the collection was provisioned by an older spec.
        for name, card_id in sorted(existing.items()):
            self.client.api("PUT", f"/card/{card_id}", {"archived": True})
            report.archived.append(name)

    def ensure_dashboard(self, spec: DashboardSpec, collection_id: int) -> tuple[int, bool]:
        for d in self._collection_items(collection_id, "dashboard"):
            if d.get("name") == spec.name:
                return int(d["id"]), False
        created = self.client.api(
            "POST",
            "/dashboard",
            {
                "name": spec.name,
                "description": spec.description or None,
                "collection_id": collection_id,
            },
        )
        assert isinstance(created, dict)
        return int(created["id"]), True

    def layout(self, spec: DashboardSpec, dashboard_id: int, cards: dict[str, int]) -> None:
        current = self.client.api("GET", f"/dashboard/{dashboard_id}")
        assert isinstance(current, dict)
        # Keep the ids of dashcards already showing the right question: Metabase
        # deletes the ones missing from the list and creates those with id < 0.
        reuse = {
            dc["card_id"]: dc["id"] for dc in current.get("dashcards", []) if dc.get("card_id")
        }
        dashcards = []
        for i, card in enumerate(spec.cards, start=1):
            card_id = cards[card.key]
            dashcards.append(
                {
                    "id": reuse.get(card_id, -i),
                    "card_id": card_id,
                    "row": card.layout.row,
                    "col": card.layout.col,
                    "size_x": card.layout.size_x,
                    "size_y": card.layout.size_y,
                    "parameter_mappings": [],
                    "visualization_settings": {},
                }
            )
        self.client.api(
            "PUT",
            f"/dashboard/{dashboard_id}",
            {"name": spec.name, "description": spec.description or None, "dashcards": dashcards},
        )

    def run_cards(self, report: DashboardReport) -> None:
        for key, card_id in report.cards.items():
            try:
                result = self.client.api("POST", f"/card/{card_id}/query", {})
            except MetabaseError as exc:
                report.failed[key] = str(exc)
                continue
            if not isinstance(result, dict) or result.get("status") != "completed":
                error = result.get("error") if isinstance(result, dict) else result
                report.failed[key] = str(error or "query did not complete")[:500]
                continue
            rows = result.get("row_count")
            if rows is None:
                rows = len(result.get("data", {}).get("rows", []))
            report.row_counts[key] = int(rows)

    def provision(self, spec: DashboardSpec, database_id: int) -> DashboardReport:
        collection_id = self.ensure_collection(spec.collection)
        dashboard_id, created = self.ensure_dashboard(spec, collection_id)
        report = DashboardReport(
            name=spec.name,
            dashboard_id=dashboard_id,
            collection_id=collection_id,
            url=f"{self.base_url}/dashboard/{dashboard_id}",
            dashboard_created=created,
        )
        self.sync_cards(spec, database_id, collection_id, report)
        self.layout(spec, dashboard_id, report.cards)
        self.run_cards(report)
        return report

    def provision_all(
        self, specs: Iterable[DashboardSpec], database_id: int
    ) -> list[DashboardReport]:
        return [self.provision(s, database_id) for s in specs]
