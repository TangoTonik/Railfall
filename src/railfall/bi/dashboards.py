"""Dashboards as code: load and validate the specs under `bi/dashboards/`.

Each sub-directory is one Metabase dashboard: a `dashboard.json` (name,
collection, cards and their place on the 24-column grid) and one `.sql` file per
card, a native query on the `marts` schema. Keeping the SQL in plain files makes
it reviewable and diffable; the JSON only says how to show it.

Validation runs before anything touches Metabase, so a typo fails fast in unit
tests and CI rather than half-way through a provisioning run:

- card keys and names are unique within a dashboard;
- `display` is one Metabase knows;
- every SQL file exists, is a single read-only statement, and only reads the
  `marts` tables the card declares, which must be ones the bootstrap syncs;
- cards fit the grid and do not overlap.

Standard library only, like the rest of `railfall.bi`.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

GRID_COLUMNS = 24
DISPLAYS = frozenset({"scalar", "line", "bar", "row", "area", "combo", "pie", "table"})
DEFAULT_ROOT = Path("bi/dashboards")

_TABLE_REF = re.compile(r"\bmarts\.(\w+)", re.IGNORECASE)
_WRITE_KEYWORDS = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|truncate|grant|revoke|copy)\b",
    re.IGNORECASE,
)
_COMMENT = re.compile(r"--[^\n]*")


class DashboardSpecError(ValueError):
    """A dashboard spec is malformed; the message names the file and the card."""


@dataclass(frozen=True)
class Layout:
    row: int
    col: int
    size_x: int
    size_y: int

    def cells(self) -> set[tuple[int, int]]:
        return {
            (r, c)
            for r in range(self.row, self.row + self.size_y)
            for c in range(self.col, self.col + self.size_x)
        }


@dataclass(frozen=True)
class CardSpec:
    key: str
    name: str
    display: str
    sql: str
    tables: tuple[str, ...]
    layout: Layout
    description: str = ""
    visualization_settings: dict = field(default_factory=dict)


@dataclass(frozen=True)
class DashboardSpec:
    slug: str
    name: str
    collection: str
    cards: tuple[CardSpec, ...]
    description: str = ""

    def card(self, key: str) -> CardSpec:
        for c in self.cards:
            if c.key == key:
                return c
        raise KeyError(key)


def _strip_comments(sql: str) -> str:
    return _COMMENT.sub("", sql)


def check_sql(sql: str, declared: Iterable[str], *, where: str) -> None:
    """Single read-only statement that reads exactly the declared marts tables."""
    body = _strip_comments(sql).strip().rstrip(";").strip()
    if not body:
        raise DashboardSpecError(f"{where}: empty query")
    if ";" in body:
        raise DashboardSpecError(f"{where}: more than one statement")
    if not re.match(r"(select|with)\b", body, re.IGNORECASE):
        raise DashboardSpecError(f"{where}: must start with SELECT or WITH")
    if m := _WRITE_KEYWORDS.search(body):
        raise DashboardSpecError(f"{where}: read-only queries only, found {m.group(1).upper()}")
    used = {t.lower() for t in _TABLE_REF.findall(body)}
    if not used:
        raise DashboardSpecError(f"{where}: reads no marts.<table>")
    declared_set = set(declared)
    if used != declared_set:
        raise DashboardSpecError(
            f"{where}: reads {sorted(used)} but declares {sorted(declared_set)}"
        )


def _layout(raw: object, *, where: str) -> Layout:
    if not isinstance(raw, dict):
        raise DashboardSpecError(f"{where}: layout must be an object")
    try:
        lay = Layout(**{k: int(raw[k]) for k in ("row", "col", "size_x", "size_y")})
    except KeyError as exc:
        raise DashboardSpecError(f"{where}: layout is missing {exc.args[0]}") from None
    if lay.row < 0 or lay.col < 0 or lay.size_x < 1 or lay.size_y < 1:
        raise DashboardSpecError(f"{where}: layout has a negative position or empty size")
    if lay.col + lay.size_x > GRID_COLUMNS:
        raise DashboardSpecError(f"{where}: layout overflows the {GRID_COLUMNS}-column grid")
    return lay


def load_dashboard(directory: Path, *, known_tables: Iterable[str] | None = None) -> DashboardSpec:
    """Load `<directory>/dashboard.json` and its SQL files, validating everything."""
    spec_path = directory / "dashboard.json"
    try:
        raw = json.loads(spec_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise DashboardSpecError(f"{spec_path}: not found") from None
    except json.JSONDecodeError as exc:
        raise DashboardSpecError(f"{spec_path}: invalid JSON ({exc})") from None

    for key in ("name", "collection", "cards"):
        if not raw.get(key):
            raise DashboardSpecError(f"{spec_path}: `{key}` is required")
    known = None if known_tables is None else set(known_tables)

    cards: list[CardSpec] = []
    seen_keys: set[str] = set()
    seen_names: set[str] = set()
    occupied: dict[tuple[int, int], str] = {}
    for i, c in enumerate(raw["cards"]):
        where = f"{spec_path} card #{i} ({c.get('key', '?')})"
        for key in ("key", "name", "display", "sql", "tables", "layout"):
            if key not in c:
                raise DashboardSpecError(f"{where}: `{key}` is required")
        if c["key"] in seen_keys:
            raise DashboardSpecError(f"{where}: duplicate key")
        if c["name"] in seen_names:
            raise DashboardSpecError(f"{where}: duplicate name {c['name']!r}")
        if c["display"] not in DISPLAYS:
            raise DashboardSpecError(f"{where}: unknown display {c['display']!r}")
        tables = tuple(c["tables"])
        if known is not None and (unknown := sorted(set(tables) - known)):
            raise DashboardSpecError(f"{where}: tables not synced by the bootstrap: {unknown}")
        sql_path = directory / c["sql"]
        if not sql_path.is_file():
            raise DashboardSpecError(f"{where}: SQL file {sql_path} not found")
        sql = sql_path.read_text(encoding="utf-8").strip()
        check_sql(sql, tables, where=f"{where} {sql_path.name}")
        layout = _layout(c["layout"], where=where)
        for cell in layout.cells():
            if cell in occupied:
                raise DashboardSpecError(f"{where}: overlaps card {occupied[cell]!r}")
            occupied[cell] = c["key"]
        seen_keys.add(c["key"])
        seen_names.add(c["name"])
        cards.append(
            CardSpec(
                key=c["key"],
                name=c["name"],
                display=c["display"],
                sql=sql,
                tables=tables,
                layout=layout,
                description=c.get("description", ""),
                visualization_settings=dict(c.get("visualization_settings") or {}),
            )
        )
    return DashboardSpec(
        slug=directory.name,
        name=raw["name"],
        collection=raw["collection"],
        cards=tuple(cards),
        description=raw.get("description", ""),
    )


def load_dashboards(
    root: Path = DEFAULT_ROOT, *, known_tables: Iterable[str] | None = None
) -> list[DashboardSpec]:
    """Every dashboard under `root`, sorted by directory name. Names must be unique."""
    dirs = sorted(p for p in Path(root).iterdir() if (p / "dashboard.json").is_file())
    if not dirs:
        raise DashboardSpecError(f"{root}: no dashboard.json found")
    known = None if known_tables is None else tuple(known_tables)
    specs = [load_dashboard(d, known_tables=known) for d in dirs]
    names = [s.name for s in specs]
    if dupes := sorted({n for n in names if names.count(n) > 1}):
        raise DashboardSpecError(f"{root}: duplicate dashboard names {dupes}")
    return specs
