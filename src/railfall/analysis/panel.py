"""Read the route-month panel the weather tests run on, straight from the marts.

The statistics in `railfall.analysis.stats` work on plain arrays; this module
turns the dbt marts into those arrays. One row per route x service x month from
`int_route_month_weather`, with its season from `dim_month`, keeping only what a
test can use:

* route-months with weather at one end at least (`weather_coverage <> 'none'`),
  the same population `mart_punctuality_by_weather` compares, so the naive gap
  printed by the analysis matches the dashboard number;
* route-months that actually ran trains, with a known on-time count (a fully
  cancelled month has no punctuality to compare).

Strata are route x service x season: inside one, the trains share a line,
a timetable family and a climate regime, and only *which* months were adverse
varies. Clusters for the bootstrap are routes, the unit that is really
independent.

The SQL is portable (no casts, no placeholders), so unit tests run it on SQLite
with `schema=None`; on Postgres the tables are read from `marts`.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
from numpy.typing import NDArray

# Factor name -> boolean column of int_route_month_weather, in report order.
FACTORS: dict[str, str] = {"rain": "is_wet", "wind": "is_windy", "frost": "is_frosty"}

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")

PANEL_COLUMNS = (
    "route_id",
    "service",
    "month",
    "season",
    "trains_run",
    "trains_on_time",
    *FACTORS.values(),
)


def panel_sql(schema: str | None = "marts") -> str:
    """The panel query, reading `<schema>.int_route_month_weather` and `dim_month`."""
    if schema is not None and not _IDENTIFIER.match(schema):
        raise ValueError(f"invalid schema name: {schema!r}")
    prefix = f"{schema}." if schema else ""
    flags = ", ".join(f"w.{column}" for column in FACTORS.values())
    return f"""
        select w.route_id, w.service, w.month, m.season,
               w.trains_run, w.trains_on_time, {flags}
        from {prefix}int_route_month_weather as w
        inner join {prefix}dim_month as m on m.month = w.month
        where w.weather_coverage <> 'none'
          and w.trains_run > 0
          and w.trains_on_time is not null
        order by w.route_id, w.service, w.month
    """


@dataclass(frozen=True)
class Panel:
    """Route-month arrays, aligned row by row."""

    route_id: NDArray[np.str_]
    service: NDArray[np.str_]
    month: NDArray[np.str_]
    season: NDArray[np.str_]
    trains_run: NDArray[np.float64]
    trains_on_time: NDArray[np.float64]
    adverse: dict[str, NDArray[np.bool_]]

    def __len__(self) -> int:
        return int(self.route_id.size)

    @property
    def strata(self) -> NDArray[np.str_]:
        """route x service x season labels."""
        return np.char.add(
            np.char.add(np.char.add(self.route_id, "|"), np.char.add(self.service, "|")),
            self.season,
        )

    @property
    def clusters(self) -> NDArray[np.str_]:
        return self.route_id

    @property
    def routes(self) -> int:
        return int(np.unique(self.route_id).size) if len(self) else 0

    @property
    def months(self) -> int:
        return int(np.unique(self.month).size) if len(self) else 0


def _flag(value: Any) -> bool:
    # Postgres gives bools; SQLite gives 0/1 (or "true"/"false" if stored as text).
    if isinstance(value, str):
        return value.strip().lower() in {"t", "true", "1"}
    return bool(value)


def build_panel(rows: Iterable[Sequence[Any]]) -> Panel:
    """Panel from rows in `PANEL_COLUMNS` order (any DB-API cursor output)."""
    rows = list(rows)
    width = len(PANEL_COLUMNS)
    if any(len(row) != width for row in rows):
        raise ValueError(f"every panel row needs {width} columns: {', '.join(PANEL_COLUMNS)}")
    cols = list(zip(*rows, strict=True)) if rows else [()] * width
    by_name = dict(zip(PANEL_COLUMNS, cols, strict=True))
    run = np.asarray(by_name["trains_run"], dtype=np.float64)
    on_time = np.asarray(by_name["trains_on_time"], dtype=np.float64)
    if np.any(on_time < 0) or np.any(on_time > run):
        raise ValueError("trains_on_time must lie between 0 and trains_run")
    return Panel(
        route_id=np.asarray([str(v) for v in by_name["route_id"]], dtype=np.str_),
        service=np.asarray([str(v or "") for v in by_name["service"]], dtype=np.str_),
        month=np.asarray([str(v)[:10] for v in by_name["month"]], dtype=np.str_),
        season=np.asarray([str(v) for v in by_name["season"]], dtype=np.str_),
        trains_run=run,
        trains_on_time=on_time,
        adverse={
            factor: np.asarray([_flag(v) for v in by_name[column]], dtype=bool)
            for factor, column in FACTORS.items()
        },
    )


def read_panel(conn: Any, schema: str | None = "marts") -> Panel:
    """Run the panel query on an open DB-API connection (psycopg or sqlite3)."""
    cursor = conn.cursor()
    try:
        cursor.execute(panel_sql(schema))
        return build_panel(cursor.fetchall())
    finally:
        cursor.close()
