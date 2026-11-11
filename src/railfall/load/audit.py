"""Audit log of warehouse loads: one row per (run, table) in `<schema>._load_runs`.

The merge itself leaves no trace of a re-run that changed nothing, which is
exactly what an idempotent load should do — but it also means "did last night's
load run, and what did it do?" has no answer in the warehouse. Every load run
therefore appends what it did per table: scope, row counts, outcome and timing.

The log is written outside the per-table transactions, in autocommit, so a
table whose merge rolled back is still recorded as failed. It is append-only
and keyed by `(run_id, table_name)`; re-recording the same run is a no-op, so
the log itself stays free of duplicates.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from psycopg import sql

if TYPE_CHECKING:
    import psycopg

LOG_TABLE = "_load_runs"

# (column, Postgres type) in insert order; started_at/finished_at bracket one table.
LOG_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "uuid not null"),
    ("table_name", "text not null"),
    ("target", "text not null"),
    ("scope", "text not null"),
    ("ok", "boolean not null"),
    ("staged", "bigint not null"),
    ("inserted", "bigint not null"),
    ("updated", "bigint not null"),
    ("unchanged", "bigint not null"),
    ("deleted", "bigint not null"),
    ("error", "text"),
    ("started_at", "timestamptz not null"),
    ("finished_at", "timestamptz not null"),
)


@dataclass(frozen=True)
class LogEntry:
    run_id: uuid.UUID
    table_name: str
    target: str
    scope: str
    ok: bool
    staged: int
    inserted: int
    updated: int
    unchanged: int
    deleted: int
    error: str | None
    started_at: datetime
    finished_at: datetime

    def as_row(self) -> tuple:
        return tuple(getattr(self, name) for name, _ in LOG_COLUMNS)


def new_run_id() -> uuid.UUID:
    return uuid.uuid4()


def log_table(schema: str = "raw") -> sql.Identifier:
    return sql.Identifier(schema, LOG_TABLE)


def create_log_sql(schema: str = "raw") -> sql.Composed:
    columns = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(kind)) for name, kind in LOG_COLUMNS
    )
    return sql.SQL("create table if not exists {} ({}, primary key (run_id, table_name))").format(
        log_table(schema), columns
    )


def insert_log_sql(schema: str = "raw") -> sql.Composed:
    names = [name for name, _ in LOG_COLUMNS]
    return sql.SQL("insert into {} ({}) values ({}) on conflict do nothing").format(
        log_table(schema),
        sql.SQL(", ").join(map(sql.Identifier, names)),
        sql.SQL(", ").join(sql.Placeholder() * len(names)),
    )


def record(conn: psycopg.Connection, entries: Iterable[LogEntry], *, schema: str = "raw") -> int:
    """Append `entries` to the log (creating it if needed); returns how many were new."""
    rows = [e.as_row() for e in entries]
    conn.execute(sql.SQL("create schema if not exists {}").format(sql.Identifier(schema)))
    conn.execute(create_log_sql(schema))
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(insert_log_sql(schema), rows)
        return max(cur.rowcount, 0)


def last_run(conn: psycopg.Connection, *, schema: str = "raw") -> list[LogEntry]:
    """Entries of the most recent run (by start time), in table order; empty if none."""
    names = [name for name, _ in LOG_COLUMNS]
    query = sql.SQL(
        "select {cols} from {log} where run_id = ("
        "select run_id from {log} order by started_at desc limit 1) order by started_at"
    ).format(cols=sql.SQL(", ").join(map(sql.Identifier, names)), log=log_table(schema))
    return [LogEntry(*row) for row in conn.execute(query).fetchall()]
