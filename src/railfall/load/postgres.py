"""Idempotent upsert of rows into a warehouse table.

One load of one table is a single transaction:

1. `ensure_table` creates the table if needed (primary key = business key) and
   adds any column silver gained since the last load. Columns are never dropped
   or retyped automatically; a type change fails the load.
2. Rows are streamed with `COPY` into a temporary stage table.
3. The stage is merged with `INSERT ... ON CONFLICT (key) DO UPDATE`, and the
   update only fires when a value actually changed (`IS DISTINCT FROM`), so a
   re-run of the same data touches no row and `_updated_at` keeps meaning
   "last time this row changed".
4. Rows of the replaced scope that are no longer in the stage are deleted, so
   the warehouse never keeps a route-month silver has since dropped. The scope
   is the whole table, or only the partitions being loaded.

Re-running a load therefore converges to the same table, whatever happened
before: no duplicate (the key is a primary key), no stale row, no spurious update.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from psycopg import sql

from railfall.load.schema import LOADED_AT, UPDATED_AT, TargetTable, canonical_pg_type

if TYPE_CHECKING:
    import psycopg


@dataclass(frozen=True)
class MergeResult:
    table: str
    staged: int
    inserted: int
    updated: int
    deleted: int

    @property
    def unchanged(self) -> int:
        return self.staged - self.inserted - self.updated

    @property
    def changed(self) -> bool:
        return bool(self.inserted or self.updated or self.deleted)


class LoadError(RuntimeError):
    """The data or the existing table makes the load unsafe; nothing was written."""


def qualified(table: TargetTable) -> sql.Identifier:
    return sql.Identifier(table.schema, table.name)


def _idents(names: Iterable[str]) -> sql.Composed:
    return sql.SQL(", ").join(sql.Identifier(n) for n in names)


def create_table_sql(table: TargetTable) -> sql.Composed:
    columns = [
        sql.SQL("{} {}").format(sql.Identifier(c.name), sql.SQL(c.pg_type)) for c in table.columns
    ]
    columns += [
        sql.SQL("{} timestamptz not null default now()").format(sql.Identifier(LOADED_AT)),
        sql.SQL("{} timestamptz not null default now()").format(sql.Identifier(UPDATED_AT)),
        sql.SQL("primary key ({})").format(_idents(table.key)),
    ]
    return sql.SQL("create table if not exists {} ({})").format(
        qualified(table), sql.SQL(", ").join(columns)
    )


def existing_columns(conn: psycopg.Connection, table: TargetTable) -> dict[str, str]:
    """`{column: format_type}` of the table, empty if it does not exist."""
    rows = conn.execute(
        """
        select a.attname, format_type(a.atttypid, a.atttypmod)
        from pg_attribute a
        join pg_class c on c.oid = a.attrelid
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = %s and c.relname = %s and a.attnum > 0 and not a.attisdropped
        order by a.attnum
        """,
        (table.schema, table.name),
    ).fetchall()
    return {name: kind for name, kind in rows}


def schema_drift(existing: dict[str, str], table: TargetTable) -> tuple[list[str], list[str]]:
    """(columns to add, type conflicts) between an existing table and the wanted one."""
    to_add, conflicts = [], []
    for col in table.columns:
        if col.name not in existing:
            to_add.append(col.name)
            continue
        want = canonical_pg_type(col.pg_type)
        if existing[col.name] != want:
            conflicts.append(f"{col.name}: warehouse has {existing[col.name]}, silver has {want}")
    return to_add, conflicts


def ensure_table(conn: psycopg.Connection, table: TargetTable) -> list[str]:
    """Create the table or add new columns to it. Returns the columns added."""
    existing = existing_columns(conn, table)
    if not existing:
        conn.execute(create_table_sql(table))
        return []
    to_add, conflicts = schema_drift(existing, table)
    if conflicts:
        raise LoadError(f"{table.schema}.{table.name}: " + "; ".join(conflicts))
    types = {c.name: c.pg_type for c in table.columns}
    for name in to_add:
        conn.execute(
            sql.SQL("alter table {} add column {} {}").format(
                qualified(table), sql.Identifier(name), sql.SQL(types[name])
            )
        )
    return to_add


def _stage_name(table: TargetTable) -> sql.Identifier:
    return sql.Identifier(f"_stage_{table.name}")


def stage_rows(conn: psycopg.Connection, table: TargetTable, rows: Iterable[Sequence[Any]]) -> int:
    """COPY rows (in `table.column_names` order) into a temp stage; returns the count."""
    stage = _stage_name(table)
    conn.execute(sql.SQL("drop table if exists {}").format(stage))
    conn.execute(
        sql.SQL("create temp table {} (like {} including defaults) on commit drop").format(
            stage, qualified(table)
        )
    )
    copy = sql.SQL("copy {} ({}) from stdin").format(stage, _idents(table.column_names))
    staged = 0
    with conn.cursor() as cur, cur.copy(copy) as cp:
        for row in rows:
            cp.write_row(row)
            staged += 1
    return staged


def duplicate_keys_sql(table: TargetTable) -> sql.Composed:
    return sql.SQL(
        "select count(*) from (select 1 from {} group by {} having count(*) > 1) d"
    ).format(_stage_name(table), _idents(table.key))


def upsert_sql(table: TargetTable) -> sql.Composed:
    """Insert new keys, update changed rows only, and count both."""
    cols = table.column_names
    values = table.value_columns
    if values:
        assignments = sql.SQL(", ").join(
            sql.SQL("{0} = excluded.{0}").format(sql.Identifier(c)) for c in values
        )
        changed = sql.SQL("({}) is distinct from ({})").format(
            sql.SQL(", ").join(sql.SQL("t.{}").format(sql.Identifier(c)) for c in values),
            sql.SQL(", ").join(sql.SQL("excluded.{}").format(sql.Identifier(c)) for c in values),
        )
        conflict = sql.SQL("do update set {}, {} = now() where {}").format(
            assignments, sql.Identifier(UPDATED_AT), changed
        )
    else:  # key-only table: nothing can change
        conflict = sql.SQL("do nothing")
    return sql.SQL(
        "with upserted as ("
        "insert into {target} as t ({cols}) select {cols} from {stage} "
        "on conflict ({key}) {conflict} "
        "returning (xmax = 0) as inserted) "
        "select count(*) filter (where inserted), count(*) filter (where not inserted) "
        "from upserted"
    ).format(
        target=qualified(table),
        cols=_idents(cols),
        stage=_stage_name(table),
        key=_idents(table.key),
        conflict=conflict,
    )


def delete_missing_sql(
    table: TargetTable, partitions: Sequence[Sequence[Any]] | None
) -> tuple[sql.Composed, list[Any]]:
    """Delete rows of the replaced scope whose key is no longer staged.

    `partitions=None` replaces the whole table; otherwise only rows whose
    `partition_by` values are one of the given tuples are candidates.
    """
    key_match = sql.SQL(" and ").join(
        sql.SQL("s.{0} = t.{0}").format(sql.Identifier(k)) for k in table.key
    )
    query = sql.SQL("delete from {} as t where not exists (select 1 from {} s where {})").format(
        qualified(table), _stage_name(table), key_match
    )
    params: list[Any] = []
    if partitions is not None:
        if not table.partition_by:
            raise ValueError(f"{table.name} is not partitioned; pass partitions=None")
        width = len(table.partition_by)
        if any(len(p) != width for p in partitions):
            raise ValueError(f"{table.name}: partitions must be {width}-tuples")
        row = sql.SQL("({})").format(sql.SQL(", ").join(sql.Placeholder() * width))
        query += sql.SQL(" and ({}) in ({})").format(
            sql.SQL(", ").join(
                sql.SQL("t.{}").format(sql.Identifier(c)) for c in table.partition_by
            ),
            sql.SQL(", ").join([row] * len(partitions)),
        )
        params = [value for p in partitions for value in p]
    return query, params


def merge_rows(
    conn: psycopg.Connection,
    table: TargetTable,
    rows: Iterable[Sequence[Any]],
    *,
    partitions: Sequence[Sequence[Any]] | None = None,
) -> MergeResult:
    """Make the replaced scope of `table` equal to `rows`, in the caller's transaction.

    Raises LoadError (before touching the target) if `rows` repeat a key.
    """
    ensure_table(conn, table)
    staged = stage_rows(conn, table, rows)
    (dupes,) = conn.execute(duplicate_keys_sql(table)).fetchone()
    if dupes:
        raise LoadError(f"{table.name}: {dupes} keys appear more than once in the load")
    inserted, updated = conn.execute(upsert_sql(table)).fetchone()
    if partitions is not None and not partitions:
        deleted = 0  # an empty scope replaces nothing
    else:
        query, params = delete_missing_sql(table, partitions)
        deleted = conn.execute(query, params).rowcount
    return MergeResult(table.name, staged, int(inserted), int(updated), int(deleted))
