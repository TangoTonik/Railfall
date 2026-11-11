"""Warehouse upsert: SQL rendering (pure) and behaviour on a real Postgres (pg_schema)."""

from datetime import date

import pytest

from railfall.load.postgres import (
    LoadError,
    create_table_sql,
    delete_missing_sql,
    ensure_table,
    merge_rows,
    schema_drift,
    upsert_sql,
)
from railfall.load.schema import target_table

SPARK_SCHEMA = [
    ("month", "date"),
    ("year", "int"),
    ("route_id", "string"),
    ("trains_run", "bigint"),
    ("rate", "double"),
]


def table(schema: str = "raw", columns=SPARK_SCHEMA):
    return target_table(
        "punctuality_monthly",
        columns,
        key=("month", "route_id"),
        partition_by=("year",),
        schema=schema,
    )


ROWS = [
    (date(2024, 1, 1), 2024, "PARIS-LYON", 400, 91.5),
    (date(2024, 1, 1), 2024, "PARIS-MARSEILLE", 300, 85.0),
    (date(2025, 1, 1), 2025, "PARIS-LYON", 410, 88.0),
]


# --------------------------------------------------------------------------- pure


def test_create_table_has_key_as_primary_key_and_audit_columns():
    text = create_table_sql(table()).as_string(None)
    assert text.startswith('create table if not exists "raw"."punctuality_monthly"')
    assert '"rate" double precision' in text
    assert '"_loaded_at" timestamptz not null default now()' in text
    assert text.endswith('primary key ("month", "route_id"))')


def test_upsert_only_updates_rows_whose_values_changed():
    text = upsert_sql(table()).as_string(None)
    assert 'on conflict ("month", "route_id") do update set' in text
    assert '"rate" = excluded."rate"' in text
    assert '"month" = excluded' not in text  # keys are never rewritten
    assert 'is distinct from (excluded."year"' in text
    assert "returning (xmax = 0) as inserted" in text


def test_upsert_of_a_key_only_table_does_nothing_on_conflict():
    key_only = target_table("k", [("a", "string")], key=("a",))
    assert 'on conflict ("a") do nothing' in upsert_sql(key_only).as_string(None)


def test_delete_scope_whole_table_or_given_partitions():
    whole, params = delete_missing_sql(table(), None)
    assert " in (" not in whole.as_string(None)
    assert params == []

    scoped, params = delete_missing_sql(table(), [(2024,), (2025,)])
    assert scoped.as_string(None).endswith(' and (t."year") in ((%s), (%s))')
    assert params == [2024, 2025]


def test_delete_scope_validates_partitions():
    with pytest.raises(ValueError, match="1-tuples"):
        delete_missing_sql(table(), [(2024, "75")])
    unpartitioned = target_table("s", [("a", "string")], key=("a",))
    with pytest.raises(ValueError, match="not partitioned"):
        delete_missing_sql(unpartitioned, [("x",)])


def test_schema_drift_lists_new_columns_and_type_conflicts():
    existing = {"month": "date", "year": "integer", "route_id": "text", "trains_run": "integer"}
    to_add, conflicts = schema_drift(existing, table())
    assert to_add == ["rate"]
    assert conflicts == ["trains_run: warehouse has integer, silver has bigint"]


# --------------------------------------------------------------------------- Postgres


@pytest.fixture
def conn(pg_schema):
    import psycopg

    dsn, _ = pg_schema
    with psycopg.connect(dsn) as connection:
        yield connection


def count(conn, schema):
    return conn.execute(f"select count(*) from {schema}.punctuality_monthly").fetchone()[0]


def test_merge_is_idempotent(conn, pg_schema):
    _, schema = pg_schema
    first = merge_rows(conn, table(schema), ROWS)
    conn.commit()
    assert (first.staged, first.inserted, first.updated, first.deleted) == (3, 3, 0, 0)

    again = merge_rows(conn, table(schema), ROWS)
    conn.commit()
    assert (again.inserted, again.updated, again.deleted, again.unchanged) == (0, 0, 0, 3)
    assert not again.changed
    assert count(conn, schema) == 3


def test_merge_updates_changed_rows_and_bumps_updated_at(conn, pg_schema):
    _, schema = pg_schema
    merge_rows(conn, table(schema), ROWS)
    conn.commit()
    changed = [ROWS[0][:-1] + (70.0,), *ROWS[1:]]
    result = merge_rows(conn, table(schema), changed)
    conn.commit()
    assert (result.inserted, result.updated) == (0, 1)
    rate, bumped = conn.execute(
        f"select rate, _updated_at > _loaded_at from {schema}.punctuality_monthly "
        "where route_id = 'PARIS-LYON' and year = 2024"
    ).fetchone()
    assert rate == 70.0 and bumped


def test_merge_deletes_only_inside_the_replaced_partitions(conn, pg_schema):
    _, schema = pg_schema
    merge_rows(conn, table(schema), ROWS)
    # Reload 2024 without PARIS-MARSEILLE: it goes, 2025 is out of scope and stays.
    result = merge_rows(conn, table(schema), [ROWS[0]], partitions=[(2024,)])
    conn.commit()
    assert result.deleted == 1
    routes = conn.execute(
        f"select year, route_id from {schema}.punctuality_monthly order by 1, 2"
    ).fetchall()
    assert routes == [(2024, "PARIS-LYON"), (2025, "PARIS-LYON")]


def test_merge_refuses_duplicate_keys_without_writing(conn, pg_schema):
    _, schema = pg_schema
    with pytest.raises(LoadError, match="1 keys appear more than once"):
        merge_rows(conn, table(schema), [ROWS[0], ROWS[0]])
    conn.rollback()
    exists = conn.execute("select to_regclass(%s)", (f"{schema}.punctuality_monthly",)).fetchone()[
        0
    ]
    assert exists is None


def test_ensure_table_adds_new_silver_columns_and_refuses_retypes(conn, pg_schema):
    _, schema = pg_schema
    merge_rows(conn, table(schema), ROWS)
    conn.commit()
    wider = table(schema, [*SPARK_SCHEMA, ("pct_cause_external", "double")])
    assert ensure_table(conn, wider) == ["pct_cause_external"]
    conn.commit()

    retyped = table(schema, [*SPARK_SCHEMA[:3], ("trains_run", "string"), SPARK_SCHEMA[4]])
    with pytest.raises(LoadError, match="trains_run: warehouse has bigint, silver has text"):
        ensure_table(conn, retyped)
