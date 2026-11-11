"""Load audit log: SQL rendering (pure) and append-only behaviour on a real Postgres."""

from datetime import UTC, datetime, timedelta

import pytest

from railfall.load import audit

T0 = datetime(2026, 11, 11, 13, 0, tzinfo=UTC)


def entry(run_id, table="stations", *, ok=True, start=T0, **counts):
    values = {"staged": 3, "inserted": 3, "updated": 0, "unchanged": 0, "deleted": 0} | counts
    return audit.LogEntry(
        run_id=run_id,
        table_name=table,
        target=f"raw.{table}",
        scope="full",
        ok=ok,
        error=None if ok else "LoadError: boom",
        started_at=start,
        finished_at=start + timedelta(seconds=2),
        **values,
    )


# --------------------------------------------------------------------------- pure


def test_log_is_keyed_by_run_and_table():
    text = audit.create_log_sql("raw").as_string(None)
    assert text.startswith('create table if not exists "raw"."_load_runs"')
    assert '"run_id" uuid not null' in text
    assert text.endswith("primary key (run_id, table_name))")


def test_insert_is_idempotent_and_matches_columns():
    text = audit.insert_log_sql("raw").as_string(None)
    assert text.endswith("on conflict do nothing")
    assert text.count("%s") == len(audit.LOG_COLUMNS)


def test_entry_row_follows_column_order():
    run = audit.new_run_id()
    row = entry(run).as_row()
    assert len(row) == len(audit.LOG_COLUMNS)
    assert row[0] == run and row[1] == "stations" and row[-1] == T0 + timedelta(seconds=2)


# --------------------------------------------------------------------------- real Postgres


@pytest.fixture
def conn(pg_schema):
    import psycopg

    dsn, schema = pg_schema
    with psycopg.connect(dsn, autocommit=True) as c:
        yield c, schema


def test_record_appends_and_ignores_a_replayed_run(conn):
    c, schema = conn
    run = audit.new_run_id()
    entries = [entry(run), entry(run, "punctuality_monthly", ok=False, staged=0, inserted=0)]

    assert audit.record(c, entries, schema=schema) == 2
    assert audit.record(c, entries, schema=schema) == 0  # same run recorded twice
    (n,) = c.execute(f"select count(*) from {schema}._load_runs").fetchone()
    assert n == 2


def test_last_run_returns_only_the_latest_run(conn):
    c, schema = conn
    assert audit.record(c, [], schema=schema) == 0  # creates the empty log
    assert audit.last_run(c, schema=schema) == []
    old, new = audit.new_run_id(), audit.new_run_id()
    audit.record(c, [entry(old, start=T0 - timedelta(days=1))], schema=schema)
    audit.record(
        c,
        [entry(new, start=T0), entry(new, "route_weather_monthly", start=T0 + timedelta(1))],
        schema=schema,
    )

    latest = audit.last_run(c, schema=schema)
    assert [e.table_name for e in latest] == ["stations", "route_weather_monthly"]
    assert {e.run_id for e in latest} == {new}
    assert latest[0] == entry(new, start=T0)
