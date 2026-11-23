"""Load job: scope rules (pure), and silver -> Postgres on a local lake + real Postgres."""

from __future__ import annotations

from datetime import date
from uuid import UUID

import pytest

from railfall.load import job
from railfall.spark import write as w
from railfall.spark.verify import CONTRACTS

RUN = date(2026, 11, 11)


@pytest.mark.parametrize("table", list(w.TABLES.values()), ids=lambda t: t.name)
def test_every_silver_table_maps_to_a_warehouse_table(table):
    contract = CONTRACTS[table.name]
    target = job.target_for(table, contract.columns.items())
    assert (target.schema, target.name) == ("raw", table.name)
    assert target.key == contract.key
    assert target.partition_by == table.partition_by
    assert target.column_names == tuple(contract.columns)


def test_only_year_partitioned_tables_are_incremental():
    assert job.is_incremental(w.PUNCTUALITY, [2025])
    assert job.is_incremental(w.WEATHER, [2025])
    assert not job.is_incremental(w.STATIONS, [2025])
    assert not job.is_incremental(w.ROUTE_DEPARTMENTS, [2025])
    assert not job.is_incremental(w.PUNCTUALITY, None)
    assert not job.is_incremental(w.PUNCTUALITY, [])


def test_scope_label():
    assert job.scope_label(w.PUNCTUALITY, [2025, 2024, 2025]) == "year=2024,2025"
    assert job.scope_label(w.STATIONS, [2025]) == "full"
    assert job.scope_label(w.WEATHER, None) == "full"


def test_run_rejects_unknown_tables():
    with pytest.raises(ValueError, match="unknown silver tables"):
        job.run(None, None, root="/nowhere", names=["nope"])


# --- Spark + Postgres --------------------------------------------------------------

PUNCT_DDL = (
    "month date, year int, service string, departure_station string, "
    "arrival_station string, trains_run bigint, arrival_punctuality_rate double"
)


def _punctuality(spark, rows):
    return spark.createDataFrame(rows, PUNCT_DDL)


def _write(spark, root, table, df):
    w.write_silver(df, None, root=root, table=table, run_date=RUN)


def _rows(conn, schema, table, order="month, departure_station"):
    return conn.execute(f"select * from {schema}.{table} order by {order}").fetchall()


PUNCT = [
    (date(2024, 3, 1), 2024, "National", "PARIS LYON", "MARSEILLE ST CHARLES", 400, 91.5),
    (date(2024, 3, 1), 2024, "National", "PARIS LYON", "LYON PART DIEU", 600, 93.0),
    (date(2025, 3, 1), 2025, "National", "PARIS LYON", "LYON PART DIEU", 610, 88.0),
]


@pytest.fixture
def warehouse(pg_schema):
    import psycopg

    dsn, schema = pg_schema
    with psycopg.connect(dsn, autocommit=True) as conn:
        yield conn, schema


@pytest.mark.spark
def test_load_twice_is_idempotent(spark, tmp_path, warehouse):
    conn, schema = warehouse
    root = str(tmp_path)
    _write(spark, root, w.PUNCTUALITY, _punctuality(spark, PUNCT))

    first = job.run(spark, conn, root=root, names=["punctuality_monthly"], schema=schema)
    second = job.run(spark, conn, root=root, names=["punctuality_monthly"], schema=schema)

    assert [r.ok for r in first + second] == [True, True], first + second
    assert (first[0].staged, first[0].inserted, first[0].updated) == (3, 3, 0)
    assert (second[0].inserted, second[0].updated, second[0].deleted) == (0, 0, 0)
    assert second[0].unchanged == 3
    (dupes,) = conn.execute(
        f"select count(*) - count(distinct (month, service, departure_station, arrival_station))"
        f" from {schema}.punctuality_monthly"
    ).fetchone()
    assert dupes == 0


@pytest.mark.spark
def test_year_load_only_replaces_that_year(spark, tmp_path, warehouse):
    conn, schema = warehouse
    root = str(tmp_path)
    _write(spark, root, w.PUNCTUALITY, _punctuality(spark, PUNCT))
    job.run(spark, conn, root=root, names=["punctuality_monthly"], schema=schema)

    # Silver now drops a 2024 route and revises 2025; load only 2025.
    revised = [PUNCT[1], (*PUNCT[2][:6], 79.5)]
    _write(spark, root, w.PUNCTUALITY, _punctuality(spark, revised))
    (result,) = job.run(
        spark, conn, root=root, names=["punctuality_monthly"], years=[2025], schema=schema
    )

    assert result.ok and result.scope == "year=2025", result
    assert (result.staged, result.updated, result.deleted) == (1, 1, 0)
    rows = _rows(conn, schema, "punctuality_monthly")
    assert len(rows) == 3  # the dropped 2024 route stays until 2024 is reloaded
    (rate,) = conn.execute(
        f"select arrival_punctuality_rate from {schema}.punctuality_monthly where year = 2025"
    ).fetchone()
    assert rate == 79.5

    (full,) = job.run(spark, conn, root=root, names=["punctuality_monthly"], schema=schema)
    assert full.deleted == 1
    assert len(_rows(conn, schema, "punctuality_monthly")) == 2


@pytest.mark.spark
def test_missing_silver_table_fails_alone(spark, tmp_path, warehouse):
    conn, schema = warehouse
    root = str(tmp_path)
    _write(spark, root, w.PUNCTUALITY, _punctuality(spark, PUNCT))

    results = job.run(
        spark, conn, root=root, names=["stations", "punctuality_monthly"], schema=schema
    )

    by_table = {r.table: r for r in results}
    assert [r.table for r in results] == ["punctuality_monthly", "stations"]
    assert by_table["punctuality_monthly"].ok
    assert not by_table["stations"].ok and by_table["stations"].error


@pytest.mark.spark
def test_every_run_is_logged_including_failed_tables(spark, tmp_path, warehouse):
    from railfall.load import audit

    conn, schema = warehouse
    root = str(tmp_path)
    _write(spark, root, w.PUNCTUALITY, _punctuality(spark, PUNCT))

    job.run(spark, conn, root=root, names=["punctuality_monthly"], schema=schema)
    second = job.run(
        spark, conn, root=root, names=["stations", "punctuality_monthly"], schema=schema
    )

    logged = audit.last_run(conn, schema=schema)
    assert {e.run_id for e in logged} == {UUID(second[0].run_id)}
    by_table = {e.table_name: e for e in logged}
    assert by_table["punctuality_monthly"].ok and by_table["punctuality_monthly"].unchanged == 3
    assert not by_table["stations"].ok and by_table["stations"].error
    assert all(e.finished_at >= e.started_at for e in logged)
    (runs,) = conn.execute(f"select count(distinct run_id) from {schema}._load_runs").fetchone()
    assert runs == 2


# --- Lost connections (no Spark, no Postgres) ----------------------------------------


class FakeConn:
    def __init__(self, name: str) -> None:
        self.name = name
        self.broken = False
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _fake_load(dies_on: str):
    """A `load_table` that breaks the connection while it loads `dies_on`."""
    used = []

    def load_table(spark, conn, table, **kwargs):
        used.append((table.name, conn.name))
        if table.name == dies_on:
            conn.broken = True
            return job.LoadResult(table.name, ok=False, error="OperationalError: terminated")
        return job.LoadResult(table.name, ok=True)

    return load_table, used


def test_a_lost_connection_only_fails_the_table_in_flight(monkeypatch):
    load_table, used = _fake_load(dies_on="weather_daily")
    logged = {}
    monkeypatch.setattr(job, "load_table", load_table)
    monkeypatch.setattr(
        job.audit, "record", lambda conn, entries, schema: logged.update(conn=conn.name)
    )
    fresh = []

    def reconnect():
        fresh.append(FakeConn(f"conn-{len(fresh) + 2}"))
        return fresh[-1]

    first = FakeConn("conn-1")
    results = job.run(None, first, root="/lake", reconnect=reconnect)

    names = list(w.TABLES)
    broken_at = names.index("weather_daily")
    assert [r.table for r in results if not r.ok] == ["weather_daily"]
    # Every table up to the broken one used the caller's connection, the rest a new one.
    assert [c for _, c in used] == ["conn-1"] * (broken_at + 1) + ["conn-2"] * (
        len(names) - broken_at - 1
    )
    assert logged == {"conn": "conn-2"}, "the failed table must still be audited"
    assert len(fresh) == 1 and fresh[0].closed, "the run closes what it opened"
    assert not first.closed, "the caller's connection is the caller's to close"


def test_without_reconnect_the_connection_is_kept(monkeypatch):
    load_table, used = _fake_load(dies_on="weather_daily")
    monkeypatch.setattr(job, "load_table", load_table)
    monkeypatch.setattr(job.audit, "record", lambda conn, entries, schema: None)

    job.run(None, FakeConn("only"), root="/lake")

    assert {c for _, c in used} == {"only"}


def test_a_failed_reconnect_propagates(monkeypatch):
    load_table, _ = _fake_load(dies_on=next(iter(w.TABLES)))
    monkeypatch.setattr(job, "load_table", load_table)

    def reconnect():
        raise ConnectionError("warehouse still down")

    with pytest.raises(ConnectionError, match="still down"):
        job.run(None, FakeConn("c"), root="/lake", reconnect=reconnect)


def test_usable():
    conn = FakeConn("c")
    assert job.usable(conn)
    conn.broken = True
    assert not job.usable(conn)
    assert job.usable(object())  # nothing to go on: assume it works
