"""Describe a warehouse table from a silver table's Spark schema.

The warehouse mirrors silver column for column: the Postgres DDL is derived from
the Parquet schema instead of being written by hand a second time, so a column
added in silver reaches `raw` without touching the loader. Only the business key
comes from the silver contract, because a Parquet file has no notion of one.

Everything here is pure Python (Spark types are given by their `simpleString`),
so it is tested without a JVM or a database.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

# Spark simpleString -> Postgres type. Anything not listed is refused rather than
# guessed: a silently widened column is harder to notice than a failed load.
SPARK_TO_PG: dict[str, str] = {
    "string": "text",
    "boolean": "boolean",
    "smallint": "smallint",
    "int": "integer",
    "bigint": "bigint",
    "float": "real",
    "double": "double precision",
    "date": "date",
    "timestamp": "timestamptz",
    "timestamp_ntz": "timestamp",
}

# format_type() spelling of the types `pg_type` emits under a short alias.
_CANONICAL = {
    "timestamptz": "timestamp with time zone",
    "timestamp": "timestamp without time zone",
}

_DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)")

# Bookkeeping columns the loader adds to every warehouse table.
LOADED_AT = "_loaded_at"
UPDATED_AT = "_updated_at"
AUDIT_COLUMNS: tuple[str, ...] = (LOADED_AT, UPDATED_AT)


def pg_type(spark_type: str) -> str:
    """Postgres type for a Spark `simpleString` type, or ValueError if unsupported."""
    if spark_type in SPARK_TO_PG:
        return SPARK_TO_PG[spark_type]
    match = _DECIMAL.fullmatch(spark_type)
    if match:
        return f"numeric({match[1]},{match[2]})"
    raise ValueError(f"no Postgres mapping for Spark type {spark_type!r}")


def canonical_pg_type(kind: str) -> str:
    """How Postgres' `format_type()` spells `kind`, for comparing with a live table."""
    return _CANONICAL.get(kind, kind)


@dataclass(frozen=True)
class Column:
    name: str
    pg_type: str


@dataclass(frozen=True)
class TargetTable:
    """A warehouse table: where it lives, its columns, key and load partitions."""

    schema: str
    name: str
    columns: tuple[Column, ...]
    key: tuple[str, ...]
    # Silver partition columns: the unit of incremental loading and of deletes.
    partition_by: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        names = [c.name for c in self.columns]
        if len(set(names)) != len(names):
            raise ValueError(f"{self.name}: duplicate column names")
        if not self.key:
            raise ValueError(f"{self.name}: a business key is required for upserts")
        for group, cols in (("key", self.key), ("partition", self.partition_by)):
            missing = [c for c in cols if c not in names]
            if missing:
                raise ValueError(f"{self.name}: {group} columns not in table: {missing}")
        clash = [c for c in names if c in AUDIT_COLUMNS]
        if clash:
            raise ValueError(f"{self.name}: reserved column names {clash}")

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    @property
    def value_columns(self) -> tuple[str, ...]:
        """Columns that are not part of the key (what an upsert may change)."""
        return tuple(c for c in self.column_names if c not in self.key)


def target_table(
    name: str,
    spark_schema: Iterable[tuple[str, str]] | Mapping[str, str],
    *,
    key: Iterable[str],
    partition_by: Iterable[str] = (),
    schema: str = "raw",
) -> TargetTable:
    """Build a TargetTable from `(column, spark simpleString)` pairs, in schema order."""
    pairs = spark_schema.items() if isinstance(spark_schema, Mapping) else spark_schema
    columns = tuple(Column(col, pg_type(kind)) for col, kind in pairs)
    return TargetTable(schema, name, columns, tuple(key), tuple(partition_by))
