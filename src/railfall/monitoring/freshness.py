"""Freshness checks: is every warehouse table recently loaded and up to date?

Two different questions, both answered per `raw.*` table:

* **Load freshness** — when did a load last *succeed* for this table? This comes
  from the audit log (`raw._load_runs`), not from the rows' `_loaded_at` /
  `_updated_at`: the load is an idempotent merge, so a healthy re-run that
  changed nothing leaves every row timestamp untouched. Only the log tells a
  quiet success apart from a pipeline that stopped running.
* **Data freshness** — how old is the newest business date in the table
  (`month`, `day`)? A load can succeed every night on a source that stopped
  publishing; this catches that. Lags are counted from the *start* of the
  period, so a monthly table whose newest row is `2026-08-01` is 92 days behind
  on 2026-11-01. The defaults follow each source's publication delay: SNCF
  posts a month's punctuality one to two months after it ends, Météo-France
  refreshes its daily files within days.

Reference tables (stations, route ↔ department mapping) have no business date;
only their load freshness and emptiness are checked.

The queries are plain SQL over a DB-API connection, so the same code runs on
Postgres in production and on SQLite in unit tests (`schema=None`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol

from railfall.load.audit import LOG_TABLE

RAW_SCHEMA = "raw"

OK = "ok"
STALE = "stale"
EMPTY = "empty"
MISSING = "missing"

# The pipeline runs daily at 05:15 UTC: 36 h tolerates one late or retried run
# without letting a skipped day go unnoticed.
DEFAULT_LOAD_SLA = timedelta(hours=36)


class Connection(Protocol):
    def execute(self, query: str, params: Sequence[Any] = ...) -> Any: ...


@dataclass(frozen=True)
class FreshnessRule:
    """What "fresh" means for one table."""

    table: str
    # Business date column (first day of the period for monthly tables).
    data_column: str | None = None
    # Oldest acceptable newest-period start, in days; None = no data check.
    max_data_lag_days: int | None = None
    max_load_age: timedelta = DEFAULT_LOAD_SLA

    def __post_init__(self) -> None:
        if (self.data_column is None) != (self.max_data_lag_days is None):
            raise ValueError(f"{self.table}: data_column and max_data_lag_days go together")
        if self.max_data_lag_days is not None and self.max_data_lag_days < 0:
            raise ValueError(f"{self.table}: max_data_lag_days must be >= 0")
        if self.max_load_age <= timedelta(0):
            raise ValueError(f"{self.table}: max_load_age must be positive")


RULES: dict[str, FreshnessRule] = {
    r.table: r
    for r in (
        # SNCF: month M appears 1-2 months after it ends -> up to ~4 months from its start.
        FreshnessRule("punctuality_monthly", "month", 125),
        FreshnessRule("route_weather_monthly", "month", 125),
        # Météo-France daily files are refreshed within days; the monthly rollup
        # always has the current or previous month.
        FreshnessRule("weather_daily", "day", 45),
        FreshnessRule("weather_monthly", "month", 70),
        FreshnessRule("stations"),
        FreshnessRule("route_departments"),
    )
}


@dataclass(frozen=True)
class Freshness:
    """Observed state of one table, and the verdict against its rule."""

    table: str
    status: str
    rows: int | None = None
    last_loaded_at: datetime | None = None
    load_age_hours: float | None = None
    latest_period: date | None = None
    data_lag_days: int | None = None
    reasons: tuple[str, ...] = field(default=())

    @property
    def ok(self) -> bool:
        return self.status == OK

    def as_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "status": self.status,
            "rows": self.rows,
            "last_loaded_at": self.last_loaded_at.isoformat() if self.last_loaded_at else None,
            "load_age_hours": self.load_age_hours,
            "latest_period": self.latest_period.isoformat() if self.latest_period else None,
            "data_lag_days": self.data_lag_days,
            "reasons": list(self.reasons),
        }


# --------------------------------------------------------------------------- SQL


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def qualified(table: str, schema: str | None) -> str:
    return _quote(table) if schema is None else f"{_quote(schema)}.{_quote(table)}"


def table_sql(rule: FreshnessRule, schema: str | None) -> str:
    """Row count and newest business date of the table, in one scan."""
    latest = f"max({_quote(rule.data_column)})" if rule.data_column else "null"
    return f"select count(*), {latest} from {qualified(rule.table, schema)}"


def last_load_sql(schema: str | None, placeholder: str = "%s") -> str:
    """Last successful load per table, from the audit log."""
    return (
        f"select max(finished_at) from {qualified(LOG_TABLE, schema)}"
        f" where table_name = {placeholder} and ok"
    )


# --------------------------------------------------------------------------- parsing


def as_datetime(value: Any) -> datetime | None:
    """A timezone-aware datetime from what the driver returned (SQLite gives text)."""
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime):
        raise TypeError(f"expected a timestamp, got {value!r}")
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = date.fromisoformat(value[:10])
    if isinstance(value, datetime):
        return value.date()
    if not isinstance(value, date):
        raise TypeError(f"expected a date, got {value!r}")
    return value


# --------------------------------------------------------------------------- verdict


def evaluate(
    rule: FreshnessRule,
    *,
    rows: int,
    last_loaded_at: datetime | None,
    latest_period: date | None,
    now: datetime,
) -> Freshness:
    """Judge observed values against `rule` (pure: no database)."""
    reasons: list[str] = []
    load_age_h = None
    if last_loaded_at is None:
        reasons.append("no successful load recorded")
    else:
        age = now - last_loaded_at
        load_age_h = round(age.total_seconds() / 3600, 2)
        if age > rule.max_load_age:
            limit_h = rule.max_load_age.total_seconds() / 3600
            reasons.append(f"last successful load {load_age_h:g} h ago (limit {limit_h:g} h)")

    lag = None
    if rule.data_column is not None and latest_period is not None:
        lag = (now.date() - latest_period).days
        if lag > rule.max_data_lag_days:
            reasons.append(
                f"newest {rule.data_column} {latest_period.isoformat()} is {lag} days old"
                f" (limit {rule.max_data_lag_days})"
            )

    if rows == 0:
        status = EMPTY
        reasons.insert(0, "table is empty")
    else:
        status = STALE if reasons else OK
    return Freshness(
        table=rule.table,
        status=status,
        rows=rows,
        last_loaded_at=last_loaded_at,
        load_age_hours=load_age_h,
        latest_period=latest_period,
        data_lag_days=lag,
        reasons=tuple(reasons),
    )


# --------------------------------------------------------------------------- queries


def _rollback(conn: Any) -> None:
    # On Postgres a failed statement aborts the transaction; later checks need a clean one.
    rollback = getattr(conn, "rollback", None)
    if rollback is not None:
        rollback()


def check_table(
    conn: Connection,
    rule: FreshnessRule,
    *,
    now: datetime,
    schema: str | None = RAW_SCHEMA,
    placeholder: str = "%s",
) -> Freshness:
    try:
        rows, latest = conn.execute(table_sql(rule, schema)).fetchone()
    except Exception as exc:  # undefined table, permission denied, ...
        _rollback(conn)
        return Freshness(rule.table, MISSING, reasons=(f"cannot read table: {exc}".strip(),))
    try:
        (loaded,) = conn.execute(last_load_sql(schema, placeholder), (rule.table,)).fetchone()
    except Exception:  # no audit log yet: nothing was ever loaded through the loader
        _rollback(conn)
        loaded = None
    return evaluate(
        rule,
        rows=int(rows),
        last_loaded_at=as_datetime(loaded),
        latest_period=as_date(latest),
        now=now,
    )


def check(
    conn: Connection,
    tables: Iterable[str] | None = None,
    *,
    rules: Mapping[str, FreshnessRule] = RULES,
    now: datetime | None = None,
    schema: str | None = RAW_SCHEMA,
    placeholder: str = "%s",
) -> list[Freshness]:
    """Freshness of `tables` (default: every table with a rule), in rule order."""
    names = list(rules) if tables is None else list(tables)
    unknown = sorted(set(names) - set(rules))
    if unknown:
        raise ValueError(f"no freshness rule for: {', '.join(unknown)}")
    now = now or datetime.now(UTC)
    return [
        check_table(conn, rules[n], now=now, schema=schema, placeholder=placeholder) for n in names
    ]
