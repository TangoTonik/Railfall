"""Unit tests for the punctuality bronze -> silver transform (local Spark)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from railfall.spark import punctuality as p

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parent / "fixtures" / "sncf" / "punctuality_sample.json"


def _records() -> list[dict]:
    return json.loads(FIXTURE.read_text())


def _frame(spark, tmp_path, records: list[dict]):
    path = tmp_path / "punctuality.json"
    path.write_text(json.dumps(records))
    return p.read_bronze(spark, str(path))


def test_fixture_becomes_typed_silver(spark, tmp_path) -> None:
    split = p.to_silver(_frame(spark, tmp_path, _records()))
    rows = split.valid.orderBy("month", "departure_station").collect()

    assert split.rejected.count() == 0
    assert len(rows) == 4
    first = rows[0]
    assert first.month == date(2024, 1, 1)
    assert first.year == 2024
    assert first.departure_station == "BORDEAUX ST JEAN"
    assert first.trains_run == 812 - 9
    assert first.arrival_punctuality_rate == round(1 - 87 / 803, 4)
    assert len(first.route_id) == 16
    assert dict(split.valid.dtypes)["trains_planned"] == "bigint"
    assert "month_raw" not in split.valid.columns


def test_station_names_are_normalised_into_the_same_route(spark, tmp_path) -> None:
    a, b = _records()[0], dict(_records()[0])
    b.update(date="2024-02", gare_depart="  paris   lyon ", gare_arrivee="Marseille St Charles")
    valid = p.to_silver(_frame(spark, tmp_path, [a, b])).valid

    assert valid.select("departure_station").distinct().collect()[0][0] == "PARIS LYON"
    assert valid.select("route_id").distinct().count() == 1


def test_duplicate_route_months_are_collapsed(spark, tmp_path) -> None:
    base = _records()[0]
    fuller = {**base, "nb_train_prevu": base["nb_train_prevu"] + 10}
    valid = p.to_silver(_frame(spark, tmp_path, [base, base, fuller])).valid

    assert valid.count() == 1
    assert valid.first().trains_planned == base["nb_train_prevu"] + 10


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"date": "2024-13"}, "bad_month"),
        ({"gare_arrivee": "   "}, "missing_station"),
        ({"nb_train_prevu": 0}, "no_planned_trains"),
        ({"nb_annulation": 900}, "cancelled_over_planned"),
        ({"nb_train_retard_arrivee": 700}, "late_over_run"),
    ],
)
def test_impossible_rows_are_quarantined_with_a_reason(spark, tmp_path, change, reason) -> None:
    good, bad = _records()[1], {**_records()[0], **change}
    split = p.to_silver(_frame(spark, tmp_path, [good, bad]))

    assert split.valid.count() == 1
    assert [r.reject_reason for r in split.rejected.collect()] == [reason]


def test_route_id_is_directed(spark) -> None:
    from pyspark.sql import functions as F

    row = (
        spark.range(1)
        .select(
            p.route_id(F.lit("PARIS LYON"), F.lit("GENEVE")).alias("out"),
            p.route_id(F.lit("GENEVE"), F.lit("PARIS LYON")).alias("back"),
        )
        .first()
    )
    assert row.out != row.back
