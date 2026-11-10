"""Unit tests for the route -> department mapping (local Spark)."""

from __future__ import annotations

import pytest

from railfall.spark import route_departments as rd

pytestmark = pytest.mark.spark

STATIONS_SCHEMA = "station_name string, department string"

STATIONS = [
    ("Paris Gare de Lyon", "75"),
    ("Paris Montparnasse", "75"),
    ("Paris Gare d'Austerlitz", "75"),
    ("Marseille Saint-Charles", "13"),
    ("Bordeaux Saint-Jean", "33"),
    ("Lyon Part-Dieu", "69"),
    ("Lille Flandres", "59"),
    ("Lille Europe", "59"),
    ("Nice Ville", "06"),
    ("Mâcon Loché TGV", "71"),
    ("Saint-Étienne Châteaucreux", "42"),
    # Two "Valence" stations in different departments: a prefix must not guess.
    ("Valence Ville", "26"),
    ("Valence d'Agen", "82"),
    ("Dijon", "21"),
]


@pytest.fixture
def stations(spark):
    return spark.createDataFrame(STATIONS, STATIONS_SCHEMA)


def _resolve(spark, stations, names):
    endpoints = spark.createDataFrame([(n,) for n in names], "station string")
    rows = rd.station_departments(endpoints, stations).collect()
    return {r.station: (r.department, r.match_method) for r in rows}


def test_match_key_folds_both_spellings(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        [
            ("Paris Gare de Lyon",),
            ("PARIS LYON",),
            ("ST ETIENNE CHATEAUCREUX",),
            ("Saint-Étienne Châteaucreux",),
            ("Paris Gare d'Austerlitz",),
            ("Gare",),
        ],
        "name string",
    )
    keys = [r.k for r in df.select(rd.match_key(F.col("name")).alias("k")).collect()]
    assert keys == [
        "PARIS LYON",
        "PARIS LYON",
        "SAINT ETIENNE CHATEAUCREUX",
        "SAINT ETIENNE CHATEAUCREUX",
        "PARIS AUSTERLITZ",
        None,
    ]


def test_exact_matches_after_folding(spark, stations):
    got = _resolve(
        spark,
        stations,
        ["PARIS LYON", "MARSEILLE ST CHARLES", "ST ETIENNE CHATEAUCREUX", "PARIS AUSTERLITZ"],
    )
    assert got["PARIS LYON"] == ("75", "exact")
    assert got["MARSEILLE ST CHARLES"] == ("13", "exact")
    assert got["ST ETIENNE CHATEAUCREUX"] == ("42", "exact")
    assert got["PARIS AUSTERLITZ"] == ("75", "exact")


def test_trailing_ville_is_optional(spark, stations):
    assert _resolve(spark, stations, ["DIJON VILLE"]) == {"DIJON VILLE": ("21", "exact")}
    assert _resolve(spark, stations, ["NICE VILLE"]) == {"NICE VILLE": ("06", "exact")}


def test_prefix_accepted_only_within_one_department(spark, stations):
    got = _resolve(spark, stations, ["LILLE", "MACON LOCHE", "VALENCE"])
    assert got["LILLE"] == ("59", "prefix")
    assert got["MACON LOCHE"] == ("71", "prefix")
    assert got["VALENCE"] == (None, "unmatched")


def test_alias_and_foreign(spark, stations):
    got = _resolve(spark, stations, ["VALENCE ALIXAN TGV", "GENEVE", "ITALIE"])
    assert got["VALENCE ALIXAN TGV"] == ("26", "alias")
    assert got["GENEVE"] == (None, "foreign")
    assert got["ITALIE"] == (None, "foreign")


def test_unknown_station_is_kept_as_unmatched(spark, stations):
    assert _resolve(spark, stations, ["NOWHERE CENTRAL"]) == {
        "NOWHERE CENTRAL": (None, "unmatched")
    }


def test_duplicate_endpoints_give_one_row(spark, stations):
    endpoints = spark.createDataFrame([("PARIS LYON",), ("PARIS LYON",), (None,)], "station string")
    out = rd.station_departments(endpoints, stations)
    assert out.count() == 1


def test_route_departments_flags_domestic_routes(spark, stations):
    punctuality = spark.createDataFrame(
        [
            ("r1", "PARIS LYON", "MARSEILLE ST CHARLES"),
            ("r1", "PARIS LYON", "MARSEILLE ST CHARLES"),  # another month, same route
            ("r2", "PARIS LYON", "GENEVE"),
            ("r3", "MARSEILLE ST CHARLES", "PARIS LYON"),
        ],
        "route_id string, departure_station string, arrival_station string",
    )
    rows = {r.route_id: r for r in rd.route_departments(punctuality, stations).collect()}
    assert sorted(rows) == ["r1", "r2", "r3"]
    assert (rows["r1"].departure_department, rows["r1"].arrival_department) == ("75", "13")
    assert rows["r1"].is_domestic is True
    assert (rows["r3"].departure_department, rows["r3"].arrival_department) == ("13", "75")
    assert rows["r2"].arrival_department is None
    assert rows["r2"].arrival_match == "foreign"
    assert rows["r2"].is_domestic is False


def test_aliases_and_foreign_do_not_overlap():
    assert not set(rd.ALIASES) & rd.FOREIGN
