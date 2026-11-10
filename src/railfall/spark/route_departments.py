"""Map TGV routes to the departments of their two endpoints.

The punctuality export names stations the AQST way (`PARIS LYON`,
`MARSEILLE ST CHARLES`, `MACON LOCHE`), the stations export the SNCF way
(`Paris Gare de Lyon`, `Marseille Saint-Charles`, `Mâcon Loché TGV`). Both are
folded to a common `match_key` (accent-free, `ST`/`STE` spelled out, `Gare de`
dropped), then each AQST station is resolved in this order:

1. **alias**: a short curated table for names no rule can bridge
   (`AEROPORT CDG2 TGV ROISSY`, `VALENCE ALIXAN TGV`, ...);
2. **foreign**: stations outside France (`GENEVE`, `MADRID`, `ITALIE`, ...) have
   no department, on purpose: Météo-France does not cover them;
3. **exact**: same match key, or same key once a trailing `VILLE` is removed
   (`NICE VILLE` -> `Nice`);
4. **prefix**: the AQST key is the start of station keys (`LILLE` ->
   `Lille Flandres`, `Lille Europe`). Accepted only when every candidate lies in
   the same department, so a prefix never guesses.

Anything else is `unmatched`, kept with a null department so coverage can be
measured instead of the station quietly disappearing from the join.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from railfall.spark.stations import name_key

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

# AQST name (as normalised by punctuality.normalise_station) -> department.
ALIASES: dict[str, str] = {
    "AEROPORT CDG2 TGV ROISSY": "95",
    "VALENCE ALIXAN TGV": "26",
    "BELLEGARDE (AIN)": "01",
    "MARNE LA VALLEE": "77",
    "LE CREUSOT MONTCEAU MONTCHANIN": "71",
    "TOURCOING": "59",
}

# AQST names of stations outside France (and the catch-all `ITALIE`).
FOREIGN: frozenset[str] = frozenset(
    {
        "BARCELONA",
        "BRUXELLES MIDI",
        "FRANCFORT",
        "GENEVE",
        "ITALIE",
        "LAUSANNE",
        "MADRID",
        "STUTTGART",
        "ZURICH",
    }
)

METHODS = ("alias", "foreign", "exact", "prefix", "unmatched")


def match_key(col: Column) -> Column:
    """Fold a station name for matching: `Paris Gare de Lyon` and `PARIS LYON` -> `PARIS LYON`."""
    from pyspark.sql import functions as F

    key = name_key(col)
    key = F.regexp_replace(key, r"\bSTE\b", "SAINTE")
    key = F.regexp_replace(key, r"\bST\b", "SAINT")
    key = F.regexp_replace(key, r"\bGARE( DE LA| DE| DU| DES| D)?\b", "")
    key = F.trim(F.regexp_replace(key, r"\s+", " "))
    return F.when(key == "", None).otherwise(key)


def _department_by_key(stations: DataFrame) -> DataFrame:
    """One row per station match key with its department, or null when keys clash."""
    from pyspark.sql import functions as F

    return (
        stations.select(match_key(F.col("station_name")).alias("skey"), "department")
        .where(F.col("skey").isNotNull())
        .groupBy("skey")
        .agg(F.collect_set("department").alias("depts"))
    )


def station_departments(endpoints: DataFrame, stations: DataFrame) -> DataFrame:
    """Resolve AQST station names to departments.

    `endpoints` has a `station` column (normalised AQST names, any multiplicity);
    `stations` is the silver stations table. Returns one row per distinct station:
    `station`, `department` (null for foreign / unmatched), `match_method`.
    """
    from pyspark.sql import functions as F

    spark = endpoints.sparkSession
    names = (
        endpoints.select("station")
        .where(F.col("station").isNotNull())
        .distinct()
        .withColumn("key", match_key(F.col("station")))
        .withColumn("key_noville", F.regexp_replace("key", r" VILLE$", ""))
    )
    by_key = _department_by_key(stations)
    unique = by_key.where(F.size("depts") == 1).select(
        "skey", F.element_at("depts", 1).alias("department")
    )

    exact = (
        names.join(unique, (unique.skey == names.key) | (unique.skey == names.key_noville))
        .groupBy("station")
        .agg(F.collect_set("department").alias("exact_depts"))
    )
    prefix = (
        names.join(by_key, by_key.skey.startswith(F.concat(names.key, F.lit(" "))))
        .select("station", F.explode("depts").alias("department"))
        .groupBy("station")
        .agg(F.collect_set("department").alias("prefix_depts"))
    )

    alias_rows = [(k, v) for k, v in sorted(ALIASES.items())]
    aliases = spark.createDataFrame(alias_rows, "station string, alias_department string")

    joined = (
        names.join(aliases, "station", "left")
        .join(exact, "station", "left")
        .join(prefix, "station", "left")
    )
    is_foreign = F.col("station").isin(*sorted(FOREIGN))
    exact_ok = F.size(F.coalesce("exact_depts", F.array())) == 1
    prefix_ok = F.size(F.coalesce("prefix_depts", F.array())) == 1

    method = (
        F.when(F.col("alias_department").isNotNull(), "alias")
        .when(is_foreign, "foreign")
        .when(exact_ok, "exact")
        .when(prefix_ok, "prefix")
        .otherwise("unmatched")
    )
    department = (
        F.when(F.col("alias_department").isNotNull(), F.col("alias_department"))
        .when(is_foreign, F.lit(None).cast("string"))
        .when(exact_ok, F.element_at("exact_depts", 1))
        .when(prefix_ok, F.element_at("prefix_depts", 1))
    )
    return joined.select("station", department.alias("department"), method.alias("match_method"))


def route_departments(punctuality: DataFrame, stations: DataFrame) -> DataFrame:
    """One row per route with the department (and match method) of each endpoint.

    `is_domestic` is true when both endpoints resolved to a French department,
    the routes the weather join can actually use.
    """
    from pyspark.sql import functions as F

    routes = punctuality.select("route_id", "departure_station", "arrival_station").distinct()
    endpoints = routes.select(F.col("departure_station").alias("station")).unionByName(
        routes.select(F.col("arrival_station").alias("station"))
    )
    resolved = station_departments(endpoints, stations)
    dep = resolved.select(
        F.col("station").alias("departure_station"),
        F.col("department").alias("departure_department"),
        F.col("match_method").alias("departure_match"),
    )
    arr = resolved.select(
        F.col("station").alias("arrival_station"),
        F.col("department").alias("arrival_department"),
        F.col("match_method").alias("arrival_match"),
    )
    out = routes.join(dep, "departure_station", "left").join(arr, "arrival_station", "left")
    domestic = F.col("departure_department").isNotNull() & F.col("arrival_department").isNotNull()
    return out.select(
        "route_id",
        "departure_station",
        "arrival_station",
        "departure_department",
        "arrival_department",
        "departure_match",
        "arrival_match",
        domestic.alias("is_domestic"),
    )
