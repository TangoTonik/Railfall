import pytest

from railfall.load.schema import Column, TargetTable, pg_type, target_table


@pytest.mark.parametrize(
    ("spark", "pg"),
    [
        ("string", "text"),
        ("int", "integer"),
        ("bigint", "bigint"),
        ("double", "double precision"),
        ("boolean", "boolean"),
        ("date", "date"),
        ("timestamp", "timestamptz"),
        ("decimal(10,2)", "numeric(10,2)"),
    ],
)
def test_pg_type_maps_silver_types(spark, pg):
    assert pg_type(spark) == pg


@pytest.mark.parametrize("spark", ["array<string>", "map<string,int>", "binary", "decimal"])
def test_pg_type_refuses_unknown_types(spark):
    with pytest.raises(ValueError, match="no Postgres mapping"):
        pg_type(spark)


def test_target_table_keeps_schema_order_and_splits_key():
    table = target_table(
        "punctuality_monthly",
        [("month", "date"), ("route_id", "string"), ("year", "int"), ("rate", "double")],
        key=("month", "route_id"),
        partition_by=("year",),
    )
    assert table.schema == "raw"
    assert table.column_names == ("month", "route_id", "year", "rate")
    assert table.value_columns == ("year", "rate")
    assert table.columns[3] == Column("rate", "double precision")


def test_target_table_accepts_a_mapping():
    table = target_table("stations", {"uic_code": "string", "name": "string"}, key=["uic_code"])
    assert table.key == ("uic_code",)
    assert table.partition_by == ()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"key": ()}, "business key is required"),
        ({"key": ("nope",)}, "key columns not in table"),
        ({"key": ("a",), "partition_by": ("nope",)}, "partition columns not in table"),
    ],
)
def test_target_table_validates_key_and_partitions(kwargs, message):
    with pytest.raises(ValueError, match=message):
        target_table("t", [("a", "string"), ("b", "int")], **kwargs)


def test_target_table_rejects_audit_and_duplicate_columns():
    with pytest.raises(ValueError, match="reserved"):
        TargetTable("raw", "t", (Column("a", "text"), Column("_loaded_at", "text")), ("a",))
    with pytest.raises(ValueError, match="duplicate"):
        TargetTable("raw", "t", (Column("a", "text"), Column("a", "text")), ("a",))
