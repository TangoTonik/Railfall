"""The raw expectation suites stay in sync with the silver contracts and with GX."""

from __future__ import annotations

import pytest

from railfall.quality.suites import SUITES, Expectation, suite_name, unique_key
from railfall.spark.verify import CONTRACTS


def test_every_suite_targets_a_loaded_table():
    assert set(SUITES) <= set(CONTRACTS)


@pytest.mark.parametrize("table", sorted(SUITES))
def test_suite_columns_exist_in_the_silver_contract(table):
    columns = set(CONTRACTS[table].columns)
    used = {c for spec in SUITES[table] for c in spec.columns}
    assert used <= columns, sorted(used - columns)


@pytest.mark.parametrize("table", sorted(SUITES))
def test_suite_checks_non_empty_and_the_business_key(table):
    kinds = [spec.kind for spec in SUITES[table]]
    assert "ExpectTableRowCountToBeBetween" in kinds
    key = CONTRACTS[table].key
    assert any(spec == unique_key(*key) for spec in SUITES[table])


def test_expectation_lists_every_column_it_reads():
    spec = Expectation("X", {"column_A": "a", "column_B": "b", "column_list": ["c", "d"]})
    assert spec.columns == ("a", "b", "c", "d")


def test_suite_name_is_schema_qualified():
    assert suite_name("stations") == "raw.stations"


def test_every_kind_is_a_gx_expectation():
    gx = pytest.importorskip("great_expectations")
    for specs in SUITES.values():
        for spec in specs:
            cls = getattr(gx.expectations, spec.kind)
            cls(**dict(spec.kwargs))  # validates argument names and types
