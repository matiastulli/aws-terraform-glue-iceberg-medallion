import pytest

from medallion.contract import raise_if_schema_mismatch, schema_mismatches

TABLE = [("station_id", "string"), ("temperature_c", "double"), ("_ingested_at", "timestamp")]


def test_an_exact_match_passes():
    assert schema_mismatches(list(reversed(TABLE)), TABLE) == []


def test_missing_extra_and_retyped_columns_are_all_reported():
    written = [("station_id", "string"), ("temperature_c", "float"), ("humidity_pct", "int")]

    assert schema_mismatches(written, TABLE) == [
        "missing column _ingested_at (timestamp)",
        "extra column humidity_pct (int)",
        "column temperature_c is float but the table has double",
    ]
    with pytest.raises(ValueError, match="doesn't match the DDL of t: missing column _ingested_at"):
        raise_if_schema_mismatch(written, TABLE, "t")
