import datetime as dt

from pyspark.sql import functions as F

from medallion.populations import KEY, add_rejection_reasons, flatten_bindings, preference
from medallion.silver import dedup_latest, merge_sql, quarantine_merge_sql, split_valid_and_rejected

TERM = "struct<datatype: string, type: string, value: string>"
BRONZE_SCHEMA = f"""
    head struct<vars: array<string>>,
    results struct<bindings: array<struct<population: {TERM}, point_in_time: {TERM}, rank: struct<type: string, value: string>>>>,
    station_id string, _batch_id string, _ingested_at timestamp, _source_file string
"""
DECIMAL = "http://www.w3.org/2001/XMLSchema#decimal"
DATETIME = "http://www.w3.org/2001/XMLSchema#dateTime"
RANK = "http://wikiba.se/ontology#"
INGESTED = dt.datetime(2026, 9, 27, 12, 0)


def binding(population, point_in_time, rank="NormalRank"):
    return (
        None if population is None else (DECIMAL, "literal", population),
        None if point_in_time is None else (DATETIME, "literal", point_in_time),
        ("uri", RANK + rank),
    )


def statements(spark, *bindings, station="ushuaia", batch="run-1", ingested=INGESTED):
    row = ((["population", "point_in_time", "rank"],), (list(bindings),), station, batch, ingested, f"s3://raw/{station}.json")
    return add_rejection_reasons(flatten_bindings(spark.createDataFrame([row], BRONZE_SCHEMA)))


def test_each_statement_becomes_a_typed_row(spark):
    df = statements(spark, binding("56956", "2010-01-01T00:00:00Z"), binding("82615", "2022-01-01T00:00:00Z", "PreferredRank"))

    rows = df.orderBy("reference_date").select(F.date_format("reference_date", "yyyy-MM-dd").alias("day"), "population", "is_preferred_rank", "rejection_reasons").collect()
    assert [tuple(r) for r in rows] == [("2010-01-01", 56956, False, []), ("2022-01-01", 82615, True, [])]


def test_undated_invalid_and_deprecated_statements_are_rejected(spark):
    # Undated values exist on Wikidata; without a date a count can't be placed in time, so it can't feed an as-of join.
    df = statements(
        spark,
        binding("82615", None),
        binding("lots", "2022-01-01T00:00:00Z"),
        binding("-5", "2023-01-01T00:00:00Z"),
        binding("82615.5", "2024-01-01T00:00:00Z"),
        binding(None, "2025-01-01T00:00:00Z"),
        binding("99999", "2021-01-01T00:00:00Z", "DeprecatedRank"),
        binding("82615", "not a date"),
    )

    assert {(r.population_raw, r.point_in_time_raw): r.rejection_reasons for r in df.collect()} == {
        ("82615", None): ["missing_reference_date"],
        ("lots", "2022-01-01T00:00:00Z"): ["invalid_population"],
        ("-5", "2023-01-01T00:00:00Z"): ["non_positive_population"],
        ("82615.5", "2024-01-01T00:00:00Z"): ["fractional_population"],
        (None, "2025-01-01T00:00:00Z"): ["missing_population"],
        ("99999", "2021-01-01T00:00:00Z"): ["deprecated_rank"],
        ("82615", "not a date"): ["invalid_reference_date"],
    }


def test_the_preferred_statement_wins_when_two_share_a_date(spark):
    # e.g. a census count and an estimate for the same year: Wikidata marks the one to use as preferred.
    df = statements(spark, binding("80000", "2022-01-01T00:00:00Z"), binding("82615", "2022-01-01T00:00:00Z", "PreferredRank"))
    valid, _ = split_valid_and_rejected(df)

    assert [r.population for r in dedup_latest(valid, KEY, preference()).collect()] == [82615]


def test_merge_statements_update_only_changed_newer_rows_and_match_rejects_null_safely():
    assert merge_sql("t_silver", "v", KEY, ("population",)) == (
        "MERGE INTO t_silver t\nUSING v s\nON t.station_id = s.station_id AND t.reference_date = s.reference_date\n"
        "WHEN MATCHED AND s._ingested_at >= t._ingested_at AND (NOT (t.population <=> s.population)) THEN UPDATE SET *\n"
        "WHEN NOT MATCHED THEN INSERT *"
    )
    assert "ON t.station_id <=> s.station_id AND t._batch_id <=> s._batch_id\nWHEN NOT MATCHED THEN INSERT *" in quarantine_merge_sql("t_q", "v", ("station_id", "_batch_id"))
