import datetime as dt

import pytest
from pyspark.sql import functions as F

from medallion.silver import (
    add_observed_at,
    add_rejection_reasons,
    change_counts,
    classify_changes,
    dedup_latest,
    flatten_hourly,
    reconcile,
    rows_to_merge,
    split_valid_and_rejected,
    to_silver,
)

BRONZE_SCHEMA = """
    utc_offset_seconds int,
    hourly_units struct<time: string, temperature_2m: string, relative_humidity_2m: string, precipitation: string, wind_speed_10m: string>,
    hourly struct<time: array<string>, temperature_2m: array<double>, relative_humidity_2m: array<int>, precipitation: array<double>, wind_speed_10m: array<double>>,
    station_id string, _batch_id string, _ingested_at timestamp, _source_file string
"""
UNITS = ("iso8601", "°C", "%", "mm", "km/h")
INGESTED = dt.datetime(2026, 9, 27, 12, 0)


def bronze_row(times, temps, humidity=None, precipitation=None, wind=None, *, station="buenos_aires", offset=0, units=UNITS, batch="run-1", ingested=INGESTED):
    n = len(times)
    hourly = (times, temps, humidity if humidity is not None else [60] * n, precipitation if precipitation is not None else [0.0] * n, wind if wind is not None else [10.0] * n)
    return (offset, units, hourly, station, batch, ingested, f"s3://raw/{batch}/{station}.json")


def readings(spark, *rows):
    return add_rejection_reasons(add_observed_at(flatten_hourly(spark.createDataFrame(list(rows), BRONZE_SCHEMA))))


def reasons_by_hour(df):
    return {row.observed_at_raw: row.rejection_reasons for row in df.collect()}


def test_parallel_arrays_become_one_row_per_hour_in_utc(spark):
    # A response in UTC-3: 2026-09-20T21:00 local is 2026-09-21T00:00 UTC.
    df = readings(spark, bronze_row(["2026-09-20T20:00", "2026-09-20T21:00"], [18.5, 17.0], offset=-3 * 3600))

    # Formatted inside Spark (session time zone UTC): collect() would turn timestamps into naive datetimes in the
    # laptop's local time zone, so a test comparing datetimes passes on Glue (UTC) and fails in Buenos Aires.
    rows = df.orderBy("observed_at").withColumn("utc", F.date_format("observed_at", "yyyy-MM-dd HH:mm")).collect()
    assert [(r.utc, r.temperature_c, r.relative_humidity_pct) for r in rows] == [
        ("2026-09-20 23:00", 18.5, 60),
        ("2026-09-21 00:00", 17.0, 60),
    ]
    assert all(r.rejection_reasons == [] for r in rows)


def test_nulls_are_rejected_not_passed_as_valid(spark):
    # null < 0 is null, not true: without an explicit rule these would pass the range checks as valid.
    df = readings(spark, bronze_row(["2026-09-20T00:00", "2026-09-20T01:00"], [None, 15.0], humidity=[60, None]))

    assert reasons_by_hour(df) == {
        "2026-09-20T00:00": ["missing_temperature_c"],
        "2026-09-20T01:00": ["missing_relative_humidity_pct"],
    }


def test_out_of_range_and_unparseable_times_are_rejected(spark):
    df = readings(spark, bronze_row(["2026-09-20T00:00", "not-a-time"], [75.0, 15.0], humidity=[101, 50], wind=[10.0, -1.0]))

    assert reasons_by_hour(df) == {
        "2026-09-20T00:00": ["out_of_range_temperature_c", "out_of_range_relative_humidity_pct"],
        "not-a-time": ["invalid_observed_at", "out_of_range_wind_speed_kmh"],
    }


def test_a_changed_or_missing_unit_rejects_the_reading(spark):
    # temperature_c must really be Celsius: a source switching to °F would otherwise publish 60 "°C" in summer.
    units = ("iso8601", "°F", "%", "mm", None)
    df = readings(spark, bronze_row(["2026-09-20T00:00"], [60.0], units=units))

    assert df.first().rejection_reasons == ["unexpected_unit_temperature_c", "unexpected_unit_wind_speed_kmh"]


def test_arrays_of_different_lengths_are_rejected_not_misaligned(spark):
    df = readings(spark, bronze_row(["2026-09-20T00:00", "2026-09-20T01:00"], [15.0], humidity=[60, 61]))

    assert reasons_by_hour(df) == {
        "2026-09-20T00:00": ["array_lengths_differ"],
        "2026-09-20T01:00": ["array_lengths_differ", "missing_temperature_c"],
    }


def test_every_row_lands_on_exactly_one_side(spark):
    df = readings(spark, bronze_row(["2026-09-20T00:00", "2026-09-20T01:00", "2026-09-20T02:00"], [15.0, None, 99.0]))

    valid, rejected = split_valid_and_rejected(df)

    assert (valid.count(), rejected.count()) == (1, 2)
    assert reconcile(3, valid.count(), rejected.count(), 1)["duplicates_dropped"] == 0


def test_the_most_recently_ingested_duplicate_wins(spark):
    earlier = bronze_row(["2026-09-20T00:00"], [15.0], batch="run-1", ingested=INGESTED)
    later = bronze_row(["2026-09-20T00:00"], [15.4], batch="run-2", ingested=INGESTED + dt.timedelta(hours=1))
    valid, _ = split_valid_and_rejected(readings(spark, later, earlier))

    unique = dedup_latest(valid).collect()

    assert [(r.temperature_c, r._batch_id) for r in unique] == [(15.4, "run-2")]
    assert reconcile(2, 2, 0, len(unique))["duplicates_dropped"] == 1


def test_counts_that_do_not_add_up_fail_the_run():
    with pytest.raises(ValueError, match="don't reconcile"):
        reconcile(input_rows=10, valid_rows=6, rejected_rows=3, unique_rows=6)


def test_a_rerun_changes_nothing_and_older_data_never_overwrites_newer(spark):
    current = to_silver(dedup_latest(split_valid_and_rejected(readings(spark, bronze_row(["2026-09-20T00:00", "2026-09-20T01:00"], [15.0, 16.0])))[0]))

    def updates(temps, ingested):
        return to_silver(dedup_latest(split_valid_and_rejected(readings(spark, bronze_row(["2026-09-20T00:00", "2026-09-20T01:00", "2026-09-20T02:00"], temps, ingested=ingested)))[0]))

    def counts(temps, ingested):
        return change_counts(classify_changes(updates(temps, ingested), current))

    assert counts([15.0, 16.0, 17.0], INGESTED) == {"new": 1, "changed": 0, "unchanged": 2}
    assert counts([15.0, 16.5, 17.0], INGESTED + dt.timedelta(hours=1)) == {"new": 1, "changed": 1, "unchanged": 1}
    # A late, older batch with different values is not allowed to replace what silver already has.
    assert counts([14.0, 16.0, 17.0], INGESTED - dt.timedelta(hours=1)) == {"new": 1, "changed": 0, "unchanged": 2}


def test_only_new_and_changed_rows_reach_the_merge(spark):
    # An unchanged row must not reach the MERGE at all: with copy-on-write, matching it rewrites its data file.
    base = readings(spark, bronze_row(["2026-09-20T00:00", "2026-09-20T01:00"], [15.0, 16.0]))
    current = to_silver(dedup_latest(split_valid_and_rejected(base)[0]))
    rerun = readings(spark, bronze_row(["2026-09-20T00:00", "2026-09-20T01:00", "2026-09-20T02:00"], [15.0, 16.5, 17.0], ingested=INGESTED + dt.timedelta(hours=1)))
    updates = to_silver(dedup_latest(split_valid_and_rejected(rerun)[0]))

    merged = rows_to_merge(classify_changes(updates, current))

    assert merged.columns == updates.columns
    assert sorted(r.temperature_c for r in merged.collect()) == [16.5, 17.0]
    assert rows_to_merge(classify_changes(current, current)).count() == 0
