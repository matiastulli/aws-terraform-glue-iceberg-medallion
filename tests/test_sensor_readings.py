import datetime as dt

from pyspark.sql import functions as F

from medallion.sensor_readings import KEY, TIEBREAK, add_observed_at, add_rejection_reasons, to_quarantine
from medallion.silver import dedup_latest, split_valid_and_rejected

BRONZE_SCHEMA = """
    event_id string, station_id string, observed_at string, temperature_c double, relative_humidity_pct int,
    precipitation_mm double, wind_speed_kmh double, _message_id string, _sent_at timestamp, _batch_id string, _ingested_at timestamp
"""
INGESTED = dt.datetime(2026, 9, 28, 17, 20)


def bronze(spark, *rows):
    return spark.createDataFrame(list(rows), BRONZE_SCHEMA)


def row(message_id, *, event="e1", station="buenos_aires", observed="2026-09-28T17:16:00Z", temp=14.2, humidity=60, batch="b1", ingested=INGESTED):
    return (event, station, observed, temp, humidity, 0.0, 10.0, message_id, INGESTED, batch, ingested)


def checked(spark, *rows):
    return add_rejection_reasons(add_observed_at(bronze(spark, *rows)))


def test_the_sensor_time_string_is_parsed_as_utc_and_a_bad_one_is_rejected(spark):
    result = checked(spark, row("m1"), row("m2", observed="28/09/2026 17:16"), row("m3", observed=None))

    by_message = {r._message_id: r for r in result.select("_message_id", F.date_format("observed_at", "yyyy-MM-dd HH:mm").alias("at"), "rejection_reasons").collect()}
    assert by_message["m1"].at == "2026-09-28 17:16" and by_message["m1"].rejection_reasons == []
    assert by_message["m2"].rejection_reasons == ["invalid_observed_at"]
    assert by_message["m3"].rejection_reasons == ["invalid_observed_at"]


def test_nulls_are_their_own_rule_and_ranges_only_apply_to_present_values(spark):
    result = checked(spark, row("m1", temp=None), row("m2", temp=75.0, humidity=None), row("m3", station=None))

    reasons = {r._message_id: r.rejection_reasons for r in result.collect()}
    assert reasons == {
        "m1": ["missing_temperature_c"],
        "m2": ["out_of_range_temperature_c", "missing_relative_humidity_pct"],
        "m3": ["missing_station_id"],
    }


def test_redeliveries_and_resends_collapse_to_the_latest_reading_per_minute(spark):
    # m1/m2: the same event delivered twice by SQS; m3: the sensor resent it later with a corrected value.
    later = INGESTED + dt.timedelta(minutes=1)
    valid, _ = split_valid_and_rejected(checked(spark, row("m1"), row("m2"), row("m3", event="e1", temp=14.5, batch="b2", ingested=later)))

    unique = dedup_latest(valid, KEY, tiebreak=TIEBREAK).collect()

    assert [(r._message_id, r.temperature_c) for r in unique] == [("m3", 14.5)]


def test_ties_are_broken_the_same_way_whatever_the_order(spark):
    a, b = row("m-a", temp=1.0), row("m-b", temp=2.0)
    first = dedup_latest(split_valid_and_rejected(checked(spark, a, b))[0], KEY, tiebreak=TIEBREAK).first()
    second = dedup_latest(split_valid_and_rejected(checked(spark, b, a))[0], KEY, tiebreak=TIEBREAK).first()

    assert first._message_id == second._message_id == "m-b"


def test_a_rejected_delivery_is_quarantined_once(spark):
    _, rejected = split_valid_and_rejected(checked(spark, row("m1", temp=None), row("m1", temp=None, batch="b2")))

    assert to_quarantine(rejected).count() == 1
