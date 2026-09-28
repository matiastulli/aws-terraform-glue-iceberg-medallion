import datetime as dt
import json
import random
from pathlib import Path

import pytest

from medallion.config import Station, Stream, parse_config
from medallion.stream import BRONZE_COLUMNS, QUARANTINE_COLUMNS, simulate_readings, to_bronze_rows

NOW = dt.datetime(2026, 9, 28, 14, 7, 42, tzinfo=dt.timezone.utc)
INGESTED = dt.datetime(2026, 9, 28, 14, 8, 5, tzinfo=dt.timezone.utc)
STATIONS = (Station("buenos_aires", "Buenos Aires", -34.61, -58.38), Station("ushuaia", "Ushuaia", -54.8, -68.3))
GOOD = {
    "event_id": "e-1",
    "station_id": "buenos_aires",
    "observed_at": "2026-09-28T14:07:00Z",
    "temperature_c": 18.5,
    "relative_humidity_pct": 70,
    "precipitation_mm": 0,
    "wind_speed_kmh": 12.0,
}


def record(message_id, body):
    return {"messageId": message_id, "body": body if isinstance(body, str) else json.dumps(body), "attributes": {"SentTimestamp": "1790604462000"}}


def stream(duplicate_rate=0.0, late_rate=0.0):
    return Stream("simulator", "readings", duplicate_rate, late_rate, 90)


def test_the_repo_config_has_the_simulator_stream():
    config = parse_config((Path(__file__).parents[1] / "config" / "sources.toml").read_text(encoding="utf-8"))
    assert config.stream("simulator_readings").max_late_minutes == 90


def test_without_duplicates_or_late_events_each_station_sends_one_reading_for_the_current_minute():
    messages = simulate_readings(STATIONS, stream(), NOW, random.Random(1))

    assert [(m["station_id"], m["observed_at"]) for m in messages] == [("buenos_aires", "2026-09-28T14:07:00Z"), ("ushuaia", "2026-09-28T14:07:00Z")]
    assert len({m["event_id"] for m in messages}) == 2


def test_duplicates_repeat_the_event_id_and_late_readings_stay_within_the_limit():
    messages = simulate_readings(STATIONS * 50, stream(duplicate_rate=0.3, late_rate=0.3), NOW, random.Random(7))
    by_event = {}
    for m in messages:
        by_event.setdefault(m["event_id"], []).append(m)
    lateness = [NOW.replace(second=0) - dt.datetime.fromisoformat(m["observed_at"]) for m in messages]

    assert any(len(copies) == 2 for copies in by_event.values())
    assert all(copies[0] == copies[-1] for copies in by_event.values())  # a resend is the same reading
    assert any(late > dt.timedelta(0) for late in lateness)
    assert all(dt.timedelta(0) <= late <= dt.timedelta(minutes=90) for late in lateness)


def test_every_record_lands_on_exactly_one_side():
    records = [
        record("m1", GOOD),
        record("m2", "not json"),
        record("m3", [GOOD]),
        record("m4", GOOD | {"relative_humidity_pct": 70.5}),  # a float in an int column would be truncated
        record("m5", GOOD | {"relative_humidity_pct": True}),  # JSON true is not 1
        record("m6", GOOD | {"battery_pct": 80}),  # a field bronze has no column for would be lost
        record("m7", {k: v for k, v in GOOD.items() if k != "temperature_c"} | {"wind_speed_kmh": None}),  # nulls: silver's problem
    ]

    rows, rejects = to_bronze_rows(records, "batch-1", INGESTED)

    assert [r["_message_id"] for r in rows] == ["m1", "m7"]
    assert {r["_message_id"]: r["rejection_reasons"] for r in rejects} == {
        "m2": ["not JSON: Expecting value"],
        "m3": ["not a JSON object but a list"],
        "m4": ["relative_humidity_pct is not int"],
        "m5": ["relative_humidity_pct is not int"],
        "m6": ["unknown field battery_pct"],
    }
    assert rows[1]["temperature_c"] is None and rows[1]["wind_speed_kmh"] is None
    # The quarantine keeps the body exactly as received, for a person to look at.
    assert rejects[0]["body"] == "not json"
    assert all(tuple(r) == tuple(name for name, _ in QUARANTINE_COLUMNS) for r in rejects)


def test_a_row_has_every_bronze_column_with_sqs_and_lambda_metadata():
    rows, _ = to_bronze_rows([record("m1", GOOD)], "batch-1", INGESTED)

    assert tuple(rows[0]) == tuple(name for name, _ in BRONZE_COLUMNS)
    assert rows[0]["_sent_at"] == dt.datetime(2026, 9, 28, 14, 7, 42, tzinfo=dt.timezone.utc)
    assert (rows[0]["_batch_id"], rows[0]["_ingested_at"]) == ("batch-1", INGESTED)


def test_a_stream_with_an_impossible_rate_is_rejected():
    text = (Path(__file__).parents[1] / "config" / "sources.toml").read_text(encoding="utf-8").replace("late_rate = 0.1", "late_rate = 1.5")
    with pytest.raises(ValueError, match="rates must be between 0 and 1"):
        parse_config(text)
