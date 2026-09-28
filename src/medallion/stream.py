"""Live sensor readings (docs/PLAN.md step 6): what the simulator sends and how the consumer turns SQS messages into
bronze rows. No pyspark and no pyarrow here: both Lambdas import this, and the tests run without AWS.

The contract with bronze is strict. A message is appended only if it's a JSON object whose fields are exactly the
known ones, each with its JSON type (or null). Anything else goes to the quarantine table with its raw body and the
reasons: retrying a malformed message can't fix it, so it never goes back to the queue. The queue's retries and
dead-letter queue are for failures that a retry can fix, such as a failed Iceberg commit. Nothing is dropped silently.
"""

import datetime as dt
import json
import random
import uuid
from collections.abc import Iterable

from medallion.config import Station, Stream

# Message fields as the sensors send them, with their bronze (Iceberg) type. observed_at stays the sensor's string:
# bronze keeps what the source sent, and silver parses it.
MESSAGE_FIELDS = (
    ("event_id", "string"),
    ("station_id", "string"),
    ("observed_at", "string"),
    ("temperature_c", "double"),
    ("relative_humidity_pct", "int"),
    ("precipitation_mm", "double"),
    ("wind_speed_kmh", "double"),
)
METADATA_FIELDS = (
    ("_message_id", "string"),  # SQS message id: two deliveries of one message share it, a sensor's resend doesn't
    ("_sent_at", "timestamptz"),  # when SQS received the message
    ("_batch_id", "string"),  # the Lambda invocation that appended it: one batch, one Iceberg commit
    ("_ingested_at", "timestamptz"),
)
BRONZE_COLUMNS = (*MESSAGE_FIELDS, *METADATA_FIELDS)
QUARANTINE_COLUMNS = (
    ("_message_id", "string"),
    ("body", "string"),  # the message exactly as received, since it may not even be JSON
    ("rejection_reasons", "list<string>"),
    ("_sent_at", "timestamptz"),
    ("_batch_id", "string"),
    ("_ingested_at", "timestamptz"),
)


def simulate_readings(stations: Iterable[Station], stream: Stream, now: dt.datetime, rng: random.Random) -> list[dict]:
    """One reading per station for the current minute, plus, on purpose, what real sensors do: some readings sent
    twice (same event_id) and some that arrive late (observed_at up to max_late_minutes ago)."""
    minute = now.astimezone(dt.timezone.utc).replace(second=0, microsecond=0)
    messages = []
    for station in stations:
        observed_at = minute
        if rng.random() < stream.late_rate:
            observed_at -= dt.timedelta(minutes=rng.randint(1, stream.max_late_minutes))
        # Plausible values: colder away from the equator, mostly dry.
        reading = {
            "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            "station_id": station.station_id,
            "observed_at": observed_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "temperature_c": round(28 - 0.45 * abs(station.latitude) + rng.gauss(0, 2), 1),
            "relative_humidity_pct": rng.randint(35, 95),
            "precipitation_mm": round(rng.expovariate(2), 1) if rng.random() < 0.15 else 0.0,
            "wind_speed_kmh": round(abs(rng.gauss(15, 8)), 1),
        }
        messages.append(reading)
        if rng.random() < stream.duplicate_rate:
            messages.append(dict(reading))
    return messages


def _fits(value, bronze_type: str) -> bool:
    if value is None:
        return True
    if bronze_type == "string":
        return isinstance(value, str)
    if bronze_type == "int":
        return isinstance(value, int) and not isinstance(value, bool) and -(2**31) <= value < 2**31
    if bronze_type == "double":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    raise ValueError(f"no JSON check for type {bronze_type}")


def message_problems(body: str) -> tuple[dict | None, list[str]]:
    """The message as a dict, or None and every reason it can't be appended as-is."""
    try:
        message = json.loads(body)
    except json.JSONDecodeError as error:
        return None, [f"not JSON: {error.msg}"]
    if not isinstance(message, dict):
        return None, [f"not a JSON object but a {type(message).__name__}"]
    types = dict(MESSAGE_FIELDS)
    problems = [f"unknown field {name}" for name in message if name not in types]
    problems += [f"{name} is not {bronze_type}" for name, bronze_type in MESSAGE_FIELDS if not _fits(message.get(name), bronze_type)]
    return (None, problems) if problems else (message, [])


def to_bronze_rows(records: list[dict], batch_id: str, ingested_at: dt.datetime) -> tuple[list[dict], list[dict]]:
    """Splits an SQS batch into rows for the bronze table and rows for its quarantine.

    Every record lands on exactly one side. Missing fields become null (silver validates them); a float in an int
    field, an unknown field or a body that isn't JSON is quarantined, since appending it would lose or change data.
    """
    rows, rejects = [], []
    for record in records:
        message, problems = message_problems(record["body"])
        metadata = {
            "_message_id": record["messageId"],
            "_sent_at": dt.datetime.fromtimestamp(int(record["attributes"]["SentTimestamp"]) / 1000, dt.timezone.utc),
            "_batch_id": batch_id,
            "_ingested_at": ingested_at,
        }
        if problems:
            rejects.append({"_message_id": metadata["_message_id"], "body": record["body"], "rejection_reasons": problems} | metadata)
        else:
            rows.append({name: message.get(name) for name, _ in MESSAGE_FIELDS} | metadata)
    if len(rows) + len(rejects) != len(records):
        raise ValueError(f"{len(records)} records became {len(rows)} rows and {len(rejects)} rejects")
    return rows, rejects
