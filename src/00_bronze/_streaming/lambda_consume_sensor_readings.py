"""Lambda: append a batch of sensor messages from SQS to the stream's bronze table, with pyiceberg (no Spark).

Invoked by the SQS event source mapping with up to a batch of messages. Every message lands in exactly one place
(medallion/stream.py to_bronze_rows): the bronze table, or its _quarantine table when it can't be stored as sent.
Each table gets one append, so one Iceberg commit per batch.

If a write fails, only that write's messages are returned in batchItemFailures (ReportBatchItemFailures): SQS
redelivers just those, and after 3 receives moves them to the dead-letter queue, which has an alarm. A retry after a
commit that did land appends the rows again: bronze is at-least-once, and silver dedups on (station_id, observed_at).

Then the newest reading of each station goes to the DynamoDB table LATEST_TABLE (one item per station), for an app
that asks "how is Ushuaia now?" in milliseconds. A conditional write keeps a late reading from replacing a newer one.
It's a view derived from bronze, written only after the bronze commit: if it fails, the error is logged and the
messages are not returned to the queue (that would append them to bronze again); the station's next reading fixes it.

Environment: STREAM (the stream's bronze table, e.g. simulator_readings), BRONZE_DB, LATEST_TABLE.
"""

import datetime as dt
import json
import os
import time
from decimal import Decimal
from pathlib import Path

import boto3
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import CommitFailedException

from medallion.config import parse_config
from medallion.contract import raise_if_schema_mismatch
from medallion.stream import BRONZE_COLUMNS, QUARANTINE_COLUMNS, latest_per_station, to_bronze_rows

CONFIG = parse_config((Path(__file__).parent / "sources.toml").read_text(encoding="utf-8"))
STREAM = CONFIG.stream(os.environ["STREAM"])
BRONZE_DB = os.environ["BRONZE_DB"]
# Credentials come from the Lambda role; the region is set for both the catalog and the S3 file IO.
catalog = load_catalog("glue", type="glue", **{"glue.region": os.environ["AWS_REGION"], "s3.region": os.environ["AWS_REGION"]})
COMMIT_ATTEMPTS = 5
latest_table = boto3.resource("dynamodb").Table(os.environ["LATEST_TABLE"])


def append(table_name: str, columns, rows: list[dict]) -> int:
    """One append = one Iceberg commit. Checks the table against the columns this code writes first (jobs never
    create or change tables), and retries a commit that lost the race with a concurrent consumer."""
    table = catalog.load_table((BRONZE_DB, table_name))
    raise_if_schema_mismatch(list(columns), [(f.name, str(f.field_type)) for f in table.schema().fields], f"{BRONZE_DB}.{table_name}")
    data = pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        try:
            table.append(data)
            return table.metadata.current_snapshot_id
        except CommitFailedException:
            # Another consumer committed first: reload the table's current metadata and commit on top of it.
            if attempt == COMMIT_ATTEMPTS:
                raise
            time.sleep(0.2 * 2**attempt)
            table.refresh()


def publish_latest(rows: list[dict]) -> dict[str, int]:
    """One conditional update per station: only if this reading is newer than the stored one. observed_at is
    normalized to YYYY-MM-DDTHH:MM:SSZ (latest_per_station), so comparing the strings compares the times."""
    counts = {"updated": 0, "late_skipped": 0}
    for station, reading in latest_per_station(rows).items():
        values = {name: Decimal(str(value)) if isinstance(value, float) else value for name, value in reading.items() if name != "station_id"}
        values["_sent_at"] = reading["_sent_at"].isoformat(timespec="milliseconds")
        names = list(values)  # placeholders by position: #a0 / :v0 are always valid, whatever the column names
        at = names.index("observed_at")
        try:
            latest_table.update_item(
                Key={"station_id": station},
                UpdateExpression="SET " + ", ".join(f"#a{i} = :v{i}" for i in range(len(names))),
                ConditionExpression=f"attribute_not_exists(#a{at}) OR #a{at} < :v{at}",
                ExpressionAttributeNames={f"#a{i}": name for i, name in enumerate(names)},
                ExpressionAttributeValues={f":v{i}": values[name] for i, name in enumerate(names)},
            )
            counts["updated"] += 1
        except latest_table.meta.client.exceptions.ConditionalCheckFailedException:
            counts["late_skipped"] += 1  # the table already has a newer reading for this station
    return counts


def handler(event, context):
    records = event["Records"]
    rows, rejects = to_bronze_rows(records, context.aws_request_id, dt.datetime.now(dt.timezone.utc))
    summary = {"batch_id": context.aws_request_id, "messages": len(records), "appended": len(rows), "quarantined": len(rejects)}
    failures = []
    for table_name, columns, batch in (
        (STREAM.bronze_table, BRONZE_COLUMNS, rows),
        (f"{STREAM.bronze_table}_quarantine", QUARANTINE_COLUMNS, rejects),
    ):
        if not batch:
            continue
        try:
            summary[f"{table_name}_snapshot"] = append(table_name, columns, batch)
        except Exception as error:  # report these messages for redelivery, keep the other table's commit
            summary[f"{table_name}_error"] = repr(error)
            failures += [{"itemIdentifier": row["_message_id"]} for row in batch]
    if rows and f"{STREAM.bronze_table}_snapshot" in summary:  # only readings that bronze has
        try:
            summary["latest_readings"] = publish_latest(rows)
        except Exception as error:  # derived view: log it, don't send the messages back to bronze
            summary["latest_readings_error"] = repr(error)
    summary["returned_to_queue"] = len(failures)
    print(json.dumps(summary))
    return {"batchItemFailures": failures}
