"""Lambda: append a batch of sensor messages from SQS to the stream's bronze table, with pyiceberg (no Spark).

Invoked by the SQS event source mapping with up to a batch of messages. Every message lands in exactly one place
(medallion/stream.py to_bronze_rows): the bronze table, or its _quarantine table when it can't be stored as sent.
Each table gets one append, so one Iceberg commit per batch.

If a write fails, only that write's messages are returned in batchItemFailures (ReportBatchItemFailures): SQS
redelivers just those, and after 3 receives moves them to the dead-letter queue, which has an alarm. A retry after a
commit that did land appends the rows again: bronze is at-least-once, and silver dedups on (station_id, observed_at).

Environment: STREAM (the stream's bronze table, e.g. simulator_readings), BRONZE_DB.
"""

import datetime as dt
import json
import os
import time
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import CommitFailedException

from medallion.config import parse_config
from medallion.contract import raise_if_schema_mismatch
from medallion.stream import BRONZE_COLUMNS, QUARANTINE_COLUMNS, to_bronze_rows

CONFIG = parse_config((Path(__file__).parent / "sources.toml").read_text(encoding="utf-8"))
STREAM = CONFIG.stream(os.environ["STREAM"])
BRONZE_DB = os.environ["BRONZE_DB"]
# Credentials come from the Lambda role; the region is set for both the catalog and the S3 file IO.
catalog = load_catalog("glue", type="glue", **{"glue.region": os.environ["AWS_REGION"], "s3.region": os.environ["AWS_REGION"]})
COMMIT_ATTEMPTS = 5


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
    summary["returned_to_queue"] = len(failures)
    print(json.dumps(summary))
    return {"batchItemFailures": failures}
