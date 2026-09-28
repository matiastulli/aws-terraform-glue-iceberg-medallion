"""Glue job: move the live sensor readings from bronze to silver with Spark Structured Streaming (docs/PLAN.md step 9).

Reads "00_bronze".simulator_readings as a stream: only the snapshots appended since the last run (the checkpoint in
--checkpoint remembers where it stopped). Each micro-batch is validated, deduplicated and MERGEd into
01_silver.sensor_readings on (station_id, observed_at), rejects to sensor_readings_quarantine, exactly like the batch
silver jobs. At-least-once from the checkpoint + an idempotent MERGE = effectively exactly-once.

trigger(once=True), not availableNow: with an Iceberg source, availableNow plans from the stream's initial offset, so
expiring the table's first snapshot broke it for good; once only needs the last consumed snapshot (measured locally).

First run (no checkpoint yet): an initial load reads the current snapshot as a batch, and the stream starts from its
commit time (stream-from-timestamp). A stream alone would skip rows that only exist in `replace` snapshots, and after
table maintenance (step 7) that's the whole table: a fresh stream read 0 of its rows (measured locally). Anything
read twice at the boundary is absorbed by the MERGE.

After the query ends without error, the job records that snapshot in DynamoDB (--watermark_table), moving forward only.
maintain_tables reads it and never expires a snapshot the stream still needs. A failed run records nothing.

Never delete the checkpoint to "clean up": the next run would reprocess the whole table. One run at a time: two runs
on one checkpoint would corrupt it.
"""

import datetime as dt
import json
import sys

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from medallion import sensor_readings as entity
from medallion.config import split_s3_uri
from medallion.contract import raise_if_schema_mismatch
from medallion.readings import MEASURES
from medallion.silver import (
    batch_bounds,
    between_sql,
    change_counts,
    classify_changes,
    dedup_latest,
    merge_sql,
    new_rejects,
    quarantine_merge_sql,
    reconcile,
    rows_to_merge,
    split_valid_and_rejected,
)

# Step 1: Read the job arguments and create the Spark session.
args = getResolvedOptions(sys.argv, ["JOB_NAME", "catalog", "bronze_db", "silver_db", "checkpoint", "watermark_table"])
spark = SparkSession.builder.getOrCreate()
catalog = args["catalog"]
source = f"{args['bronze_db']}.simulator_readings"
bronze_table = f"`{catalog}`.`{args['bronze_db']}`.simulator_readings"
silver_table = f"`{catalog}`.`{args['silver_db']}`.sensor_readings"
quarantine_table = f"`{catalog}`.`{args['silver_db']}`.sensor_readings_quarantine"

# Step 2: Load the silver and quarantine tables to compare against them during validation and MERGE.
try:
    silver, quarantine = spark.table(silver_table), spark.table(quarantine_table)
except AnalysisException as error:
    raise RuntimeError(f"a table is missing: run apply_ddl first (jobs never create tables): {error}") from error

# Step 3: Track totals across all micro-batches for the run summary.
totals = {"batches": 0, "input_rows": 0, "new": 0, "changed": 0, "unchanged": 0, "duplicates_dropped": 0, "quarantined_now": 0}


def clean_batch(bronze: DataFrame, batch_id: int) -> None:
    """One micro-batch, the same way the batch silver jobs clean one bronze batch. An exception fails the query before
    the checkpoint records this batch, so the next run gets it again.

    Everything here runs on the micro-batch's own session (bronze.sparkSession), not the outer `spark`: foreachBatch
    hands over a DataFrame from a cloned session, so a temp view registered from it isn't visible to spark.sql
    (TABLE_OR_VIEW_NOT_FOUND on AWS; the initial load, a plain batch DataFrame, didn't show it)."""
    session = bronze.sparkSession
    # Step 4: Validate each micro-batch: add rejection reasons and observed_at, then split valid vs rejected rows.
    checked = entity.add_rejection_reasons(entity.add_observed_at(bronze)).cache()
    valid, rejected = split_valid_and_rejected(checked)

    # Step 5: Deduplicate valid rows to keep the latest row per sensor key.
    unique = dedup_latest(valid, entity.KEY, tiebreak=entity.TIEBREAK)
    counts = reconcile(checked.count(), valid.count(), rejected.count(), unique.count())

    # Step 6: Convert the cleaned records into the silver and quarantine output schemas.
    updates, rejects = entity.to_silver(unique), entity.to_quarantine(rejected)
    raise_if_schema_mismatch(updates.dtypes, silver.dtypes, silver_table)
    raise_if_schema_mismatch(rejects.dtypes, quarantine.dtypes, quarantine_table)

    # Step 7: Narrow the comparison to the same time window as this batch, then classify each row as new/changed/unchanged.
    # silver bounded to the batch's minutes, and the same literal range in the MERGE's ON (step 3's review fix).
    bounds = batch_bounds(updates, "observed_at")
    current = session.table(silver_table).where(between_sql("observed_at", bounds))
    classified = classify_changes(updates, current, entity.KEY, MEASURES).cache()
    counts |= change_counts(classified)

    # Step 8: Identify rows that should be added to the quarantine table.
    to_quarantine = new_rejects(rejects, session.table(quarantine_table), entity.QUARANTINE_MATCH).cache()
    counts["quarantined_now"] = to_quarantine.count()

    # Step 9: Create temp views for the MERGE sources.
    rows_to_merge(classified).createOrReplaceTempView("clean_sensor_readings_updates")
    to_quarantine.createOrReplaceTempView("clean_sensor_readings_rejects")

    # Step 10: Apply the silver MERGE and quarantine MERGE only when there are rows to write.
    if counts["new"] + counts["changed"]:
        session.sql(merge_sql(silver_table, "clean_sensor_readings_updates", entity.KEY, MEASURES, between_sql("t.observed_at", bounds)))
    if counts["quarantined_now"]:
        session.sql(quarantine_merge_sql(quarantine_table, "clean_sensor_readings_rejects", entity.QUARANTINE_MATCH))

    # Step 11: Accumulate the batch totals for the final summary.
    totals["batches"] += 1
    for name in ("input_rows", "new", "changed", "unchanged", "duplicates_dropped", "quarantined_now"):
        totals[name] += counts[name]
    print(json.dumps({"micro_batch": batch_id, **counts}))
    checked.unpersist()


# Step 12: Detect whether this is the first run and, if so, load the latest snapshot from bronze before the stream starts.
checkpoint_bucket, checkpoint_prefix = split_s3_uri(args["checkpoint"])
first_run = "Contents" not in boto3.client("s3").list_objects_v2(Bucket=checkpoint_bucket, Prefix=checkpoint_prefix, MaxKeys=1)
reader = spark.readStream.format("iceberg")
initial_snapshot = None
if first_run:
    current = spark.sql(f"SELECT snapshot_id, unix_millis(committed_at) AS ms FROM {bronze_table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    if current is not None:
        print(json.dumps({"initial_load_from_snapshot": current.snapshot_id}))
        clean_batch(spark.read.format("iceberg").option("snapshot-id", current.snapshot_id).load(bronze_table), -1)
        reader = reader.option("stream-from-timestamp", str(current.ms))
        initial_snapshot = current.snapshot_id

# Step 13: Start the stream and process each new micro-batch with foreachBatch.
query = (
    reader.load(bronze_table)
    .writeStream.trigger(once=True)
    .option("checkpointLocation", args["checkpoint"])
    .foreachBatch(clean_batch)
    .start()
)
query.awaitTermination()  # raises if a micro-batch failed: nothing below runs, the watermark doesn't move

# Step 14: Read the end offset and determine the snapshot that was successfully consumed.
progress = query.lastProgress
end_offset = progress["sources"][0]["endOffset"] if progress and progress["sources"] else None
# A first run with nothing new after the initial load reports no offset, but the checkpoint now starts at the initial
# load's snapshot, so that is the one to keep (a first run on AWS wrote no watermark before this).
consumed_snapshot = int(end_offset["snapshot_id"]) if end_offset else initial_snapshot
watermark = None
if consumed_snapshot is not None:
    snapshot_id = consumed_snapshot
    # Step 15: Look up the commit time for that snapshot and advance the watermark in DynamoDB.
    # Formatted in Spark (UTC session): collect() would return the machine's local time.
    committed_at = spark.sql(
        f"SELECT date_format(committed_at, \"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\") AS at FROM {bronze_table}.snapshots WHERE snapshot_id = {snapshot_id}"
    ).first().at
    table = boto3.resource("dynamodb").Table(args["watermark_table"])
    try:
        # Forward only: an older snapshot never replaces a newer watermark (ISO 8601 UTC strings sort by time).
        table.update_item(
            Key={"process": "clean_sensor_readings"},
            UpdateExpression="SET source_table = :source, snapshot_id = :id, snapshot_committed_at = :at, last_run = :run",
            ConditionExpression="attribute_not_exists(snapshot_committed_at) OR snapshot_committed_at <= :at",
            ExpressionAttributeValues={
                ":source": source,
                ":id": snapshot_id,
                ":at": committed_at,
                ":run": {k: v for k, v in totals.items()} | {"finished_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")},
            },
        )
        watermark = {"snapshot_id": snapshot_id, "snapshot_committed_at": committed_at}
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        watermark = {"skipped": f"the stored watermark is newer than {committed_at}"}

# Step 16: Print the final summary with source, totals, and watermark information.
print(json.dumps({"source": source, "first_run": first_run, **totals, "watermark": watermark}))
