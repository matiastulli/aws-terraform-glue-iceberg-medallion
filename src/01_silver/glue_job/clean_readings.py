"""Glue job: turn one bronze batch into silver readings and quarantined rejects.

Reads the bronze rows of --batch_id (the Step Functions execution that loaded them), flattens them to one row per
station-hour, validates them, and MERGEs the valid ones into 01_silver.readings on (station_id, observed_at). Rejected
rows go to 01_silver.readings_quarantine with their rejection_reasons.

Safe to run twice: the MERGE inserts only new keys and updates only changed values from data that isn't older, so a
rerun of the same batch inserts and updates 0 rows. Nothing is written unless every input row is accounted for
exactly once (reconcile) and both outputs match their tables' DDL (contract).
"""

import json
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from medallion.contract import raise_if_schema_mismatch
from medallion.silver import (
    KEY,
    MEASURES,
    add_observed_at,
    add_rejection_reasons,
    change_counts,
    classify_changes,
    dedup_latest,
    flatten_hourly,
    reconcile,
    rows_to_merge,
    split_valid_and_rejected,
    to_quarantine,
    to_silver,
)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "catalog", "bronze_db", "silver_db", "batch_id"])
spark = SparkSession.builder.getOrCreate()
catalog, batch_id = args["catalog"], args["batch_id"]
bronze_table = f"`{catalog}`.`{args['bronze_db']}`.open_meteo_hourly"
readings_table = f"`{catalog}`.`{args['silver_db']}`.readings"
quarantine_table = f"`{catalog}`.`{args['silver_db']}`.readings_quarantine"

try:
    bronze, readings, quarantine = (spark.table(t) for t in (bronze_table, readings_table, quarantine_table))
except AnalysisException as error:
    raise RuntimeError(f"a table is missing: run apply_ddl first (jobs never create tables): {error}") from error

batch = bronze.where(F.col("_batch_id") == batch_id)
checked = add_rejection_reasons(add_observed_at(flatten_hourly(batch))).cache()
valid, rejected = split_valid_and_rejected(checked)
unique = dedup_latest(valid)

counts = reconcile(checked.count(), valid.count(), rejected.count(), unique.count())
if counts["input_rows"] == 0:
    raise RuntimeError(f"bronze has no rows for batch {batch_id!r}: nothing to clean, so the load step didn't run or wrote elsewhere")

updates = to_silver(unique)
rejects = to_quarantine(rejected)
raise_if_schema_mismatch(updates.dtypes, readings.dtypes, readings_table)
raise_if_schema_mismatch(rejects.dtypes, quarantine.dtypes, quarantine_table)

# Only new and changed rows reach the MERGE, and an empty MERGE is skipped: with copy-on-write, a matched key rewrites
# its whole data file even when nothing changes, so a rerun would rewrite silver and commit a snapshot for nothing.
classified = classify_changes(updates, readings).cache()
counts |= change_counts(classified)
to_merge = rows_to_merge(classified)
new_rejects = rejects.join(
    quarantine.select("station_id", "observed_at_raw", "_batch_id"),
    [rejects.station_id.eqNullSafe(quarantine.station_id), rejects.observed_at_raw.eqNullSafe(quarantine.observed_at_raw), rejects._batch_id == quarantine._batch_id],
    "left_anti",
).cache()
counts["quarantined_now"] = new_rejects.count()

# The temp views are the MERGE sources; they live only for this run (no DDL, per the conventions).
to_merge.createOrReplaceTempView("clean_readings_updates")
new_rejects.createOrReplaceTempView("clean_readings_rejects")

# The conditions repeat what classify_changes decided, so the MERGE stays correct on its own (e.g. if another writer
# committed in between).
values_differ = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in MEASURES)
if counts["new"] + counts["changed"]:
    spark.sql(f"""
    MERGE INTO {readings_table} t
    USING clean_readings_updates s
    ON {" AND ".join(f"t.{k} = s.{k}" for k in KEY)}
    WHEN MATCHED AND s._ingested_at >= t._ingested_at AND ({values_differ}) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# <=> (null-safe equality): a reject may have a null station_id or time, and `null = null` would never match, so a
# rerun would insert the same reject again.
if counts["quarantined_now"]:
    spark.sql(f"""
    MERGE INTO {quarantine_table} t
    USING clean_readings_rejects s
    ON t.station_id <=> s.station_id AND t.observed_at_raw <=> s.observed_at_raw AND t._batch_id = s._batch_id
    WHEN NOT MATCHED THEN INSERT *
    """)


def latest_snapshot(table: str):
    row = spark.sql(f"SELECT snapshot_id, operation, summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return None if row is None else {"snapshot_id": row.snapshot_id, "operation": row.operation, "added_records": row.summary.get("added-records", "0")}


print(json.dumps({
    "batch_id": batch_id,
    **counts,
    "readings_snapshot": latest_snapshot(readings_table),
    "quarantine_snapshot": latest_snapshot(quarantine_table),
}))
