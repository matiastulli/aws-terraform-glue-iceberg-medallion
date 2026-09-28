"""Glue job: turn one bronze batch of Wikidata responses into silver populations and quarantined rejects.

Reads the wikidata_population rows of --batch_id (the Step Functions execution that loaded them), flattens them to one
row per population statement, validates them (medallion/populations.py), and MERGEs the valid ones into
01_silver.populations on (station_id, reference_date), the preferred statement winning a shared date. Rejected
statements go to 01_silver.populations_quarantine with their rejection_reasons.

Safe to run twice: only new and changed rows reach the MERGE, so a rerun of the same batch writes nothing. Nothing is
written unless every input row is accounted for exactly once (reconcile) and both outputs match their DDL (contract).
"""

import json
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from medallion import populations as entity
from medallion.contract import raise_if_schema_mismatch
from medallion.silver import (
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
args = getResolvedOptions(sys.argv, ["JOB_NAME", "catalog", "bronze_db", "silver_db", "batch_id"])
spark = SparkSession.builder.getOrCreate()
catalog, batch_id = args["catalog"], args["batch_id"]
bronze_table = f"`{catalog}`.`{args['bronze_db']}`.wikidata_population"
silver_table = f"`{catalog}`.`{args['silver_db']}`.populations"
quarantine_table = f"`{catalog}`.`{args['silver_db']}`.populations_quarantine"

# Step 2: Load the bronze and silver tables that this job will read and compare against.
try:
    bronze, silver, quarantine = (spark.table(t) for t in (bronze_table, silver_table, quarantine_table))
except AnalysisException as error:
    raise RuntimeError(f"a table is missing: run apply_ddl first (jobs never create tables): {error}") from error

# Step 3: Filter the bronze batch, flatten the nested Wikidata payload, and add validation reasons.
checked = entity.add_rejection_reasons(entity.flatten_bindings(bronze.where(F.col("_batch_id") == batch_id))).cache()

# Step 4: Split the rows into valid and rejected records before any writes happen.
valid, rejected = split_valid_and_rejected(checked)

# Step 5: Remove duplicate valid records by keeping the preferred statement per key.
unique = dedup_latest(valid, entity.KEY, entity.preference())

# Step 6: Reconcile accounting so we can assert that every input row is handled exactly once.
counts = reconcile(checked.count(), valid.count(), rejected.count(), unique.count())
if counts["input_rows"] == 0:
    raise RuntimeError(f"{bronze_table} has no rows for batch {batch_id!r}: the load step didn't run or wrote elsewhere")

# Step 7: Convert the valid and rejected records into the silver/quarantine table shapes.
updates, rejects = entity.to_silver(unique), entity.to_quarantine(rejected)
raise_if_schema_mismatch(updates.dtypes, silver.dtypes, silver_table)
raise_if_schema_mismatch(rejects.dtypes, quarantine.dtypes, quarantine_table)

# Step 8: Compare the candidate silver updates to the existing table and classify each row as new, changed, or unchanged.
# Only new and changed rows reach the MERGE, and an empty MERGE is skipped: with copy-on-write, a matched key rewrites
# its whole data file even when nothing changes (docs/PLAN.md step 3).
classified = classify_changes(updates, silver, entity.KEY, entity.VALUES).cache()
counts |= change_counts(classified)

# Step 9: Identify quarantined rows that should be inserted into the quarantine table.
to_quarantine = new_rejects(rejects, quarantine, entity.QUARANTINE_MATCH).cache()
counts["quarantined_now"] = to_quarantine.count()

# Step 10: Build temporary views; these are the MERGE sources for this run only.
# Temp views are the MERGE sources; they live only for this run (no DDL, per the conventions).
rows_to_merge(classified).createOrReplaceTempView("clean_populations_updates")
to_quarantine.createOrReplaceTempView("clean_populations_rejects")

# Step 11: Run the silver MERGE for rows that are new or changed.
if counts["new"] + counts["changed"]:
    spark.sql(merge_sql(silver_table, "clean_populations_updates", entity.KEY, entity.VALUES))

# Step 12: Run the quarantine MERGE for new rejected rows.
if counts["quarantined_now"]:
    spark.sql(quarantine_merge_sql(quarantine_table, "clean_populations_rejects", entity.QUARANTINE_MATCH))


def latest_snapshot(table: str):
    row = spark.sql(f"SELECT snapshot_id, operation, summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return None if row is None else {"snapshot_id": row.snapshot_id, "operation": row.operation, "added_records": row.summary.get("added-records", "0")}


# Step 13: Print the final counts and snapshot metadata for the pipeline run.
print(json.dumps({"batch_id": batch_id, **counts, "silver_snapshot": latest_snapshot(silver_table), "quarantine_snapshot": latest_snapshot(quarantine_table)}))
