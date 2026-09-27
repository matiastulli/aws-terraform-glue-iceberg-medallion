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

args = getResolvedOptions(sys.argv, ["JOB_NAME", "catalog", "bronze_db", "silver_db", "batch_id"])
spark = SparkSession.builder.getOrCreate()
catalog, batch_id = args["catalog"], args["batch_id"]
bronze_table = f"`{catalog}`.`{args['bronze_db']}`.wikidata_population"
silver_table = f"`{catalog}`.`{args['silver_db']}`.populations"
quarantine_table = f"`{catalog}`.`{args['silver_db']}`.populations_quarantine"

try:
    bronze, silver, quarantine = (spark.table(t) for t in (bronze_table, silver_table, quarantine_table))
except AnalysisException as error:
    raise RuntimeError(f"a table is missing: run apply_ddl first (jobs never create tables): {error}") from error

checked = entity.add_rejection_reasons(entity.flatten_bindings(bronze.where(F.col("_batch_id") == batch_id))).cache()
valid, rejected = split_valid_and_rejected(checked)
unique = dedup_latest(valid, entity.KEY, entity.preference())
counts = reconcile(checked.count(), valid.count(), rejected.count(), unique.count())
if counts["input_rows"] == 0:
    raise RuntimeError(f"{bronze_table} has no rows for batch {batch_id!r}: the load step didn't run or wrote elsewhere")

updates, rejects = entity.to_silver(unique), entity.to_quarantine(rejected)
raise_if_schema_mismatch(updates.dtypes, silver.dtypes, silver_table)
raise_if_schema_mismatch(rejects.dtypes, quarantine.dtypes, quarantine_table)

# Only new and changed rows reach the MERGE, and an empty MERGE is skipped: with copy-on-write, a matched key rewrites
# its whole data file even when nothing changes (docs/PLAN.md step 3).
classified = classify_changes(updates, silver, entity.KEY, entity.VALUES).cache()
counts |= change_counts(classified)
to_quarantine = new_rejects(rejects, quarantine, entity.QUARANTINE_MATCH).cache()
counts["quarantined_now"] = to_quarantine.count()

# Temp views are the MERGE sources; they live only for this run (no DDL, per the conventions).
rows_to_merge(classified).createOrReplaceTempView("clean_populations_updates")
to_quarantine.createOrReplaceTempView("clean_populations_rejects")
if counts["new"] + counts["changed"]:
    spark.sql(merge_sql(silver_table, "clean_populations_updates", entity.KEY, entity.VALUES))
if counts["quarantined_now"]:
    spark.sql(quarantine_merge_sql(quarantine_table, "clean_populations_rejects", entity.QUARANTINE_MATCH))


def latest_snapshot(table: str):
    row = spark.sql(f"SELECT snapshot_id, operation, summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return None if row is None else {"snapshot_id": row.snapshot_id, "operation": row.operation, "added_records": row.summary.get("added-records", "0")}


print(json.dumps({"batch_id": batch_id, **counts, "silver_snapshot": latest_snapshot(silver_table), "quarantine_snapshot": latest_snapshot(quarantine_table)}))
