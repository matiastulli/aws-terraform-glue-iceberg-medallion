"""Glue job: rebuild 02_gold.agg_readings_daily from silver readings and populations, in the order
compute -> data quality checks -> write, so a failing check raises before anything is published.

A full rebuild (the data is small, and the 7-day window needs the days before anyway), written with an overwrite of
the whole table. If the rebuild is identical to what's published, nothing is written: a rerun commits no snapshot.
"""

import json
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from medallion.contract import raise_if_schema_mismatch
from medallion.gold import build_agg_readings_daily, differs_from
from medallion.quality import gold_checks, raise_if_any_failed

args = getResolvedOptions(sys.argv, ["JOB_NAME", "catalog", "silver_db", "gold_db"])
spark = SparkSession.builder.getOrCreate()
catalog = args["catalog"]
readings_table = f"`{catalog}`.`{args['silver_db']}`.readings"
populations_table = f"`{catalog}`.`{args['silver_db']}`.populations"
gold_table = f"`{catalog}`.`{args['gold_db']}`.agg_readings_daily"

try:
    readings, populations, current = (spark.table(t) for t in (readings_table, populations_table, gold_table))
except AnalysisException as error:
    raise RuntimeError(f"a table is missing: run apply_ddl first (jobs never create tables): {error}") from error

# 1. Compute.
gold = build_agg_readings_daily(readings, populations).cache()

# 2. Check: raises DataQualityError before any write, so consumers keep the last good version.
checks = gold_checks(gold, readings)
print(json.dumps({"checks": [{"name": c.name, "passed": c.passed, "detail": c.detail} for c in checks]}))
raise_if_any_failed(checks)
raise_if_schema_mismatch(gold.dtypes, current.dtypes, gold_table)

# 3. Write, only if something changed.
published = differs_from(gold, current)
if published:
    gold.writeTo(gold_table).overwrite(F.lit(True))

snapshot = spark.sql(f"SELECT snapshot_id FROM {gold_table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
print(json.dumps({
    "table": gold_table,
    "rows": gold.count(),
    "published": published,
    "snapshot_id": snapshot.snapshot_id if snapshot else None,
}))
