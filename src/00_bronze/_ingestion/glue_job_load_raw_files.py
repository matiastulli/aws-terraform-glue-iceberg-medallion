"""Glue job: load one day of raw files for one source into its bronze table.

Generic over sources: --source names the bronze table (<system>_<table>, from config/sources.toml), which decides the
raw prefix to read. Adding a source is a config entry plus its migration, never a new job.

The table must already exist (apply_ddl creates it). The raw JSON is read with the table's schema, never inferred,
and the result must match the table exactly before it's appended. Bronze is append-only: a rerun appends the same
files again under a new _batch_id, and silver's MERGE on (station_id, observed_at) absorbs the repeat.
"""

import json
import sys
from datetime import date

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from medallion.bronze import add_ingestion_metadata, source_schema
from medallion.config import parse_config, raw_prefix, split_s3_uri
from medallion.contract import raise_if_schema_mismatch

args = getResolvedOptions(sys.argv, ["JOB_NAME", "config_path", "catalog", "bronze_db", "raw_bucket", "source", "date", "batch_id"])
spark = SparkSession.builder.getOrCreate()

bucket, key = split_s3_uri(args["config_path"])
config = parse_config(boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))
source = config.source(args["source"])
table = f"`{args['catalog']}`.`{args['bronze_db']}`.{source.bronze_table}"
try:
    target = spark.table(table)
except AnalysisException as error:
    raise RuntimeError(f"{table} doesn't exist: run apply_ddl first (jobs never create tables)") from error

path = f"s3://{args['raw_bucket']}/{raw_prefix(source, date.fromisoformat(args['date']))}"
raw = spark.read.schema(source_schema(target.schema)).option("multiLine", "true").option("mode", "FAILFAST").json(path)
loaded = add_ingestion_metadata(raw, args["batch_id"], F.col("_metadata.file_path"))

raise_if_schema_mismatch(loaded.dtypes, target.dtypes, table)
loaded = loaded.select(*target.columns).cache()
rows = loaded.count()
loaded.writeTo(table).append()

snapshot = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first().snapshot_id
print(json.dumps({"table": table, "path": path, "batch_id": args["batch_id"], "rows": rows, "snapshot_id": snapshot}))
