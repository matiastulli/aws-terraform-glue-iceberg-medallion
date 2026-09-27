"""Glue job: apply pending DDL migrations. Tables are created and changed only here, never by the jobs that write to them.

Reads the write-once SQL files in src/<NN_layer>/ddl/<table>/ (deployed to S3 by Terraform), applies the ones that
haven't run yet, and records each in ops.schema_migrations. What runs, and in which order, is decided and unit-tested
in src/medallion/migrations.py; this job only executes and records.

- Reruns do nothing: applied versions are skipped.
- Applied migrations are write-once: if one was edited or deleted since it ran, the job fails before executing anything.
- A migration is identified by its content checksum, so a moved file (a renamed table) only refreshes its history row.
- --dry_run true lists what would be applied without executing it.

Spark, not Athena: Athena's Iceberg DDL can't set write.merge.mode / format-version, sort order or partition evolution,
and tables it creates break Iceberg 1.10 Spark writes (docs/PLAN.md "Conventions").
"""

import json
import sys

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from medallion.config import split_s3_uri
from medallion.migrations import HISTORY_TABLE, PLACEHOLDERS, moved_migrations, parse_migrations, pending_migrations, render, split_statements

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ddl_prefix", "dry_run", *PLACEHOLDERS])
values = {name: args[name] for name in PLACEHOLDERS}
dry_run = args["dry_run"] == "true"
spark = SparkSession.builder.getOrCreate()
history_table = f"`{values['catalog']}`.`{values['ops_db']}`.{HISTORY_TABLE}"

# The migration files, keyed by their path relative to src/ (s3://<artifacts>/ddl/00_bronze/ddl/<table>/v001_create.sql).
bucket, prefix = split_s3_uri(args["ddl_prefix"])
s3 = boto3.client("s3")
keys = [obj["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix) for obj in page.get("Contents", [])]
files = {key[len(prefix):]: s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8") for key in keys if key.endswith(".sql")}
migrations = parse_migrations(files)

# The history table is the runner's own bookkeeping, so the runner creates it (like Flyway's history table): it has to
# exist before any migration can be recorded. The ops database itself is Terraform's.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {history_table} (
  migration  STRING    NOT NULL COMMENT '<layer>_<table>, what the migration versions',
  version    INT       NOT NULL COMMENT 'Version within that table, v<NNN>',
  file       STRING    NOT NULL COMMENT 'Migration file, relative to src/',
  checksum   STRING    NOT NULL COMMENT 'SHA-256 of the file as applied: the migration identity',
  applied_at TIMESTAMP NOT NULL
)
USING iceberg
COMMENT 'DDL migrations applied by the apply_ddl Glue job'
TBLPROPERTIES ('format-version' = '2')
""")

applied = {row.checksum: (row.migration, row.version, row.file) for row in spark.table(history_table).collect()}
pending = pending_migrations(migrations, applied)
moved = moved_migrations(migrations, applied)
print(f"{len(migrations)} migrations, {len(applied)} already applied, {len(pending)} pending, {len(moved)} moved" + (" (dry run)" if dry_run else ""))

for migration in [] if dry_run else moved:
    print(f"   moved: {applied[migration.checksum][2]} -> {migration.path}")
    spark.sql(
        f"UPDATE {history_table} SET migration = '{migration.key}', version = {migration.version}, file = '{migration.path}' "
        f"WHERE checksum = '{migration.checksum}'"
    )

for migration in pending:
    statements = split_statements(render(migration.sql, values))
    print(f"{migration.path}: {len(statements)} statement(s)")
    if dry_run:
        continue
    for statement in statements:
        spark.sql(statement)
    # Recorded only after every statement in the file succeeded.
    (
        spark.createDataFrame([(migration.key, migration.version, migration.path, migration.checksum)], "migration string, version int, file string, checksum string")
        .withColumn("applied_at", F.current_timestamp())
        .writeTo(history_table)
        .append()
    )

print(json.dumps({
    "dry_run": dry_run,
    "already_applied": len(applied),
    "moved": [m.path for m in moved],
    ("pending" if dry_run else "applied_now"): [m.path for m in pending],
}))
