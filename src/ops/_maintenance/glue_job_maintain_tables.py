"""Glue job: maintain the Iceberg tables (docs/PLAN.md step 7). Compacts small files, rewrites manifests, expires old
snapshots and removes orphan files, then prints what changed per table: data files, bytes and snapshots, before and
after. The SQL and its safety limits come from medallion/maintenance.py; this job lists tables, runs and measures.

--tables: comma-separated db.table, or all (every Iceberg table in --databases).
--expire_older_than_days / --retain_last: snapshots older than N days go, but the last --retain_last always stay.
--orphan_older_than_days: never under 1 (Iceberg refuses; a younger file may belong to a commit in progress).
--rewrite_all true: rewrite every data file, e.g. once after a partition change moves new data to a new spec.
--dry_run true: list orphan files without deleting them (compaction and expiry still run).

Safe to run while the pipelines write: a rewrite commits like any writer and retries on a conflict, and nothing
younger than the cutoffs is deleted.
"""

import datetime as dt
import json
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

from medallion.maintenance import (
    Options,
    expire_snapshots_sql,
    procedure_table,
    remove_orphan_files_sql,
    rewrite_data_files_sql,
    rewrite_manifests_sql,
)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "catalog", "databases", "tables", "expire_older_than_days", "retain_last", "orphan_older_than_days", "rewrite_all", "dry_run"],
)
catalog = args["catalog"]
options = Options(
    expire_older_than_days=int(args["expire_older_than_days"]),
    retain_last=int(args["retain_last"]),
    orphan_older_than_days=int(args["orphan_older_than_days"]),
    rewrite_all=args["rewrite_all"] == "true",
    dry_run=args["dry_run"] == "true",
)
spark = SparkSession.builder.getOrCreate()


def properties(database: str, table: str) -> dict[str, str]:
    return {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES `{catalog}`.`{database}`.{table}").collect()}


def iceberg_tables() -> list[tuple[str, str]]:
    if args["tables"] != "all":
        return [tuple(name.strip().split(".", 1)) for name in args["tables"].split(",")]
    found = []
    for database in args["databases"].split(","):
        for row in spark.sql(f"SHOW TABLES IN `{catalog}`.`{database}`").collect():
            if properties(database, row.tableName).get("format", "").startswith("iceberg"):
                found.append((database, row.tableName))
    return found


def state(database: str, table: str) -> dict:
    name = f"`{catalog}`.`{database}`.{table}"
    files = spark.sql(f"SELECT count(*) AS n, coalesce(sum(file_size_in_bytes), 0) AS bytes FROM {name}.files").first()
    snapshots = spark.sql(f"SELECT count(*) AS n FROM {name}.snapshots").first()
    return {"data_files": files.n, "bytes": files.bytes, "snapshots": snapshots.n}


def maintain(database: str, table: str) -> dict:
    target = procedure_table(database, table)
    before = state(database, table)
    rewrite = spark.sql(rewrite_data_files_sql(catalog, target, properties(database, table).get("sort-order"), options.rewrite_all)).first()
    manifests = spark.sql(rewrite_manifests_sql(catalog, target)).first()
    expired = spark.sql(expire_snapshots_sql(catalog, target, options, dt.datetime.now(dt.timezone.utc))).first()
    orphans = spark.sql(remove_orphan_files_sql(catalog, target, options, dt.datetime.now(dt.timezone.utc))).collect()
    return {
        "table": f"{database}.{table}",
        "before": before,
        "after": state(database, table),
        "rewritten_data_files": rewrite.rewritten_data_files_count,
        "added_data_files": rewrite.added_data_files_count,
        "rewritten_manifests": manifests.rewritten_manifests_count,
        "expired": {"data_files": expired.deleted_data_files_count, "manifests": expired.deleted_manifest_files_count, "manifest_lists": expired.deleted_manifest_lists_count},
        "orphan_files": len(orphans),
        "orphans_deleted": not options.dry_run,
    }


results, failures = [], []
for database, table in iceberg_tables():
    try:
        result = maintain(database, table)
        results.append(result)
        print(json.dumps(result))
    except Exception as error:  # one table's failure doesn't skip the others; the job fails at the end
        failures.append(f"{database}.{table}: {error!r}")
        print(json.dumps({"table": f"{database}.{table}", "error": repr(error)[:500]}))
print(json.dumps({"tables": len(results), "failed": len(failures), "options": options.__dict__}))
if failures:
    raise RuntimeError(f"maintenance failed for {len(failures)} table(s): {failures}")
