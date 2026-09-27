# Glue Data Catalog databases. Medallion layers are numbered so they sort in pipeline order; `ops` holds
# pipeline bookkeeping (e.g. ops.schema_migrations). Terraform owns the databases, never the tables:
# tables are created and changed by versioned DDL migrations (apply_ddl).
#
# A leading digit is fine in Glue, Athena DDL and Spark, but Athena DML (Trino) needs it double-quoted:
# SELECT ... FROM "00_bronze".open_meteo_hourly (verified 2026-09-27, see docs/PLAN.md "Conventions").
locals {
  databases = {
    "00_bronze" = "Bronze: source data as received, append-only"
    "01_silver" = "Silver: typed, validated and deduplicated entities, plus their quarantine tables"
    "02_gold"   = "Gold: facts, dimensions and aggregates for consumers"
    "ops"       = "Pipeline bookkeeping: migration history and watermarks"
  }
}

resource "aws_glue_catalog_database" "this" {
  for_each     = local.databases
  name         = each.key
  description  = each.value
  location_uri = "s3://${aws_s3_bucket.this["lake"].bucket}/${each.key}/"

  # Deleting a Glue database also deletes every table registered in it (the Iceberg files stay in S3 but
  # are no longer tables). Tearing down means removing this line first, as a reviewed change.
  lifecycle {
    prevent_destroy = true
  }
}
