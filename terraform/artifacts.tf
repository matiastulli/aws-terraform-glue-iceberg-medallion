# Code deployed to S3 for the Glue jobs: scripts, the shared medallion package, DDL migrations and config.
# etag = content hash, so a changed file is re-uploaded on the next apply and nothing else is.
locals {
  src          = "${path.module}/../src"
  artifacts    = aws_s3_bucket.this["artifacts"].bucket
  medallion_py = fileset("${local.src}/medallion", "*.py")
  ddl_files    = fileset(local.src, "*/*/ddl_*.sql") # medallion/migrations.py GLOB
  glue_scripts = {
    apply_ddl             = "ops/schema_migrations/glue_job_apply_ddl.py"
    load_raw_files        = "00_bronze/_ingestion/glue_job_load_raw_files.py"
    clean_readings        = "01_silver/readings/glue_job_clean_readings.py"
    clean_populations     = "01_silver/populations/glue_job_clean_populations.py"
    build_reading_metrics = "02_gold/agg_readings_daily/glue_job_build_reading_metrics.py"
    maintain_tables       = "ops/_maintenance/glue_job_maintain_tables.py"
  }
}

# Glue imports --extra-py-files zips from their root, so the package goes in as medallion/*.py.
data "archive_file" "medallion" {
  type        = "zip"
  output_path = "${path.module}/.build/medallion.zip"

  dynamic "source" {
    for_each = local.medallion_py
    content {
      content  = file("${local.src}/medallion/${source.value}")
      filename = "medallion/${source.value}"
    }
  }
}

resource "aws_s3_object" "medallion" {
  bucket = local.artifacts
  key    = "packages/medallion.zip"
  source = data.archive_file.medallion.output_path
  etag   = data.archive_file.medallion.output_md5
}

resource "aws_s3_object" "glue_script" {
  for_each = local.glue_scripts
  bucket   = local.artifacts
  key      = "jobs/${each.value}"
  source   = "${local.src}/${each.value}"
  etag     = filemd5("${local.src}/${each.value}")
}

# ddl/<path relative to src/>, the path apply_ddl records as the migration's file.
resource "aws_s3_object" "ddl" {
  for_each = local.ddl_files
  bucket   = local.artifacts
  key      = "ddl/${each.value}"
  source   = "${local.src}/${each.value}"
  etag     = filemd5("${local.src}/${each.value}")
}

resource "aws_s3_object" "sources_config" {
  bucket = local.artifacts
  key    = "config/sources.toml"
  source = "${path.module}/../config/sources.toml"
  etag   = filemd5("${path.module}/../config/sources.toml")
}
