locals {
  # Glue 5.1: Spark 3.5.6, Python 3.11, Iceberg 1.10.0, the versions the local setup pins.
  # --datalake-formats adds the Iceberg jars; --conf chains the catalog settings (Glue takes one --conf key, so the
  # rest ride inside its value). glue_catalog is the Spark catalog name the jobs and migrations use.
  catalog = "glue_catalog"
  iceberg_conf = join(" --conf ", [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.${local.catalog}=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.${local.catalog}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.${local.catalog}.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.${local.catalog}.warehouse=s3://${aws_s3_bucket.this["lake"].bucket}/",
    "spark.sql.session.timeZone=UTC",
  ])
  database_arguments = {
    "--catalog"   = local.catalog
    "--bronze_db" = aws_glue_catalog_database.this["00_bronze"].name
    "--silver_db" = aws_glue_catalog_database.this["01_silver"].name
    "--gold_db"   = aws_glue_catalog_database.this["02_gold"].name
    "--ops_db"    = aws_glue_catalog_database.this["ops"].name
  }
  glue_jobs = {
    apply_ddl = {
      description = "Apply pending DDL migrations (src/<NN_layer>/<entity>/ddl_*.sql) and record them in ops.schema_migrations"
      arguments = merge(local.database_arguments, {
        "--ddl_prefix" = "s3://${local.artifacts}/ddl/"
        "--dry_run"    = "false"
      })
    }
    load_raw_files = {
      description = "Load one day of raw files for one source (--source, --date) into its bronze table"
      arguments = {
        "--catalog"     = local.catalog
        "--bronze_db"   = aws_glue_catalog_database.this["00_bronze"].name
        "--raw_bucket"  = aws_s3_bucket.this["raw"].bucket
        "--config_path" = "s3://${local.artifacts}/${aws_s3_object.sources_config.key}"
        # Filled in per run by Step Functions; these defaults make a manual run fail loudly instead of guessing.
        "--source"   = "open_meteo_hourly"
        "--date"     = "set-per-run"
        "--batch_id" = "manual"
      }
    }
    clean_readings = {
      description = "Flatten, validate and MERGE one bronze batch (--batch_id) into 01_silver.readings, rejects to readings_quarantine"
      arguments = {
        "--catalog"   = local.catalog
        "--bronze_db" = aws_glue_catalog_database.this["00_bronze"].name
        "--silver_db" = aws_glue_catalog_database.this["01_silver"].name
        "--batch_id"  = "set-per-run"
      }
    }
    build_reading_metrics = {
      description = "Rebuild 02_gold.agg_readings_daily: compute, run the data quality checks, write only if they pass"
      # One at a time: two full rebuilds racing would only repeat each other. A second start fails with
      # ConcurrentRunsExceededException, which gold_pipeline retries.
      max_concurrent_runs = 1
      arguments = {
        "--catalog"   = local.catalog
        "--silver_db" = aws_glue_catalog_database.this["01_silver"].name
        "--gold_db"   = aws_glue_catalog_database.this["02_gold"].name
      }
    }
    maintain_tables = {
      description = "Compact, expire snapshots and remove orphan files on the Iceberg tables; prints before/after per table"
      # One at a time: two maintenance runs on one table would only conflict with each other.
      max_concurrent_runs = 1
      arguments = {
        "--catalog"                = local.catalog
        "--databases"              = join(",", [for db in ["00_bronze", "01_silver", "02_gold", "ops"] : aws_glue_catalog_database.this[db].name])
        "--tables"                 = "all"
        "--expire_older_than_days" = "7"
        "--retain_last"            = "5"
        "--orphan_older_than_days" = "3"
        "--rewrite_all"            = "false"
        "--dry_run"                = "false"
      }
    }
    clean_populations = {
      description = "Validate and MERGE one bronze batch (--batch_id) of Wikidata population statements into 01_silver.populations"
      arguments = {
        "--catalog"   = local.catalog
        "--bronze_db" = aws_glue_catalog_database.this["00_bronze"].name
        "--silver_db" = aws_glue_catalog_database.this["01_silver"].name
        "--batch_id"  = "set-per-run"
      }
    }
  }
}

resource "aws_glue_job" "this" {
  for_each          = local.glue_jobs
  name              = each.key
  description       = each.value.description
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 2  # the minimum: the data is a few KB per day
  timeout           = 15 # minutes
  max_retries       = 0  # Step Functions owns retries

  command {
    name            = "glueetl"
    script_location = "s3://${local.artifacts}/${aws_s3_object.glue_script[each.key].key}"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = lookup(each.value, "max_concurrent_runs", 3)
  }

  default_arguments = merge(each.value.arguments, {
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = local.iceberg_conf
    "--extra-py-files"                   = "s3://${local.artifacts}/${aws_s3_object.medallion.key}"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
    # Changes when the code does, so a new package or script version is a visible job update in the plan.
    "--code-version" = substr(sha1(join("", [data.archive_file.medallion.output_md5, aws_s3_object.glue_script[each.key].etag])), 0, 12)
  })
}

# Weekly maintenance, DISABLED until switched on: a Scheduler universal target calls glue:StartJobRun directly, no
# state machine needed for a single job.
resource "aws_scheduler_schedule" "maintain_tables" {
  name                         = "maintain_tables"
  schedule_expression          = "cron(0 8 ? * SUN *)" # Sundays, 08:00 UTC
  schedule_expression_timezone = "UTC"
  state                        = "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:glue:startJobRun"
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ JobName = aws_glue_job.this["maintain_tables"].name })
  }
}
