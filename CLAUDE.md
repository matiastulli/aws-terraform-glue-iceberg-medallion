# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

A **learning-path portfolio project**: a medallion lakehouse on **AWS** (S3, Glue, Athena, Lambda, Step Functions, Kinesis, Lake Formation, DynamoDB) with **Apache Iceberg** tables and **Terraform**. It's the AWS counterpart of `~/Code/databricks-pyspark-delta-medallion` (Databricks + Delta), built to prepare for an AWS data engineering interview on 2026-09-29.

**Build it one step at a time.** Never scaffold later steps ahead of time; each step adds only what it needs, with explanations. The user prefers easy-going example data (hourly weather readings) over business rules.

`docs/PLAN.md` is the only place that tracks the learning path: update its status, decisions and learnings as steps land, and put plan changes for later steps there before writing the code. The README describes what exists, not the plan.

## Environment

- AWS CLI v2, region `us-east-2`, `default` profile. **Never ask for, print or handle access keys in chat.** Keep the account ID and ARNs containing it out of committed files; redact them in command output.
- **AWS Free plan**: $100 of credits until 2027-02-28 (`aws freetier get-account-plan-state`). Nothing billed by the hour left running (Glue streaming or interactive sessions); keep Glue jobs small.
- **Blocked on the Free plan** (`SubscriptionRequiredException`): Kinesis, Firehose, MSK and EMR Serverless. Streaming uses SQS + Lambda. Available and verified: S3, Glue (jobs, Data Catalog, Data Quality), Athena, Lambda, Step Functions, DynamoDB (+ Streams), SQS, SNS, EventBridge (+ Pipes), Lake Formation, CloudTrail, CloudWatch, S3 Tables.
- The DynamoDB table `trucks` predates this repo. Leave it alone.
- Local: Python 3.11 and Java 17 (Homebrew `openjdk@17`). Versions match **AWS Glue 5.1** (Spark 3.5.6, Iceberg 1.10.0).

```sh
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
.venv/bin/python scripts/local_iceberg_smoke.py      # Iceberg MERGE, hidden partitioning, time travel in ./local-warehouse
```

A local Iceberg session needs `spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0`, `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` (without it there is no `MERGE INTO`), and a catalog (`spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog`, `type=hadoop`, a warehouse path).

Infrastructure (Terraform ≥ 1.10). `bootstrap/` is applied once with local state; the main stack keeps its state in S3:

```sh
terraform -chdir=terraform/bootstrap init && terraform -chdir=terraform/bootstrap apply   # state bucket + budget; needs terraform.tfvars (alert_email)
terraform -chdir=terraform/bootstrap output -raw backend_hcl > terraform/backend.hcl
terraform -chdir=terraform init -backend-config=backend.hcl   # the main stack also needs terraform/terraform.tfvars (alert_email, for SNS)
terraform -chdir=terraform plan -out=tfplan && terraform -chdir=terraform apply tfplan   # review the plan before applying
terraform -chdir=terraform fmt -recursive && terraform -chdir=terraform validate
```

Deploy and run (CD from the laptop; deploy order is `terraform apply` → `apply_ddl` → pipeline):

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17); .venv/bin/pytest          # pure logic on local PySpark (pyproject sets pythonpath=src)
aws glue start-job-run --job-name apply_ddl --arguments '{"--dry_run":"true"}'   # list pending migrations; without the argument, apply them
aws stepfunctions start-execution --state-machine-arn <source_pipeline ARN> --name <source>-<date>-a --input '{"source":"open_meteo_hourly","date":"2026-09-20"}'   # one source per execution; date optional (default: today - lag_days); the name becomes _batch_id
aws stepfunctions start-execution --state-machine-arn <backfill ARN> --name <source>-<start>-<end>-a --input '{"source":"open_meteo_hourly","start_date":"2026-09-15","end_date":"2026-09-19"}'   # one source_pipeline per date (2 at a time, ≤ 31 days), then gold once
aws stepfunctions start-execution --state-machine-arn <gold_pipeline ARN> --input '{"triggered_by":"manual"}'   # also starts by itself when a source_pipeline execution not named backfill-* succeeds (EventBridge rule)
aws scheduler update-schedule ...                                           # schedules source_pipeline-<source> are deployed DISABLED; enable one on purpose
aws glue start-job-run --job-name clean_readings --arguments '{"--batch_id":"<execution name>"}'   # re-clean one bronze batch; a rerun must report new: 0, changed: 0
aws glue get-job-run --job-name load_raw_files --run-id <id>                 # job stdout is in CloudWatch /aws-glue/jobs/output/<run id>
```

Layout: `config/sources.toml` (sources + stations; names, silver job and lag are derived from it), `src/medallion/` (pure logic, tested), `src/<NN_layer>/<entity>/` holding the entity's migrations `ddl_<table>_v<NNN>_<verb>.sql` and the job that writes it (`glue_job_<process>.py`, `lambda_<process>.py`), `src/00_bronze/_ingestion/` (generic processes that serve every source), `src/ops/schema_migrations/glue_job_apply_ddl.py`, `terraform/` (uploads code to the `artifacts` bucket; Glue jobs use Spark catalog `glue_catalog`), `terraform/state_machines/*.asl.json`.

Queries go through the `weather-lakehouse` Athena workgroup (enforced result bucket, 1 GiB scan cutoff). **Athena DML needs the numbered databases double-quoted**: `SELECT … FROM "00_bronze".open_meteo_hourly` (unquoted is `MALFORMED_QUERY`). Athena DDL and Spark accept them unquoted; quote them anyway (backticks in DDL and Spark).

## Conventions

Adopted from `~/Code/databricks-pyspark-delta-medallion` and translated to AWS. The reasoning and the verified AWS facts are in `docs/PLAN.md` "Conventions".

**Naming**
- Glue databases: `00_bronze`, `01_silver`, `02_gold` (numbered to sort in pipeline order) and `ops` for bookkeeping. Terraform owns them.
- Tables: `^[a-z][a-z0-9_]*$` and never the layer in the name. Bronze `<source_system>_<source_table>` (`open_meteo_hourly`), silver plural `<entity>` + `<entity>_quarantine` (`readings`, `readings_quarantine`), gold `fct_<event>` / `dim_<entity>` / `agg_<subject>_<grain>` (`agg_readings_daily`), temporary `_tmp_<process>_<purpose>` (created and dropped within one run, never in DDL).
- Names are **derived** from config (source system + table), never configured by hand.
- Columns: bronze keeps source names untouched, silver renames. `_` prefix only for pipeline metadata (`_batch_id`, `_ingested_at`, `_source_file`, `_merged_at`). `<event>_at` timestamps (UTC), `<event>_date` dates, units in names (`temperature_c`, `wind_speed_kmh`, `precipitation_mm`), keys `<entity>_id`, booleans `is_`/`has_`, money `<name>_amount` as `DECIMAL`.
- Processes are named after what they do (`ingest_source`, `clean_readings`, `build_reading_metrics`), never after the layer. **Code is grouped by entity** (the user's decision, step 3b): `src/<NN_layer>/<entity>/` holds the entity's tables' migrations and the job that writes them, with the kind of file as a prefix: `ddl_<table>_v<NNN>_<verb>.sql`, `glue_job_<process>.py`, `lambda_<process>.py`. The entity folder holds its main table and tables named `<entity>_<suffix>` (its quarantine). Generic processes that serve many tables live in `_<purpose>/` at the layer level (`00_bronze/_ingestion/`). The process name is also the deployed Lambda or Glue job name.

**Tables are code**
- Jobs never create or redefine tables. They check the table exists and the schema contract (exact column names + types), then append / overwrite / `MERGE`. No schema inference, no `mergeSchema`, no automatic schema evolution.
- Published tables get versioned migrations: `src/<NN_layer>/<entity>/ddl_<table>_v<NNN>_<create|alter|rename|drop>.sql`, with placeholders for the catalog and database names. Identity is the content checksum, so moving files is free (apply_ddl refreshes the recorded paths); never edit an applied migration, add the next version. History in `ops.schema_migrations`.
- `apply_ddl` is a **Glue Spark job** (`spark.sql`), not Athena: Athena can't set `write.merge.mode` / `format-version`, sort order or partition evolution, and tables it creates carry `write.object-storage.path`, which breaks Spark writes on Iceberg 1.10 (verified).
- Partitioning, sort order and table properties (`format-version`, `write.merge.mode`, …) are set in migrations only. Intermediates (DataFrames, temp views, CTEs, `_tmp_` tables) never get DDL.
- Terraform owns infrastructure (buckets, databases, IAM, jobs, state machines), never table schemas.

**Code structure**
- Pure logic lives in `src/medallion/` with pytest tests on local PySpark. Glue and Lambda scripts only read, call it, write, and orchestrate; Iceberg `MERGE`s and table writes stay in the scripts.
- **One pipeline execution per source.** `source_pipeline` is one generic state machine (ingest → load → the source's `silver_job`), run per source with `{"source": …}` by its own schedule: a failing source never blocks another, and each source retries and backfills on its own. Code is shared by *kind* of source (`build_request`), not copied per source. Add a source by adding config plus its migration, never a new script, job or state machine. Real-time sources (step 6) are a separate streaming path that lands in bronze too.
- Gold runs compute → data quality checks → write. If a check can only run after a write, roll back to the prior snapshot, and advance a watermark only after the checks pass.
- Everything that writes is safe to run twice: silver `MERGE`s on `(station_id, observed_at)`, and a rerun inserts 0 rows.
- Bronze is append-only. Never delete a streaming checkpoint to "clean up": that's a full reprocess.
- Every distinct input row lands in exactly one of silver / quarantine, and the job asserts it. Validation handles nulls explicitly (a null comparison is null, not false).

## Working agreements

- **One step at a time**, never scaffolding later steps. `docs/PLAN.md` is the only tracker (status, decisions, "Learned:" notes with verified facts). The README describes only what exists.
- **Get the delivery path working early** (Terraform apply, a real Step Functions run, CI) and test only the high-risk logic until the final step: the `(station_id, observed_at)` key, dedup, the silver/quarantine split, gold data quality checks, and the migration runner.
- **CI on GitHub, CD from the laptop.** GitHub Actions runs pytest + `terraform fmt -check` / `validate` only. No AWS credentials in CI.
- **Verify on the real account and record numbers** (row counts, snapshot IDs, Athena bytes scanned). Measure layout claims before and after; don't claim a fix you didn't measure. Try risky DDL against a throwaway database first (`zz_<purpose>`, dropped afterwards).
- **Terraform:** `plan -out`, review, then `apply` the saved plan. Never `-auto-approve`.
- The repo is **public**: no account IDs, access keys or ARNs with the account ID in committed files or chat output. Local settings go in a git-ignored `.env` with a committed `.env.example`, and a variable is added only when code uses it.
- **One commit per step** (or logical piece). Confirm with the user before pushing. When the project is published, update its entry in the user's GitHub profile README (`~/Code/matiastulli`, pull first).
- **Free plan:** $100 of credits; nothing billed per hour left running; small Glue jobs.
- **Interview on 2026-09-29:** go deep on Iceberg, Glue, Athena and Step Functions, and add a "Learned:" note for anything that would make a good interview answer.
- Add commands to this file as each step introduces them. Don't document commands that don't exist yet.
