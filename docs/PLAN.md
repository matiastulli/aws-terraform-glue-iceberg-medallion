# Plan

How this project is built, one step at a time. Each step lands in its own commits. Later steps are only planned here: their code is written when the step starts, so the details below may change as earlier steps teach us something.

**Status:** steps 0–1 done, step 2 next

| Step | Status | What it delivers |
|---|---|---|
| 0. Local environment | ✅ Done | PySpark + Apache Iceberg on the laptop, at Glue 5.1's versions |
| 1. AWS access + foundations | ✅ Done | IAM permissions, a budget alarm, and Terraform for S3, Glue databases and an Athena workgroup |
| 2. Thin end-to-end | ⏳ Next | Lambda ingestion → S3 → Glue job → bronze Iceberg, run by Step Functions, with CI on GitHub Actions |
| 3. Silver | | Typed, deduplicated hourly readings through an idempotent Iceberg `MERGE`, plus a quarantine table |
| 4. Gold + data quality | | Daily aggregates with window functions, checks that fail the state machine, and SNS alerts |
| 5. Backfill | | A Step Functions `Map` state over a date range, safe to rerun |
| 6. Near real-time | | Simulated live sensors → SQS → Lambda (pyiceberg + pyarrow) → Iceberg, with late and duplicate events (Kinesis isn't available on the Free plan) |
| 7. Iceberg operations | | Schema evolution, partition evolution, time travel and rollback, compaction and snapshot expiry, measured with Athena bytes scanned |
| 8. Governance | | Lake Formation permissions, LF-tags, a column/row filter, and CloudTrail audit |
| 9. Spark Structured Streaming | | A Glue job with `trigger(availableNow)` and an S3 checkpoint over landed files, the Auto Loader equivalent, plus a DynamoDB watermark |
| 10. Complete tests | | The remaining transformations and edge cases |

**Timebox:** the interview is on 2026-09-29, two days after step 0. Steps 1–4 are day 1 and steps 5–8 are day 2. Anything that doesn't fit stays planned here.

## Constraints that shape every step

- **The AWS stack from the target job:** Glue (PySpark), Athena, Lambda, S3, Step Functions, Kinesis, Lake Formation, DynamoDB, Iceberg, Terraform. The sibling repos cover the same medallion on [Databricks + Delta](https://github.com/matiastulli/databricks-pyspark-delta-medallion) and with [dbt + Terraform + Postgres](https://github.com/matiastulli/dbt-terraform-postgres-medallion).
- **AWS Free plan** (checked with `aws freetier get-account-plan-state`): $100 of credits until 2027-02-28. When they run out the account is paused rather than billed. Small data, short jobs, and nothing billed by the hour left running.
- **Services the Free plan blocks** (`SubscriptionRequiredException`, checked 2026-09-27): Kinesis Data Streams, Firehose, MSK (Kafka) and EMR Serverless. Streaming uses SQS + Lambda instead, and Kinesis is covered in the study notes.
- **Public repo:** no account IDs, access keys or ARNs with account IDs in committed files.
- **CD from the laptop:** GitHub Actions only runs tests and `terraform validate`. Deploys are `terraform apply` from the laptop.
- **Simple data, not business rules:** hourly weather readings.

## The data

Weather-station telemetry for a handful of cities:

- **Batch and backfill:** the [Open-Meteo historical API](https://open-meteo.com/en/docs/historical-weather-api) returns real hourly readings (temperature, humidity, precipitation, wind), is free, and needs no API key.
- **Streaming (step 6):** a Lambda simulator emits "live" readings for the same stations, with duplicates, late events and out-of-order events on purpose.
- The natural key is `(station_id, observed_at)`, so dedup and upserts have a real key, unlike the taxi trips in the Databricks repo.

## How Databricks concepts map to AWS

| Databricks repo | This repo |
|---|---|
| Delta Lake | Apache Iceberg, registered in the Glue Data Catalog |
| Unity Catalog (catalog → schema → table) | Glue Data Catalog (database → table) + Lake Formation permissions |
| Schemas `00_bronze`, `01_silver`, `02_gold`, `ops` | Glue databases with the same names, owned by Terraform |
| `apply_ddl` workflow (notebook, `spark.sql`) | `apply_ddl` Glue Spark job |
| Serverless notebooks | Glue Spark jobs |
| SQL warehouse | Athena |
| Asset Bundle jobs, schedules, table triggers | Step Functions state machines + EventBridge schedules |
| Auto Loader + checkpoint | Glue Spark Structured Streaming with `availableNow` + S3 checkpoint (step 9) |
| (streaming source) | SQS → Lambda here; Kinesis / MSK in production |
| Liquid clustering | Iceberg hidden partitioning + sort order |
| Deletion vectors (merge-on-read) | Iceberg `write.*.mode` = copy-on-write (default) or merge-on-read |
| `OPTIMIZE` / `VACUUM` | `rewrite_data_files` / `expire_snapshots` + `remove_orphan_files` (Athena: `OPTIMIZE` / `VACUUM`) |
| `ops.processed_versions` | DynamoDB watermark table |

## Conventions

Adopted on 2026-09-27, before any table exists, from the sibling repo's [CLAUDE.md](https://github.com/matiastulli/databricks-pyspark-delta-medallion/blob/main/CLAUDE.md) and its PLAN steps 8 (tables as versioned migrations, naming) and 12 (one folder per table, checksum identity). CLAUDE.md has the short rules; this section has the reasoning and what AWS changed.

**Kept as they are:**
- **Naming.** Numbered layer databases (`00_bronze`, `01_silver`, `02_gold`) so they sort in pipeline order, `ops` for bookkeeping, tables without the layer in the name (bronze `<source_system>_<source_table>`, silver plural entity + `_quarantine`, gold `fct_` / `dim_` / `agg_<subject>_<grain>`, temporary `_tmp_<process>_<purpose>`), bronze names derived from config, the column rules (`_` metadata prefix, `<event>_at`, units in names), and processes named after what they do.
- **Tables are code.** Jobs never create or redefine tables, and every write is checked against a schema contract first. Published tables have versioned migrations, one folder per table (`src/<NN_layer>/ddl/<table>/v<NNN>_<verb>.sql`), identified by content checksum, with history in `ops.schema_migrations`. Why: the schema is a contract with its own review, history and owner, and a job that creates its own table (`CREATE TABLE IF NOT EXISTS`, schema inference) lets the code and the real table drift apart silently.
- **Code structure.** Pure logic in `src/medallion/` with tests; jobs only read, call, write, and orchestrate. One workflow per process, config-driven bronze, compute → check → write in gold, idempotent writes, append-only bronze, and every input row in exactly one of silver / quarantine.

**Where AWS forces a different choice (each verified on the account on 2026-09-27):**

1. **A leading digit works everywhere, but Athena DML needs double quotes.** Checked with a throwaway database `99_zz_probe`:

   | Engine | Unquoted `99_zz_probe.t` | Quoted |
   |---|---|---|
   | Glue API (`CreateDatabase`) | ✅ | n/a |
   | Athena DDL (Hive parser): `CREATE DATABASE`, `CREATE TABLE … TBLPROPERTIES ('table_type'='ICEBERG')`, `ALTER TABLE ADD COLUMNS` | ✅ | ✅ backticks |
   | Athena DML (Trino parser): `INSERT`, `MERGE`, `SELECT`, `"t$snapshots"` | ❌ `MALFORMED_QUERY` | ✅ double quotes |
   | Spark 3.5.6 + Iceberg 1.10.0 `GlueCatalog` (local, against the real catalog): `SELECT`, `CREATE TABLE … PARTITIONED BY (days(…))`, `INSERT`, `MERGE`, `.snapshots`, `SHOW TABLES` | ✅ | ✅ backticks |

   So the numbered names stay. The rule is to always quote them: backticks in Spark and Athena DDL, double quotes in Athena DML. The PLAN used to say Glue database names can't start with a digit; that was never tested, and it's wrong.

2. **Databases are Terraform's, not a migration's.** The sibling repo moved its schemas into a migration because the bundle's `schema` resource was renamed in dev mode and dropped its data on `bundle destroy`. Terraform has no dev-mode renaming, and it's the project's infrastructure tool, so the databases stay in [`terraform/glue.tf`](../terraform/glue.tf). The destroy risk is real on AWS too: **deleting a Glue database also deletes every table in it** (the probe database went with its 3 tables). The Iceberg files survive in S3, but they're no longer tables until they're re-registered from their metadata files. So the databases have `prevent_destroy = true`, and a teardown removes it first as a reviewed change. Migrations own only tables.

3. **`apply_ddl` is a Glue Spark job, not an Athena runner** (the user's choice, from these results). Athena DDL is free and fast, but on Iceberg it's too limited for "partitioning, sort order and table properties live in migrations":
   - `CREATE TABLE` or `ALTER TABLE SET TBLPROPERTIES` with `write.merge.mode` or `format-version` → `Unsupported table property key`
   - `ALTER TABLE … WRITE ORDERED BY` and `ALTER TABLE … ADD PARTITION FIELD` → `MALFORMED_QUERY` (no sort order, no partition evolution)
   - **Tables created by Athena break Spark writes.** Athena sets `write.object-storage.enabled=true` and `write.object-storage.path`. The second is deprecated, and Iceberg 1.10 fails every Spark write with `IllegalArgumentException: Property 'write.object-storage.path' has been deprecated … use 'write.data.path'`, while Spark reads keep working. `ALTER TABLE … UNSET TBLPROPERTIES ('write.object-storage.path')` from Spark fixed it, and Athena can't unset it itself (`Unsupported table property key`).

   With Spark, the DDL runs in the same engine and Iceberg version as the writers, with the whole Iceberg DDL (properties, `WRITE ORDERED BY`, `ADD PARTITION FIELD`, `CALL` procedures). A run costs about a cent (Glue 5.x bills per second with a 1-minute minimum). Athena stays the query engine.

4. **Migration placeholders** cover what varies per deployment: database names and the lake bucket.

**Learned:**
- Athena has two parsers. DDL goes through Hive (backticks, accepts `00_bronze` unquoted), and DML goes through Trino (double quotes, rejects it). That's why the same name works in `CREATE TABLE` and fails in `SELECT`.
- Athena's Iceberg DDL accepts only a short list of table properties. The layout knobs that matter for merges (`write.merge.mode`, `write.delete.mode`, `write.update.mode`) can only be set from Spark (or another full Iceberg client).

---

## 0. Local environment ✅

**Goal:** run PySpark with Iceberg on the laptop, at the versions Glue runs.

- AWS Glue 5.1 is the default Glue version: Spark 3.5.6, Python 3.11, Java 17, Iceberg 1.10.0. Glue 6.0 exists (Spark 4.1.1, Python 3.13, Iceberg 1.11.0), but 5.x is what most teams run, so we match 5.1.
- `requirements.txt` pins `pyspark==3.5.6`. The Iceberg runtime is a JVM jar (`iceberg-spark-runtime-3.5_2.12:1.10.0`), which the session downloads from Maven.
- [`scripts/local_iceberg_smoke.py`](../scripts/local_iceberg_smoke.py) uses a Hadoop catalog in `./local-warehouse` to check hidden partitioning (`days(observed_at)`), `MERGE INTO` and time travel (`VERSION AS OF <snapshot_id>`).

**Learned:**
- `MERGE INTO` needs `IcebergSparkSessionExtensions`. Without it, Spark has no `MERGE` for Iceberg tables.
- **Iceberg's default for `MERGE` is copy-on-write.** Correcting one reading wrote an `overwrite` snapshot with 4 added records: the whole day-1 file (3 rows) was rewritten, plus the new day-2 row. Delta on Databricks would have written a deletion vector instead. Iceberg has the same option (`write.merge.mode = merge-on-read`, which writes delete files), and it's a per-table choice between faster writes and faster reads.
- Metadata tables (`<table>.snapshots`, `.partitions`, `.files`, `.history`) are how you inspect an Iceberg table. They're the equivalent of `DESCRIBE HISTORY` / `DESCRIBE DETAIL`.

## 1. AWS access + foundations ✅

**Goal:** a CLI that can create resources, a cost guardrail, and the empty lakehouse, all in Terraform.

- **Permissions:** `AdministratorAccess` is attached to the CLI's IAM user (done in the console, 2026-09-27). A later improvement is short-lived credentials (`aws login` or IAM Identity Center) instead of access keys.
- [`terraform/bootstrap/`](../terraform/bootstrap/) (local state, applied once): the state bucket and a **$20/month budget** ($100 of credits over ~5 months), with emails at 50% and 100% of actual spend and 100% of forecast. The email is in a gitignored `terraform.tfvars`.
- [`terraform/`](../terraform/) (state in S3 with native locking, `use_lockfile`): buckets `raw`, `lake` and `athena-results` (private, SSE-S3, a random suffix instead of the account ID), Glue databases `00_bronze`, `01_silver`, `02_gold` and `ops` pointing at `s3://<lake>/<database>/`, with `prevent_destroy` (see Conventions), and the Athena workgroup `weather-lakehouse`.
- The backend is a partial config: `backend.hcl` (gitignored) holds the bucket name and is generated from the bootstrap output.

**Learned:**
- **Budgets on the Free plan must exclude credits.** Budgets counts cost net of credits by default, and credits cover everything until they run out, so the tracked cost would stay at $0 and never alert. `include_credit = false` tracks gross spend, which is what burns the credits.
- **S3 native state locking** (Terraform ≥ 1.10, `use_lockfile = true`) writes a `.tflock` object next to the state, so a DynamoDB lock table is no longer needed.
- **S3 tag values reject commas** (`InvalidTag`). The allowed characters are letters, digits, spaces and `+ - = . _ : / @`. The failure was partial: the bucket without a comma was created, the others weren't, and a re-plan picked up only what was missing.
- **Lake versioning + Iceberg:** Iceberg already keeps history in snapshots, so when `expire_snapshots` deletes a file, the S3 noncurrent version is only a safety net. A lifecycle rule expires noncurrent versions after 7 days, so they don't silently keep costing storage.
- **The Athena workgroup enforces its settings** (`enforce_workgroup_configuration`): the result location, SSE-S3, and a 1 GiB bytes-scanned cutoff per query win over whatever the client sends. That's the per-query cost guardrail, since Athena bills per byte scanned.
- A leading digit in a database name works in Glue, Athena and Spark, but Athena DML needs it double-quoted (see Conventions). The databases were first created as `bronze`/`silver`/`gold` on an untested claim that digits aren't allowed, then replaced while they were still empty.

## 2. Thin end-to-end

**Goal:** one real run through every piece of the delivery path before any feature work.

- `config/stations.toml`: the source (`open_meteo` + `hourly`, from which the bronze table name `open_meteo_hourly` is derived) and a few stations (`station_id`, name, latitude, longitude)
- `src/medallion/`: the first pure functions (config parsing and name derivation, ingestion metadata, the schema contract) with tests on local PySpark
- **Tables are code from the first table:** the migration runner logic in `src/medallion/migrations.py` (tested: run order, pending only, checksum drift, misnamed or misplaced files), the `apply_ddl` Glue Spark job in `src/ops/` with `dry_run`, and `src/00_bronze/ddl/open_meteo_hourly/v001_create.sql`. The runner creates `ops.schema_migrations` itself.
- Lambda `ingest_weather`: calls Open-Meteo for a date and writes raw JSON to `s3://<raw>/open_meteo/date=YYYY-MM-DD/<station_id>.json`
- Glue job `load_open_meteo` (`src/00_bronze/`): reads the raw JSON, adds `_batch_id`, `_ingested_at`, `_source_file`, checks the table exists and matches its contract, and appends to `"00_bronze".open_meteo_hourly`
- Step Functions `weather_pipeline`: ingest → load, with retries and a catch. Deploy order: `terraform apply` → `apply_ddl` → pipeline.
- Verify what Iceberg itself enforces on a `_tmp_` table (extra column, missing nullable column, castable type, null into a required column) to decide what the contract check must catch. Delta accepted a missing nullable column in the sibling repo.
- GitHub Actions: pytest + `terraform fmt -check` + `terraform validate`

## 3. Silver

- Flatten Open-Meteo's column arrays into one row per station-hour, type them, and validate them (nulls, physical ranges)
- Glue job `clean_readings` (`src/01_silver/`): dedup within the batch, then `MERGE` into `"01_silver".readings` on `(station_id, observed_at)`, with `_merged_at`. Partitioning (`days(observed_at)`) and `write.merge.mode` are set in its `v001_create` migration. A rerun inserts 0 rows.
- Invalid rows go to `"01_silver".readings_quarantine` with `rejection_reasons`. Rules handle nulls explicitly, and the job asserts that every distinct input row landed in exactly one of the two tables.

## 4. Gold + data quality

- Glue job `build_reading_metrics` (`src/02_gold/`): `"02_gold".agg_readings_daily` (min/max/avg per station-day) and window functions (rolling 24h average, `LAG` gap detection)
- Checks in order compute → check → write, so a failing check fails the Glue job, the state machine catches it, and SNS sends an email

## 5. Backfill

- The state machine takes `start_date` / `end_date`. A `Map` state fans out per date with a small `MaxConcurrency`. Idempotency comes from silver's `MERGE`.

## 6. Near real-time

Kinesis, Firehose and MSK are blocked on the Free plan, so the stream is SQS:

- Simulator Lambda (on an EventBridge schedule) → SQS queue → consumer Lambda (event source mapping, batch size + batching window) → append to a bronze table named from config like any source (e.g. `simulator_readings`), created by a migration, with **pyiceberg + pyarrow** against the Glue catalog, no Spark
- `ReportBatchItemFailures` for partial batch failures, a dead-letter queue after N receives, and an alarm on DLQ depth
- SQS standard queues are at-least-once and unordered, so duplicates and out-of-order readings reach bronze. Silver's `MERGE` on `(station_id, observed_at)` already absorbs them. That's the same contract as Kinesis, and the notes compare the two (shards, partition keys, ordering, replay, iterator age).
- The consumer checks the table's schema contract before appending, like the Spark jobs (pyiceberg must also handle the quoted `00_bronze`: verify).
- Lambda micro-batches make small files in Iceberg, which is the reason for the compaction in step 7.

## 7. Iceberg operations

- Schema evolution (add, rename and widen columns: Iceberg tracks columns by ID) and partition evolution (`days` → `hours` without rewriting old data), both as `v00N_alter` migrations applied by `apply_ddl`, never ad hoc
- Time travel and rollback (`rollback_to_snapshot`), `rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`
- Athena bytes scanned before and after, as the measurement

## 8. Governance

- Lake Formation: register the lake location, an `analyst` role that can read gold only, an LF-tag, a column filter, and CloudTrail for the audit trail

## 9. Spark Structured Streaming

- A Glue job reading landed files with `readStream`, `trigger(availableNow=True)` and a checkpoint in S3, writing to Iceberg: Glue's equivalent of Auto Loader, billed as a batch job
- A DynamoDB table as the pipeline's watermark/control table (bookkeeping, so named under `ops`). The watermark advances only after the checks pass. The checkpoint is never deleted to "clean up".

## 10. Complete tests
