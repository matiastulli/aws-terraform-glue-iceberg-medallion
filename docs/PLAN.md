# Plan

How this project is built, one step at a time. Each step lands in its own commits. Later steps are only planned here: their code is written when the step starts, so the details below may change as earlier steps teach us something.

**Status:** steps 0–3b done, step 4 next

| Step | Status | What it delivers |
|---|---|---|
| 0. Local environment | ✅ Done | PySpark + Apache Iceberg on the laptop, at Glue 5.1's versions |
| 1. AWS access + foundations | ✅ Done | IAM permissions, a budget alarm, and Terraform for S3, Glue databases and an Athena workgroup |
| 2. Thin end-to-end | ✅ Done | Lambda ingestion → S3 → Glue job → bronze Iceberg, run by Step Functions, with CI on GitHub Actions |
| 3. Silver | ✅ Done | Typed, deduplicated hourly readings through an idempotent Iceberg `MERGE`, plus a quarantine table |
| 3b. Second source: population | ✅ Done | City population from Wikidata through the same stack, so gold can show how many people the weather affects |
| 4. Gold + data quality | ⏳ Next | Daily aggregates with window functions, checks that fail the state machine, and SNS alerts |
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

## 2. Thin end-to-end ✅

**Goal:** one real run through every piece of the delivery path before any feature work.

Built:
- [`config/sources.toml`](../config/sources.toml) (renamed from the planned `stations.toml`, since it holds the sources too): the source `open_meteo` + `hourly` and three stations. Everything is derived from `system` + `table`: the bronze table `open_meteo_hourly`, the raw prefix `open_meteo/hourly/date=YYYY-MM-DD/`, and the API request (for Open-Meteo, `table` is both the request parameter listing the variables and the response block holding them). So a `daily` source would be config plus a migration, with no new code.
- [`src/medallion/`](../src/medallion/): `config.py` (parsing, validation, derived names; no pyspark, because the Lambda imports it), `bronze.py` (read schema = table schema minus the added columns; `station_id` from the file name, and a misnamed file fails the load), `contract.py`, and `migrations.py` (ported from the sibling repo without its `catalog/` folder, because Terraform owns the databases here). **28 tests**, all on the high-risk logic.
- Migrations: [`src/00_bronze/ddl/open_meteo_hourly/v001_create.sql`](../src/00_bronze/open_meteo_hourly/ddl_open_meteo_hourly_v001_create.sql). Bronze mirrors the API response: one row per station-day, with the `hourly` block of parallel arrays kept as a struct (silver flattens it), plus `station_id` and `_batch_id`, `_ingested_at`, `_source_file`. It's partitioned by `days(_ingested_at)`, with `format-version` 2 and zstd.
- Glue job [`apply_ddl`](../src/ops/schema_migrations/glue_job_apply_ddl.py): reads the migrations from `s3://<artifacts>/ddl/`, and creates `ops.schema_migrations` itself.
- Lambda `ingest_weather` (since step 3b the generic [`ingest_source`](../src/00_bronze/_ingestion/lambda_ingest_source.py)): writes each API response untouched to the raw bucket. Rewriting the same key makes reruns safe.
- Glue job [`load_raw_files`](../src/00_bronze/_ingestion/glue_job_load_raw_files.py): generic over sources (`--source`, `--date`, `--batch_id`). It fails if the table is missing, reads the JSON with the table's schema (`FAILFAST`), checks the contract, and appends.
- Step Functions `weather_pipeline` (replaced by `source_pipeline` in step 3b): Ingest → `Map` over the sources the Lambda returns → `glue:startJobRun.sync`, with a catch to a `Fail` state. `_batch_id` is the execution name.
- Terraform: an `artifacts` bucket (scripts, the zipped `medallion` package, DDL, config, each uploaded with a content-hash `etag`), three IAM roles (Glue, Lambda, Step Functions), the two Glue 5.1 jobs (2 × G.1X, `max_retries = 0` because Step Functions owns retries), the Lambda (Python 3.11) with 14-day logs, and the state machine.
- **Layout by runtime** (the user's request, for readability): each layer folder has one subfolder per place the code runs, so the folder tells you where a file executes:
  ```
  src/00_bronze/lambda/ingest_weather.py        Lambda handler
  src/00_bronze/glue_job/load_raw_files.py      Glue Spark script
  src/00_bronze/ddl/open_meteo_hourly/v001_create.sql
  src/ops/glue_job/apply_ddl.py
  src/medallion/                                shared pure logic, imported by both runtimes
  ```
- [GitHub Actions](../.github/workflows/ci.yml): pytest, plus `terraform fmt -check` and `validate` with `-backend=false` (no credentials).

Verified on the account (2026-09-27):
- `apply_ddl --dry_run true`: 1 pending, nothing executed. The real run applied `v001_create`, and a rerun reported **1 already applied, 0 pending**. Glue runs took 38–53 s.
- The run `run-2026-09-20-a` (date 2026-09-20) **succeeded end to end in 2 minutes**: 3 raw files, then `load_raw_files` in 79 s, then **3 rows** in bronze (snapshot `1420641045076479683`), each with 24 hours. Athena read them back through `"00_bronze".open_meteo_hourly` and `"ops".schema_migrations`.
- Failure path: a bad date fails the Lambda with `ValueError`, and the catch ends in `WeatherPipelineFailed`.

Decisions:
- **Retries only where they're safe and useful.** The Ingest task retries only transient errors (Lambda service errors, `HTTPError`, `URLError`, `TimeoutError`). The first version retried `States.ALL`, and the bad-date test showed a deterministic `ValueError` being retried 3 times (4 attempts, ~35 s) before failing; now it fails at once (0.1 s). The Glue load retries only errors raised before the job runs (concurrency, throttling), never a failed job: bronze appends, so a retry after a commit would load the batch twice.
- **A bronze rerun appends the same files under a new `_batch_id`.** That's by design (bronze is append-only, and silver's `MERGE` on the key absorbs repeats), not something to fix with deletes.
- `station_id` has no `_` prefix: it isn't pipeline metadata, it's the entity key, taken from the file name because the API response doesn't carry it.

**Learned:**
- **What Iceberg enforces on its own** (Spark 3.5.6 + Iceberg 1.10.0, `writeTo().append()`, tested on a `_tmp_` table): it **rejects** an extra column, a **missing nullable column** (Delta accepts that one and fills in nulls), string → int, an overflowing bigint → int, and null into `NOT NULL`. It **silently accepts** double `50.7` → int stored as `50` (truncated), double `1.555` → `DECIMAL(10,2)` stored as `1.56` (rounded), and float `1.1` → double stored as `1.100000023841858`. It matches columns **by name**, so order doesn't matter. That's why `contract.py` still compares exact types before every write.
- **Glue takes one `--conf` key**, so the Iceberg catalog settings ride inside its value: `spark.sql.extensions=… --conf spark.sql.catalog.glue_catalog=… --conf …`. With `--datalake-formats iceberg`, Glue adds the jars. A plain `SparkSession.builder.getOrCreate()` works in the script; `GlueContext` is only needed for Glue features such as bookmarks and DynamicFrames.
- **The Iceberg `GlueCatalog` puts a table under its database's `LocationUri`** (`s3://<lake>/00_bronze/open_meteo_hourly`), so migrations need no bucket placeholder, only the catalog and database names. Tables created by Spark don't get Athena's `write.object-storage.*` properties.
- **For Lambda tasks, Step Functions error names are the Python exception class names** (`ValueError`, `HTTPError`), so `ErrorEquals` can tell retryable errors from bugs.
- `glue:startJobRun.sync` makes Step Functions wait for the job run and fail the state if the job fails. Glue 5.1 bills per second with a 1-minute minimum, and a 2 × G.1X job costs about $0.015 per minute.

## 3. Silver ✅

**Goal:** one typed, validated row per station-hour in `"01_silver".readings`, safe to rerun, with rejects kept and explained.

Built:
- [`src/medallion/silver.py`](../src/medallion/silver.py), all pure DataFrame functions:
  - `flatten_hourly`: bronze's parallel arrays → one row per hour (`posexplode` on `time`, `F.get` for the others), renamed to the column convention (`temperature_2m` → `temperature_c`, `relative_humidity_2m` → `relative_humidity_pct`, `precipitation` → `precipitation_mm`, `wind_speed_10m` → `wind_speed_kmh`)
  - `add_observed_at`: the source's local time string minus `utc_offset_seconds` → a UTC timestamp
  - `add_rejection_reasons`: missing values, physical ranges, unparseable times, arrays of different lengths, and **units that don't match the column name** (compared null-safely)
  - `dedup_latest`: one row per key, most recently ingested first, with deterministic tie-breaks
  - `reconcile`: input = valid + rejected, and valid = unique + duplicates, or the job fails before writing
  - `classify_changes`: new / changed / unchanged against silver
- Migrations: [`readings`](../src/01_silver/readings/ddl_readings_v001_create.sql) (`NOT NULL` columns, `days(observed_at)`, explicit copy-on-write, `WRITE ORDERED BY station_id, observed_at` as a second statement) and [`readings_quarantine`](../src/01_silver/readings/ddl_readings_quarantine_v001_create.sql) (nullable values, `rejection_reasons`, `observed_at_raw`).
- Glue job [`clean_readings`](../src/01_silver/readings/glue_job_clean_readings.py): reads the bronze rows of `--batch_id`, checks both contracts, and MERGEs.
  - **Silver:** a new key is inserted. A matched key is updated only when its values differ *and* the incoming data isn't older (`s._ingested_at >= t._ingested_at`), so a late backfill can't overwrite newer data.
  - **Quarantine:** a log per batch, merged on `(station_id, observed_at_raw, _batch_id)` with `<=>`, so null keys still match on a rerun.
- `weather_pipeline` gained a `CleanReadings` step after the loads, with `--batch_id` = the execution name.
- 38 tests (10 new for silver).

Verified:
- **Locally first**, running the real job script against a local Iceberg catalog with the real migrations and the raw files from S3, with Glue's argument parser stubbed. That's minutes cheaper per iteration than Glue. Batch 1: 72 new. A rerun: 0 new, 0 changed. Batch 2 (the same data with one null temperature): 71 unchanged and 1 quarantined (`missing_temperature_c`), and a rerun quarantined nothing more. Batch 3 (one revised value): 1 changed, and silver took the new value.
- **On AWS:**
  - `apply_ddl` applied the 2 silver migrations; `readings` ran 2 statements.
  - `clean_readings` for the two existing batches: 72 new each (59 s and 71 s).
  - `run-2026-09-22-a` ran the full pipeline end to end in 4 minutes (Lambda, load 64 s, clean 78 s): 72 new.
  - Re-cleaning `run-2026-09-20-a`: **0 new, 0 changed, 72 unchanged, and no new snapshot.** Silver now has 216 rows (3 stations × 72 hours) and 3 snapshots, one per day.
  - Quarantine is empty: the real data had nothing to reject.

**Learned:**
- **A copy-on-write `MERGE` rewrites every data file holding a *matched* key, even when the `WHEN MATCHED AND …` condition is false for all of them.** The first version sent every row to the MERGE. A rerun that changed nothing still committed an `overwrite` snapshot rewriting all 72 records (72 added, 72 deleted), and the empty quarantine MERGE committed an empty `append` snapshot. The data was idempotent; the table and the cost weren't. Now only new and changed rows reach the MERGE, and an empty MERGE is skipped, so a rerun commits nothing. The MERGE keeps its own conditions as a safety net.
- **Copy-on-write means a 1-row update rewrites the whole file** (measured locally: 1 changed reading → 72 deleted + 72 added). Merge-on-read would write a delete file and 1 row instead, and readers would merge them at query time. Silver's MERGE is insert-mostly, so copy-on-write fits; step 7 measures the alternative.
- **`MERGE … ON a = b` never matches nulls**, so a quarantine rerun would insert the same reject again. Keys that can be null use `<=>` (null-safe equality).
- **`collect()` returns timestamps as naive datetimes in the machine's local time zone**, not the Spark session's. A test comparing `datetime`s passed on UTC machines and failed in Buenos Aires (20:00 instead of 23:00 UTC). Tests compare `date_format(...)` strings computed in Spark instead.
- `F.get(array, i)` (Spark 3.4+) is 0-based and returns null past the end, even with ANSI mode on. `element_at` is 1-based and throws under ANSI.
- The Step Functions `Map` fans out the loads, and silver runs once after all of them, cleaning everything the execution loaded (one `_batch_id`).

## 3b. Second source: population (Wikidata) ✅

**Goal (the user's request):** gold can show how many people live where the weather happens. A second source system goes through the same stack (Lambda → raw → `load_raw_files` → bronze → silver), which tests whether bronze really is config-driven.

**Source, compared on 2026-09-27 (the user chose Wikidata):**

| | Open-Meteo Geocoding (GeoNames) | Wikidata SPARQL |
|---|---|---|
| Buenos Aires / Córdoba / Ushuaia | 2,891,082 / 2,106,734 / 56,825 | 2022 census: 3,121,707 / 1,505,250 / 82,615 |
| Dated | No (one number, ≈ 2010) | Every census back to 1887 (`P1082` population, `P585` point in time) |

Wikidata is a different source system with a different response shape, and its values are dated, so gold has to pick the population in effect on each day (an as-of join).

**Plan:**
- `config/sources.toml`: a second source (`system = "wikidata"`, `table = "population"` → `wikidata_population`) and a `wikidata_id` per station. Sources get a `kind` (`open_meteo_archive` | `wikidata_sparql`), because each API builds its request differently. The SPARQL query (population, point in time, statement rank) is built in `medallion/config.py`.
- The Lambda fetches every source, so it's renamed `ingest_weather` → **`ingest_sources`**. Raw: `wikidata/population/date=YYYY-MM-DD/<station_id>.json`, the SPARQL response untouched.
- Bronze: `src/00_bronze/ddl/wikidata_population/v001_create.sql` mirrors the SPARQL JSON (`head`, `results.bindings[]` of `{type, value, datatype}`). **`load_raw_files` doesn't change**: that's the test of config-driven bronze.
- Silver: `01_silver.populations`, one row per `(station_id, reference_date)` with `population` (people), plus `populations_quarantine`. Rules: missing point in time (undated values happen on Wikidata), a population that isn't a positive whole number, and `DeprecatedRank` statements (Wikidata's way of marking a value wrong). Duplicates for one date: preferred rank first, then the latest ingest.
- Shared mechanics (split, dedup, reconcile, classify changes, the MERGE statement text) move into a generic `medallion/silver.py`, with the entity rules in `medallion/readings.py` and `medallion/populations.py`. The MERGE still runs in the job scripts.
- Glue job `clean_populations`; `weather_pipeline` runs the two clean jobs in a `Parallel` state after the loads.
- Gold (step 4) joins each station-day to the latest `reference_date` on or before that day, and shows population as a column (the user's choice: no alert thresholds).

**Changed while building it (the user's two questions):**

1. *"What if weather is real time and Wikidata is batch: are you sure to mix the logic?"* The first version had one Lambda (`ingest_sources`) looping over every source inside one `weather_pipeline`. That couples sources with nothing in common: a Wikidata outage would fail the weather run, every daily run would re-download population counts that change once a decade, and one timeout and one retry policy would cover two very different APIs. Asked what I'd choose for **30 sources in production**, the answer was neither one pipeline for everything nor 30 hand-written ones, but **metadata-driven** ingestion:
   - **Code is shared by kind of source.** `build_request` covers `open_meteo_archive` and `wikidata_sparql`. `load_raw_files` and the silver mechanics are generic.
   - **Runs are isolated by execution.** A single state machine, **`source_pipeline`** (ingest → `load_raw_files` → the source's `silver_job`, which the Lambda returns), is started once per source with `{"source": …}`. Each source succeeds, fails, retries and backfills on its own.
   - **Config per source:** `silver_job` and `lag_days` (the weather archive publishes about 5 days late, Wikidata is current) in `config/sources.toml`. Schedules are in Terraform (`source_schedules`, EventBridge Scheduler, one per source: weather daily, population monthly), because Terraform can't read TOML. They're deployed **DISABLED**, so nothing runs or bills by accident.
   - A real-time source (step 6) is a separate streaming path into bronze, not this batch framework.
2. *Group code by entity instead of by kind of file* (the user's proposal, adopted with two adjustments). The layout goes from `src/<layer>/ddl/<table>/v001_create.sql` and `src/<layer>/glue_job/<process>.py` to:
   ```
   src/00_bronze/_ingestion/lambda_ingest_source.py, glue_job_load_raw_files.py   generic: serve every source
   src/00_bronze/open_meteo_hourly/ddl_open_meteo_hourly_v001_create.sql
   src/01_silver/readings/ddl_readings_v001_create.sql
                         /ddl_readings_quarantine_v001_create.sql
                         /glue_job_clean_readings.py
   src/ops/schema_migrations/glue_job_apply_ddl.py
   ```
   The adjustments: (a) processes that serve many tables (the Lambda, `load_raw_files`) don't belong to one entity, so they live in `_ingestion/` at the layer level; (b) a DDL file name carries its table and version (`ddl_<table>_v<NNN>_<verb>.sql`), because an entity folder holds its main table *and* its quarantine, each with its own versions. The runner enforces that a table lives in its entity's folder (the table is the entity or starts with `<entity>_`). This differs from the sibling repo's one-folder-per-table (its step 12): there the unit is the table, here it's the entity and the process that writes it.

Built:
- Sources get a `kind`, plus `silver_job` and `lag_days`. Stations get a `wikidata_id` (Q1486, Q44210, Q44254).
- [`medallion/silver.py`](../src/medallion/silver.py) now holds the generic mechanics: `collect_reasons`, split, `dedup_latest` (with a `prefer` order), `reconcile`, `classify_changes`, `merge_sql`, `quarantine_merge_sql`, `new_rejects`. The entity rules live in [`readings.py`](../src/medallion/readings.py) and [`populations.py`](../src/medallion/populations.py), so the two clean jobs differ only in their entity module and tables. **47 tests.**
- Migrations: bronze `wikidata_population` (the SPARQL JSON as is: `results.bindings[]` of `{datatype, type, value}`), silver `populations` (`NOT NULL`, `WRITE ORDERED BY station_id, reference_date`) and `populations_quarantine`.
- Terraform: Lambda `ingest_source`; state machine `source_pipeline` (the Clean step's `JobName` comes from the Lambda's output); two EventBridge Scheduler schedules (disabled) with their own role, allowed only `states:StartExecution`.

Verified (2026-09-27):
- **The reorganisation re-applied nothing.** `apply_ddl` reported **6 already applied, 0 pending, 6 moved** and refreshed the paths in `ops.schema_migrations`; a second run reported 0 moved. That's what identifying migrations by checksum buys.
- **`load_raw_files` loaded the new source without a code change**, locally first and then on AWS.
- **Two sources, two independent executions started together:** `wikidata_population-2026-09-27-a` (load 61 s, clean 68 s) and `open_meteo_hourly-2026-09-23-a` (load 49 s, clean 66 s), both SUCCEEDED and running at the same time.
- Silver `populations`: 27 dated counts (Buenos Aires 18 back to 1887, Córdoba 2, Ushuaia 7), with the 2022 census preferred: 3,121,707 / 1,505,250 / 82,615. `readings`: 288 rows over 4 days. Locally, a rerun of `clean_populations` changed nothing and committed no snapshot.
- **The as-of join works in Athena:** for each station-day, `max_by(population, reference_date)` over the counts with `reference_date <= day` gives the 2022 census for 2026-09-23.
- An unknown source (`nasa_satellites`) fails the execution in seconds: `ValueError` isn't retried.

**Learned:**
- **Building a Spark `Column` needs an active session.** A module-level `PREFER = (F.desc(...),)` failed at import in tests, and would have failed the same way in the Glue job, which imports modules before creating its session. Column expressions go inside functions.
- **Wikidata keeps its own data quality metadata:** statement **ranks** (preferred / normal / deprecated) instead of deleting wrong values, and qualifiers such as the point in time. Silver uses both: deprecated statements are rejected, and the preferred one wins a shared date. When a new census arrives, the old one is demoted to normal rank, so `is_preferred_rank` is a tracked value that the MERGE updates.
- **Step Functions can take a task's resource name from the state data** (`"JobName.$": "$.ingest.silver_job"`), so one state machine drives any source's silver job. IAM must still list every job it may start.
- **EventBridge Scheduler** (not the older EventBridge rules) has its own `state = DISABLED`, a time zone, and a role for its target. It's available on the Free plan.
- Terraform has `jsondecode` and `yamldecode` but no TOML decoder, so deployment settings that Terraform needs (schedules) live in Terraform, and source semantics live in the TOML.

## 4. Gold + data quality

- Glue job `build_reading_metrics` (`src/02_gold/`): `"02_gold".agg_readings_daily` (min/max/avg per station-day, plus the station's population as of that day from `01_silver.populations`) and window functions (rolling 24h average, `LAG` gap detection)
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
