# AWS Glue + Iceberg medallion lakehouse

A medallion lakehouse (bronze → silver → gold) on AWS, built with **Apache Iceberg**, **AWS Glue** (PySpark), **Athena**, **Step Functions** and **Terraform**, using hourly weather-station telemetry.

It's the AWS counterpart of [databricks-pyspark-delta-medallion](https://github.com/matiastulli/databricks-pyspark-delta-medallion). The build is done one step at a time; [`docs/PLAN.md`](docs/PLAN.md) tracks the steps and what each one taught.

## Local setup

Python 3.11 and Java 17. Versions match AWS Glue 5.1 (Spark 3.5.6, Iceberg 1.10.0).

```sh
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
.venv/bin/python scripts/local_iceberg_smoke.py
```

The smoke test creates an Iceberg table partitioned by `days(observed_at)` in `./local-warehouse`, upserts into it with `MERGE INTO`, and reads the first snapshot back with time travel.

## Infrastructure

Terraform in [`terraform/`](terraform/), region `us-east-2`:

- [`terraform/bootstrap/`](terraform/bootstrap/): the S3 bucket for Terraform state and a monthly cost budget with email alerts. Applied once, with local state.
- [`terraform/`](terraform/): S3 buckets `raw` (landing JSON), `lake` (Iceberg warehouse), `athena-results` and `artifacts` (deployed code); Glue Data Catalog databases `00_bronze`, `01_silver`, `02_gold` (numbered so they sort in pipeline order) and `ops`; and an Athena workgroup that enforces the result location and a per-query bytes-scanned limit.

```sh
cp terraform/bootstrap/terraform.tfvars.example terraform/bootstrap/terraform.tfvars   # set alert_email
terraform -chdir=terraform/bootstrap init && terraform -chdir=terraform/bootstrap apply
terraform -chdir=terraform/bootstrap output -raw backend_hcl > terraform/backend.hcl
terraform -chdir=terraform init -backend-config=backend.hcl && terraform -chdir=terraform apply
```

## Pipeline

Two sources, each run as its own execution of one generic state machine:

| Source (bronze table) | API | Silver | Schedule |
|---|---|---|---|
| `open_meteo_hourly` | [Open-Meteo archive](https://open-meteo.com/en/docs/historical-weather-api): hourly temperature, humidity, precipitation, wind | `readings` (one row per station-hour) | daily |
| `wikidata_population` | [Wikidata SPARQL](https://query.wikidata.org/): each city's population counts with their dates | `populations` (one row per station and count date) | monthly |

```
EventBridge Scheduler (one schedule per source, deployed disabled)
  └─ Step Functions source_pipeline ({"source": "<bronze table>", "date": "YYYY-MM-DD"})
       ├─ Lambda ingest_source   the source's API → s3://<raw>/<system>/<table>/date=…/<station_id>.json
       ├─ Glue load_raw_files    raw JSON → "00_bronze".<source> (Iceberg, append-only)
       └─ Glue <silver_job>      the batch → "01_silver".<entity> (MERGE on its key)
                                             + "01_silver".<entity>_quarantine (rejects with rejection_reasons)
```

- Sources and stations are configured in [`config/sources.toml`](config/sources.toml). Table names, raw paths, API requests, the silver job and the default run date are derived from it. A failing source never blocks another.
- Code is grouped by entity: `src/<NN_layer>/<entity>/` holds the entity's migrations (`ddl_<table>_v<NNN>_<verb>.sql`) and the job that writes it (`glue_job_<process>.py`); processes that serve every source are in `src/00_bronze/_ingestion/`.
- Tables are created and changed only by those migrations, applied by the `apply_ddl` Glue job and recorded in `ops.schema_migrations`. Jobs check that their output matches the table exactly before writing.
- Pure logic lives in [`src/medallion/`](src/medallion/) and is tested with `pytest` on local PySpark. [CI](.github/workflows/ci.yml) runs the tests and `terraform fmt` / `validate`; deploys are `terraform apply` from a laptop.

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17) && .venv/bin/pytest
terraform -chdir=terraform plan -out=tfplan && terraform -chdir=terraform apply tfplan
aws glue start-job-run --job-name apply_ddl
aws stepfunctions start-execution --state-machine-arn <source_pipeline ARN> --name open_meteo_hourly-2026-09-20-a --input '{"source":"open_meteo_hourly","date":"2026-09-20"}'
aws stepfunctions start-execution --state-machine-arn <source_pipeline ARN> --name wikidata_population-2026-09-27-a --input '{"source":"wikidata_population"}'
```

Query in Athena (workgroup `weather-lakehouse`). The numbered databases need double quotes in queries:

```sql
SELECT station_id, cardinality(hourly.time) AS hours FROM "00_bronze".open_meteo_hourly;
SELECT station_id, observed_at, temperature_c FROM "01_silver".readings ORDER BY observed_at DESC LIMIT 10;
SELECT * FROM "01_silver"."readings$snapshots";   -- Iceberg metadata table: one snapshot per commit
SELECT station_id, reference_date, population FROM "01_silver".populations ORDER BY 1, 2;
```
