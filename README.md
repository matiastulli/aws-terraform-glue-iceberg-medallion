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

```
Step Functions weather_pipeline ({"date": "YYYY-MM-DD"})
  ├─ Lambda ingest_weather      Open-Meteo archive API → s3://<raw>/open_meteo/hourly/date=…/<station_id>.json
  └─ Map over sources
       └─ Glue load_raw_files   raw JSON → "00_bronze".open_meteo_hourly (Iceberg, append-only)
```

- Sources and stations are configured in [`config/sources.toml`](config/sources.toml). Table names, raw paths and API requests are derived from it.
- Tables are created and changed only by versioned migrations in `src/<NN_layer>/ddl/<table>/`, applied by the `apply_ddl` Glue job and recorded in `ops.schema_migrations`. Jobs check that their output matches the table exactly before writing.
- Pure logic lives in [`src/medallion/`](src/medallion/) and is tested with `pytest` on local PySpark. [CI](.github/workflows/ci.yml) runs the tests and `terraform fmt` / `validate`; deploys are `terraform apply` from a laptop.

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17) && .venv/bin/pytest
terraform -chdir=terraform plan -out=tfplan && terraform -chdir=terraform apply tfplan
aws glue start-job-run --job-name apply_ddl
aws stepfunctions start-execution --state-machine-arn <weather_pipeline ARN> --input '{"date":"2026-09-20"}'
```

Query in Athena (workgroup `weather-lakehouse`). The numbered databases need double quotes in queries:

```sql
SELECT station_id, cardinality(hourly.time) AS hours FROM "00_bronze".open_meteo_hourly;
```
