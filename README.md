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
- [`terraform/`](terraform/): S3 buckets `raw` (landing JSON), `lake` (Iceberg warehouse) and `athena-results`; Glue Data Catalog databases `bronze`, `silver` and `gold`; and an Athena workgroup that enforces the result location and a per-query bytes-scanned limit.

```sh
cp terraform/bootstrap/terraform.tfvars.example terraform/bootstrap/terraform.tfvars   # set alert_email
terraform -chdir=terraform/bootstrap init && terraform -chdir=terraform/bootstrap apply
terraform -chdir=terraform/bootstrap output -raw backend_hcl > terraform/backend.hcl
terraform -chdir=terraform init -backend-config=backend.hcl && terraform -chdir=terraform apply
```
