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

## Working agreements

- **Get the delivery path working early** (Terraform apply, Step Functions run, CI) and test only the high-risk logic until the end: the `(station_id, observed_at)` key, dedup, the validation that splits silver from quarantine, and gold data quality checks.
- **CI on GitHub, CD from the laptop.** GitHub Actions runs tests and `terraform validate` only. No AWS credentials in CI.
- The repo is **public**. Confirm with the user before pushing.
- Add commands to this file as each step introduces them. Don't document commands that don't exist yet.
