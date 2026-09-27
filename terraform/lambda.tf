# One generic ingest Lambda: the event names the source, config/sources.toml says how to ask its API. Each source runs
# in its own source_pipeline execution, so the function is shared but the runs are isolated.
data "archive_file" "ingest_source" {
  type        = "zip"
  output_path = "${path.module}/.build/ingest_source.zip"

  source {
    content  = file("${local.src}/00_bronze/_ingestion/lambda_ingest_source.py")
    filename = "lambda_ingest_source.py"
  }
  source {
    content  = file("${local.src}/medallion/__init__.py")
    filename = "medallion/__init__.py"
  }
  source {
    content  = file("${local.src}/medallion/config.py")
    filename = "medallion/config.py"
  }
  source {
    content  = file("${path.module}/../config/sources.toml")
    filename = "sources.toml"
  }
}

resource "aws_cloudwatch_log_group" "ingest_source" {
  name              = "/aws/lambda/ingest_source"
  retention_in_days = 14
}

resource "aws_lambda_function" "ingest_source" {
  function_name    = "ingest_source"
  description      = "Fetch one source from config/sources.toml for every station and land the responses in the raw bucket"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.11"
  handler          = "lambda_ingest_source.handler"
  filename         = data.archive_file.ingest_source.output_path
  source_code_hash = data.archive_file.ingest_source.output_base64sha256
  timeout          = 120
  memory_size      = 256

  environment {
    variables = {
      RAW_BUCKET = aws_s3_bucket.this["raw"].bucket
    }
  }

  depends_on = [aws_cloudwatch_log_group.ingest_source]
}
