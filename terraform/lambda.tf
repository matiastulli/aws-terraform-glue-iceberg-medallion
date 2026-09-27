data "archive_file" "ingest_weather" {
  type        = "zip"
  output_path = "${path.module}/.build/ingest_weather.zip"

  source {
    content  = file("${local.src}/00_bronze/ingest_weather.py")
    filename = "ingest_weather.py"
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

resource "aws_cloudwatch_log_group" "ingest_weather" {
  name              = "/aws/lambda/ingest_weather"
  retention_in_days = 14
}

resource "aws_lambda_function" "ingest_weather" {
  function_name    = "ingest_weather"
  description      = "Fetch one day of Open-Meteo readings per station and land them in the raw bucket"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.11"
  handler          = "ingest_weather.handler"
  filename         = data.archive_file.ingest_weather.output_path
  source_code_hash = data.archive_file.ingest_weather.output_base64sha256
  timeout          = 120
  memory_size      = 256

  environment {
    variables = {
      RAW_BUCKET = aws_s3_bucket.this["raw"].bucket
    }
  }

  depends_on = [aws_cloudwatch_log_group.ingest_weather]
}
