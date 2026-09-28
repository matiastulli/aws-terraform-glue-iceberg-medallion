# Near real-time (docs/PLAN.md step 6): simulate_sensors -> SQS -> consume_sensor_readings -> bronze Iceberg tables.
# Kinesis is blocked on the Free plan, so the stream is an SQS standard queue: at-least-once and unordered, like the
# sensors themselves. The stream's name is its bronze table, from config/sources.toml [[streams]] (Terraform can't read
# TOML; a name that isn't in the config fails in the Lambdas with the list of known streams).
locals {
  stream = "simulator_readings"
  streaming_code = {
    simulate_sensors        = "lambda_simulate_sensors.py"
    consume_sensor_readings = "lambda_consume_sensor_readings.py"
  }
}

# --- Queue ------------------------------------------------------------------------------------------------------------

# Messages that failed 3 receives: a write that kept failing (malformed messages go to the quarantine table instead).
resource "aws_sqs_queue" "stream_dlq" {
  name                      = "${local.stream}-dlq"
  message_retention_seconds = 1209600 # 14 days, the maximum: time to look, fix and redrive
}

resource "aws_sqs_queue" "stream" {
  name                       = local.stream
  visibility_timeout_seconds = 360 # 6x the consumer's timeout, as AWS recommends for Lambda event sources
  message_retention_seconds  = 86400
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.stream_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_cloudwatch_metric_alarm" "stream_dlq" {
  alarm_name          = "${local.stream}-dlq-not-empty"
  alarm_description   = "Messages reached the ${local.stream} dead-letter queue: a bronze write failed 3 times"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.stream_dlq.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# --- Code -------------------------------------------------------------------------------------------------------------

# pyiceberg, pyarrow and their dependencies: built by scripts/build_pyiceberg_layer.sh (Linux wheels), zipped here.
# The zip is over the 50 MB a direct upload allows, so the layer is published from the artifacts bucket.
data "archive_file" "pyiceberg_layer" {
  type        = "zip"
  source_dir  = "${path.module}/.build/pyiceberg_layer"
  output_path = "${path.module}/.build/pyiceberg_layer.zip"
}

resource "aws_s3_object" "pyiceberg_layer" {
  bucket      = aws_s3_bucket.this["artifacts"].id
  key         = "layers/pyiceberg_layer.zip"
  source      = data.archive_file.pyiceberg_layer.output_path
  source_hash = data.archive_file.pyiceberg_layer.output_base64sha256
}

resource "aws_lambda_layer_version" "pyiceberg" {
  layer_name          = "pyiceberg"
  description         = "pyiceberg[glue,pyarrow,pyiceberg-core] 0.12.0 with pyarrow 20.0.0 (src/00_bronze/_streaming/requirements.txt)"
  s3_bucket           = aws_s3_object.pyiceberg_layer.bucket
  s3_key              = aws_s3_object.pyiceberg_layer.key
  source_code_hash    = data.archive_file.pyiceberg_layer.output_base64sha256
  compatible_runtimes = ["python3.11"]
}

data "archive_file" "streaming" {
  for_each    = local.streaming_code
  type        = "zip"
  output_path = "${path.module}/.build/${each.key}.zip"

  source {
    content  = file("${local.src}/00_bronze/_streaming/${each.value}")
    filename = each.value
  }
  dynamic "source" {
    for_each = ["__init__.py", "config.py", "contract.py", "stream.py"]
    content {
      content  = file("${local.src}/medallion/${source.value}")
      filename = "medallion/${source.value}"
    }
  }
  source {
    content  = file("${path.module}/../config/sources.toml")
    filename = "sources.toml"
  }
}

resource "aws_cloudwatch_log_group" "streaming" {
  for_each          = local.streaming_code
  name              = "/aws/lambda/${each.key}"
  retention_in_days = 14
}

# --- Simulator --------------------------------------------------------------------------------------------------------

resource "aws_lambda_function" "simulate_sensors" {
  function_name    = "simulate_sensors"
  description      = "Send one minute of simulated sensor readings, with resends and late readings, to the stream's queue"
  role             = aws_iam_role.simulate_sensors.arn
  runtime          = "python3.11"
  handler          = "lambda_simulate_sensors.handler"
  filename         = data.archive_file.streaming["simulate_sensors"].output_path
  source_code_hash = data.archive_file.streaming["simulate_sensors"].output_base64sha256
  timeout          = 30
  memory_size      = 128

  environment {
    variables = {
      STREAM    = local.stream
      QUEUE_URL = aws_sqs_queue.stream.url
    }
  }
  depends_on = [aws_cloudwatch_log_group.streaming]
}

# Every minute, DISABLED until switched on for a test: nothing runs, and nothing is billed, by accident.
resource "aws_scheduler_schedule" "simulate_sensors" {
  name                = "simulate_sensors"
  schedule_expression = "rate(1 minute)"
  state               = "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.simulate_sensors.arn
    role_arn = aws_iam_role.scheduler.arn
  }
}

# --- Consumer ---------------------------------------------------------------------------------------------------------

resource "aws_lambda_function" "consume_sensor_readings" {
  function_name    = "consume_sensor_readings"
  description      = "Append SQS sensor messages to 00_bronze.${local.stream} (or its quarantine) with pyiceberg"
  role             = aws_iam_role.consume_sensor_readings.arn
  runtime          = "python3.11"
  handler          = "lambda_consume_sensor_readings.handler"
  filename         = data.archive_file.streaming["consume_sensor_readings"].output_path
  source_code_hash = data.archive_file.streaming["consume_sensor_readings"].output_base64sha256
  layers           = [aws_lambda_layer_version.pyiceberg.arn]
  timeout          = 60
  memory_size      = 512 # pyarrow

  environment {
    variables = {
      STREAM       = local.stream
      BRONZE_DB    = aws_glue_catalog_database.this["00_bronze"].name
      LATEST_TABLE = aws_dynamodb_table.latest_readings.name
    }
  }
  depends_on = [aws_cloudwatch_log_group.streaming]
}

# Lambda polls the queue and invokes the consumer with a batch: up to 100 messages or 20 seconds, whichever first.
# Each batch is an Iceberg commit, so bigger batches mean fewer, larger files. At most 2 consumers at a time: commits
# race on the table's metadata pointer, and the consumer retries a lost race.
resource "aws_lambda_event_source_mapping" "consume_sensor_readings" {
  event_source_arn                   = aws_sqs_queue.stream.arn
  function_name                      = aws_lambda_function.consume_sensor_readings.arn
  batch_size                         = 100
  maximum_batching_window_in_seconds = 20
  function_response_types            = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = 2
  }
}

# --- IAM --------------------------------------------------------------------------------------------------------------

resource "aws_iam_role" "simulate_sensors" {
  name               = "${var.project}-simulate-sensors"
  assume_role_policy = data.aws_iam_policy_document.assume["lambda"].json
}

resource "aws_iam_role_policy_attachment" "simulate_sensors_logs" {
  role       = aws_iam_role.simulate_sensors.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "simulate_sensors" {
  name = "send-to-stream"
  role = aws_iam_role.simulate_sensors.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sqs:SendMessage", Resource = aws_sqs_queue.stream.arn }]
  })
}

resource "aws_iam_role" "consume_sensor_readings" {
  name               = "${var.project}-consume-sensor-readings"
  assume_role_policy = data.aws_iam_policy_document.assume["lambda"].json
}

resource "aws_iam_role_policy_attachment" "consume_sensor_readings_logs" {
  role       = aws_iam_role.consume_sensor_readings.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "consume_sensor_readings" {
  statement {
    sid       = "ReadQueue"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.stream.arn]
  }
  statement {
    sid       = "PublishLatestReadings"
    actions   = ["dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.latest_readings.arn]
  }
  statement {
    sid     = "CommitToTheStreamTables"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:UpdateTable"]
    resources = [
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.this["00_bronze"].name}",
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.this["00_bronze"].name}/${local.stream}",
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.this["00_bronze"].name}/${local.stream}_quarantine",
    ]
  }
  # DeleteObject: when a commit loses the race with the other consumer, pyiceberg deletes the manifests it wrote for
  # it; without the permission they stay behind as orphan files (seen on the first concurrent run).
  statement {
    sid       = "ReadWriteTheStreamTablesFiles"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.this["lake"].arn}/00_bronze/${local.stream}*"]
  }
  statement {
    sid       = "ListTheStreamTablesFiles"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.this["lake"].arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["00_bronze/${local.stream}*"]
    }
  }
}

resource "aws_iam_role_policy" "consume_sensor_readings" {
  name   = "append-to-stream-tables"
  role   = aws_iam_role.consume_sensor_readings.id
  policy = data.aws_iam_policy_document.consume_sensor_readings.json
}
