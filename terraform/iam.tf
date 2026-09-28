data "aws_iam_policy_document" "assume" {
  for_each = {
    glue           = "glue.amazonaws.com"
    lambda         = "lambda.amazonaws.com"
    step_functions = "states.amazonaws.com"
    scheduler      = "scheduler.amazonaws.com"
    events         = "events.amazonaws.com"
  }
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = [each.value]
    }
  }
}

# Glue jobs: read code, config and raw files; read and write the lake; the Glue Data Catalog and logs come from the
# AWS managed service role policy.
resource "aws_iam_role" "glue" {
  name               = "${var.project}-glue-jobs"
  assume_role_policy = data.aws_iam_policy_document.assume["glue"].json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue" {
  statement {
    sid       = "ReadCodeAndRaw"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = flatten([for b in ["artifacts", "raw"] : [aws_s3_bucket.this[b].arn, "${aws_s3_bucket.this[b].arn}/*"]])
  }
  statement {
    sid       = "ReadWriteLake"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.this["lake"].arn, "${aws_s3_bucket.this["lake"].arn}/*"]
  }
}

resource "aws_iam_role_policy" "glue" {
  name   = "lakehouse-data"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue.json
}

# Lambda: write raw files, and logs.
resource "aws_iam_role" "lambda" {
  name               = "${var.project}-lambda"
  assume_role_policy = data.aws_iam_policy_document.assume["lambda"].json
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.this["raw"].arn}/*"]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "write-raw"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

# Step Functions: invoke the ingest Lambda and run the Glue jobs synchronously (.sync polls the job run).
resource "aws_iam_role" "step_functions" {
  name               = "${var.project}-step-functions"
  assume_role_policy = data.aws_iam_policy_document.assume["step_functions"].json
}

data "aws_iam_policy_document" "step_functions" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.ingest_source.arn]
  }
  statement {
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = [for job in ["load_raw_files", "clean_readings", "clean_populations", "build_reading_metrics"] : aws_glue_job.this[job].arn]
  }
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
  # backfill runs source_pipeline and gold_pipeline as child executions and waits for them (.sync).
  statement {
    actions   = ["states:StartExecution"]
    resources = [for machine in local.child_state_machines : machine.arn]
  }
  statement {
    actions   = ["states:DescribeExecution", "states:StopExecution"]
    resources = [for machine in local.child_state_machines : "${replace(machine.arn, ":stateMachine:", ":execution:")}:*"]
  }
  # .sync waits on child executions through a rule that Step Functions manages in the account.
  statement {
    actions   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
    resources = ["arn:aws:events:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule"]
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  child_state_machines = [aws_sfn_state_machine.source_pipeline, aws_sfn_state_machine.gold_pipeline]
}

resource "aws_iam_role_policy" "step_functions" {
  name   = "run-pipeline"
  role   = aws_iam_role.step_functions.id
  policy = data.aws_iam_policy_document.step_functions.json
}

# EventBridge Scheduler: start source_pipeline executions, invoke the sensor simulator and start table maintenance.
resource "aws_iam_role" "scheduler" {
  name               = "${var.project}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.assume["scheduler"].json
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.source_pipeline.arn]
  }
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.simulate_sensors.arn]
  }
  statement {
    actions   = ["glue:StartJobRun"]
    resources = [aws_glue_job.this["maintain_tables"].arn]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  name   = "start-source-pipeline"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}

# EventBridge rule: start gold_pipeline executions, nothing else.
resource "aws_iam_role" "events" {
  name               = "${var.project}-events"
  assume_role_policy = data.aws_iam_policy_document.assume["events"].json
}

data "aws_iam_policy_document" "events" {
  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.gold_pipeline.arn]
  }
}

resource "aws_iam_role_policy" "events" {
  name   = "start-gold-pipeline"
  role   = aws_iam_role.events.id
  policy = data.aws_iam_policy_document.events.json
}
