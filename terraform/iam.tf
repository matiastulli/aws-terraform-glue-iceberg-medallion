data "aws_iam_policy_document" "assume" {
  for_each = {
    glue           = "glue.amazonaws.com"
    lambda         = "lambda.amazonaws.com"
    step_functions = "states.amazonaws.com"
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
    resources = [aws_lambda_function.ingest_weather.arn]
  }
  statement {
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = [aws_glue_job.this["load_raw_files"].arn]
  }
}

resource "aws_iam_role_policy" "step_functions" {
  name   = "run-pipeline"
  role   = aws_iam_role.step_functions.id
  policy = data.aws_iam_policy_document.step_functions.json
}
