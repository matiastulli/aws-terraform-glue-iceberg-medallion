# One state machine for every source: ingest -> load into bronze -> clean into silver. A source is an execution
# input ({"source": "open_meteo_hourly"}), so each source runs, fails, retries and backfills on its own.
resource "aws_sfn_state_machine" "source_pipeline" {
  name     = "source_pipeline"
  role_arn = aws_iam_role.step_functions.arn
  definition = templatefile("${path.module}/state_machines/source_pipeline.asl.json", {
    ingest_function = aws_lambda_function.ingest_source.arn
    load_job        = aws_glue_job.this["load_raw_files"].name
    alerts_topic    = aws_sns_topic.alerts.arn
  })
}

resource "aws_sfn_state_machine" "gold_pipeline" {
  name     = "gold_pipeline"
  role_arn = aws_iam_role.step_functions.arn
  definition = templatefile("${path.module}/state_machines/gold_pipeline.asl.json", {
    build_job    = aws_glue_job.this["build_reading_metrics"].name
    alerts_topic = aws_sns_topic.alerts.arn
  })
}

# Event-driven gold: every source_pipeline execution that succeeds starts a gold rebuild. Step Functions publishes
# execution status changes to the default EventBridge bus by itself. Enabled: it only fires when a source runs.
resource "aws_cloudwatch_event_rule" "gold_after_source" {
  name        = "gold_after_source_pipeline"
  description = "Start gold_pipeline when a source_pipeline execution succeeds"
  event_pattern = jsonencode({
    source      = ["aws.states"]
    detail-type = ["Step Functions Execution Status Change"]
    detail = {
      status          = ["SUCCEEDED"]
      stateMachineArn = [aws_sfn_state_machine.source_pipeline.arn]
    }
  })
}

resource "aws_cloudwatch_event_target" "gold_after_source" {
  rule     = aws_cloudwatch_event_rule.gold_after_source.name
  arn      = aws_sfn_state_machine.gold_pipeline.arn
  role_arn = aws_iam_role.events.arn
  input_transformer {
    input_paths    = { execution = "$.detail.name" }
    input_template = "{\"triggered_by\": \"<execution>\"}"
  }
}

# When each source runs. Deployment settings, so they live here rather than in config/sources.toml (Terraform can't
# read TOML); a schedule naming a source that isn't in the config fails in the Lambda with the list of known sources.
# DISABLED until switched on on purpose: nothing runs, and nothing is billed, by accident.
locals {
  source_schedules = {
    open_meteo_hourly   = "cron(0 6 * * ? *)" # daily, 06:00 UTC
    wikidata_population = "cron(0 7 1 * ? *)" # monthly, the 1st at 07:00 UTC: census counts change every few years
  }
}

resource "aws_scheduler_schedule" "source_pipeline" {
  for_each                     = local.source_schedules
  name                         = "source_pipeline-${each.key}"
  schedule_expression          = each.value
  schedule_expression_timezone = "UTC"
  state                        = "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.source_pipeline.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ source = each.key })
  }
}
