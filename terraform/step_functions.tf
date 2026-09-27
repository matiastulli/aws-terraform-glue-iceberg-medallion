resource "aws_sfn_state_machine" "weather_pipeline" {
  name     = "weather_pipeline"
  role_arn = aws_iam_role.step_functions.arn
  definition = templatefile("${path.module}/state_machines/weather_pipeline.asl.json", {
    ingest_function = aws_lambda_function.ingest_weather.arn
    load_job        = aws_glue_job.this["load_raw_files"].name
  })
}
