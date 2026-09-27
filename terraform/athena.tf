# enforce_workgroup_configuration makes these settings win over whatever a client sends,
# so every query writes results to our bucket, encrypted, and is cut off above the bytes-scanned limit.
resource "aws_athena_workgroup" "lakehouse" {
  name          = var.project
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["athena-results"].bucket}/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
