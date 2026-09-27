# Pipeline failures (a source that can't be ingested, a gold data quality check that fails) are published here.
# An email subscription stays "pending confirmation" until the address clicks the link AWS sends.
resource "aws_sns_topic" "alerts" {
  name = "${var.project}-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
