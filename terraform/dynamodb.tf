# Pipeline control table (docs/PLAN.md step 9): where each stream stopped. clean_sensor_readings writes its last
# consumed bronze snapshot after a successful run (a conditional write that only moves forward); maintain_tables reads
# it and never expires a snapshot a stream still needs. Bookkeeping, like ops.schema_migrations, but a key-value item
# per process: a single-item read and an atomic conditional update, with no files or commits per run.
resource "aws_dynamodb_table" "watermarks" {
  name         = "${var.project}-watermarks"
  billing_mode = "PAY_PER_REQUEST" # a few writes an hour: on-demand costs nothing idle
  hash_key     = "process"

  attribute {
    name = "process"
    type = "S"
  }
}

# The newest reading of each station (one item per station_id), for an app that asks "how is Ushuaia now?" by key in
# milliseconds, without Athena. consume_sensor_readings updates it after each bronze commit, with a conditional write
# that keeps a late reading from replacing a newer one. A view derived from bronze: bronze stays the source of truth.
resource "aws_dynamodb_table" "latest_readings" {
  name         = "${var.project}-latest-readings"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "station_id"

  attribute {
    name = "station_id"
    type = "S"
  }
}
