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
