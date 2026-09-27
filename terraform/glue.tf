# Glue Data Catalog databases, one per medallion layer. Iceberg tables in them are registered by Glue jobs
# and queried by Athena. The location is the default prefix for tables created without an explicit one.
resource "aws_glue_catalog_database" "layer" {
  for_each     = toset(["bronze", "silver", "gold"])
  name         = each.key
  description  = "${each.key} layer of the weather medallion lakehouse"
  location_uri = "s3://${aws_s3_bucket.this["lake"].bucket}/${each.key}/"
}
