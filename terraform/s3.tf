# S3 bucket names are global, so a random suffix keeps them unique without putting the account ID in the name.
resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  # S3 tag values allow letters, digits, spaces and + - = . _ : / @ only (no commas).
  buckets = {
    raw            = "landing zone: raw JSON from Open-Meteo as received"
    lake           = "Iceberg warehouse: data and metadata files for bronze silver and gold"
    athena-results = "Athena query results"
    artifacts      = "deployed code: Glue job scripts and the medallion package and DDL migrations and config"
  }
}

# force_destroy lets terraform destroy empty the buckets. This is a learning project, not production data.
resource "aws_s3_bucket" "this" {
  for_each      = local.buckets
  bucket        = "${var.project}-${each.key}-${random_id.suffix.hex}"
  force_destroy = true

  tags = {
    Purpose = each.value
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Versioning on the lake protects against accidental deletes. Iceberg keeps its own history in snapshots,
# so once expire_snapshots deletes a file, the old S3 version is only a safety net: expire it after 7 days.
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.this["lake"].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket     = aws_s3_bucket.this["lake"].id
  depends_on = [aws_s3_bucket_versioning.lake]

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.this["athena-results"].id

  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}
    expiration {
      days = 7
    }
  }
}
