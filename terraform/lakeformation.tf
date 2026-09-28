# Governance (docs/PLAN.md step 8): an analyst who reads gold through Lake Formation, and only part of it.
#
# Hybrid access mode: gold's S3 location is registered with hybrid access, and only the analyst is opted in. Opted-in
# principals get Lake Formation permissions (tables, columns, rows); everyone else, the Glue jobs included, keeps IAM +
# IAM_ALLOWED_PRINCIPALS as before. Bronze and silver aren't registered: IAM alone decides there.

locals {
  gold_db = aws_glue_catalog_database.this["02_gold"].name
}

# The Terraform user administers Lake Formation (a grant needs an admin). The defaults are the account's current ones,
# written out because this resource replaces them: new databases and tables keep IAM_ALLOWED_PRINCIPALS, so the next
# table apply_ddl creates behaves like the ones before it.
resource "aws_lakeformation_data_lake_settings" "this" {
  admins = [data.aws_caller_identity.current.arn]

  create_database_default_permissions {
    permissions = ["ALL"]
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }
  create_table_default_permissions {
    permissions = ["ALL"]
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }
  parameters = {
    CROSS_ACCOUNT_VERSION = "4"
    SET_CONTEXT           = "TRUE"
  }
}

# Lake Formation hands out credentials for this prefix to principals it authorizes (credential vending), through its
# service-linked role.
resource "aws_lakeformation_resource" "gold" {
  arn                     = "${aws_s3_bucket.this["lake"].arn}/${local.gold_db}"
  use_service_linked_role = true
  hybrid_access_enabled   = true
  depends_on              = [aws_lakeformation_data_lake_settings.this]
}

# --- LF-tags: grant by tag instead of by table ------------------------------------------------------------------------

resource "aws_lakeformation_lf_tag" "layer" {
  key        = "layer"
  values     = ["bronze", "silver", "gold"]
  depends_on = [aws_lakeformation_data_lake_settings.this]
}

# On the database: its tables inherit the tag, so a new gold table is covered by the tag grants without a new grant.
resource "aws_lakeformation_resource_lf_tags" "gold" {
  database {
    name = local.gold_db
  }
  lf_tag {
    key   = aws_lakeformation_lf_tag.layer.key
    value = "gold"
  }
}

# --- The analyst ------------------------------------------------------------------------------------------------------

# Assumed by the Terraform user for tests (sts assume-role): temporary credentials, no access keys.
resource "aws_iam_role" "analyst" {
  name = "${var.project}-analyst"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = data.aws_caller_identity.current.arn }
      Action    = "sts:AssumeRole"
    }]
  })
}

# IAM decides which APIs the analyst may call; Lake Formation decides which data those calls return. No S3 access to
# the lake: data only reaches the analyst through Lake Formation's vended credentials.
data "aws_iam_policy_document" "analyst" {
  statement {
    sid       = "QueryInTheWorkgroup"
    actions   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution", "athena:GetWorkGroup"]
    resources = [aws_athena_workgroup.lakehouse.arn]
  }
  statement {
    sid     = "ReadTheGoldCatalog"
    actions = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
    resources = [
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:database/${local.gold_db}",
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${local.gold_db}/*",
    ]
  }
  statement {
    sid       = "GetCredentialsFromLakeFormation"
    actions   = ["lakeformation:GetDataAccess"]
    resources = ["*"]
  }
  statement {
    sid       = "WriteQueryResults"
    actions   = ["s3:GetBucketLocation", "s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
    resources = [aws_s3_bucket.this["athena-results"].arn, "${aws_s3_bucket.this["athena-results"].arn}/*"]
  }
}

resource "aws_iam_role_policy" "analyst" {
  name   = "query-gold"
  role   = aws_iam_role.analyst.id
  policy = data.aws_iam_policy_document.analyst.json
}

# Rows of the mainland stations only, and every column but population.
resource "aws_lakeformation_data_cells_filter" "analyst_readings_daily" {
  table_data {
    table_catalog_id = data.aws_caller_identity.current.account_id
    database_name    = local.gold_db
    table_name       = "agg_readings_daily"
    name             = "analyst_mainland_without_population"

    column_wildcard {
      excluded_column_names = ["population"]
    }
    row_filter {
      filter_expression = "station_id IN ('buenos_aires', 'cordoba')"
    }
  }
  depends_on = [aws_lakeformation_data_lake_settings.this]
}

# See every gold database and table (by tag), and read agg_readings_daily only through the filter.
resource "aws_lakeformation_permissions" "analyst_describe_gold_databases" {
  principal   = aws_iam_role.analyst.arn
  permissions = ["DESCRIBE"]
  lf_tag_policy {
    resource_type = "DATABASE"
    expression {
      key    = aws_lakeformation_lf_tag.layer.key
      values = ["gold"]
    }
  }
  depends_on = [aws_lakeformation_resource_lf_tags.gold]
}

resource "aws_lakeformation_permissions" "analyst_describe_gold_tables" {
  principal   = aws_iam_role.analyst.arn
  permissions = ["DESCRIBE"]
  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = aws_lakeformation_lf_tag.layer.key
      values = ["gold"]
    }
  }
  depends_on = [aws_lakeformation_resource_lf_tags.gold]
}

resource "aws_lakeformation_permissions" "analyst_select_readings_daily" {
  principal   = aws_iam_role.analyst.arn
  permissions = ["SELECT"]
  data_cells_filter {
    table_catalog_id = data.aws_caller_identity.current.account_id
    database_name    = local.gold_db
    table_name       = "agg_readings_daily"
    name             = aws_lakeformation_data_cells_filter.analyst_readings_daily.table_data[0].name
  }
}

# Opt the analyst in on gold's tables: from here on, Lake Formation (not IAM_ALLOWED_PRINCIPALS) decides what they see.
resource "aws_lakeformation_opt_in" "analyst_gold" {
  principal {
    data_lake_principal_identifier = aws_iam_role.analyst.arn
  }
  resource_data {
    table {
      catalog_id    = data.aws_caller_identity.current.account_id # AWS stores it; leaving it out plans a replacement every time
      database_name = local.gold_db
      wildcard      = true
    }
  }
  depends_on = [
    aws_lakeformation_resource.gold,
    aws_lakeformation_permissions.analyst_describe_gold_tables,
    aws_lakeformation_permissions.analyst_select_readings_daily,
  ]
}

output "analyst_role_arn" {
  description = "Assume it to query gold as the analyst (sts assume-role)"
  value       = aws_iam_role.analyst.arn
  sensitive   = true # holds the account ID: keep it out of logs
}
