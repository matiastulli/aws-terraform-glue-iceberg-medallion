output "buckets" {
  value = { for k, b in aws_s3_bucket.this : k => b.bucket }
}

output "glue_databases" {
  value = [for d in aws_glue_catalog_database.layer : d.name]
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lakehouse.name
}
