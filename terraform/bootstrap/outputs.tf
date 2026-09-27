output "tfstate_bucket" {
  value = aws_s3_bucket.tfstate.bucket
}

# Partial backend config for the main stack: terraform output -raw backend_hcl > ../backend.hcl
output "backend_hcl" {
  value = <<-EOT
    bucket = "${aws_s3_bucket.tfstate.bucket}"
    region = "${var.region}"
  EOT
}
