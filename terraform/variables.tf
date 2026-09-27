variable "region" {
  type    = string
  default = "us-east-2"
}

variable "project" {
  type    = string
  default = "weather-lakehouse"
}

# Athena bills per byte scanned; queries that would scan more than this are cancelled.
variable "athena_bytes_scanned_cutoff" {
  type    = number
  default = 1073741824 # 1 GiB
}

variable "alert_email" {
  description = "Where pipeline failure alerts go (SNS email). Set it in terraform.tfvars (gitignored)."
  type        = string
}
