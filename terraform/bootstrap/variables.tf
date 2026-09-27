variable "region" {
  type    = string
  default = "us-east-2"
}

variable "project" {
  type    = string
  default = "weather-lakehouse"
}

variable "alert_email" {
  description = "Where budget alerts go. Set it in terraform.tfvars (gitignored)."
  type        = string
}

# $100 of Free plan credits over ~5 months (until 2027-02-28) is about $20 a month.
variable "monthly_budget_usd" {
  type    = string
  default = "20"
}
