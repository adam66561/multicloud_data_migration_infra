variable "prefix" { type = string }
variable "glue_catalog_id" { type = string }
variable "glue_database_name" { type = list(string) }
# variable "s3_objects" { type        = list(string) }
variable "destination_s3_bucket_id" { type = string }
variable "destination_s3_bucket_arn" { type = string }
variable "config_s3_bucket_id" { type = string }
variable "config_s3_bucket_arn" { type = string }

variable "lambda_runtime" {
  description = "Lambda Python runtime"
  type        = string
  default     = "python3.12"
}

variable "lambda_memory_size" {
  type    = number
  default = 1024
}

variable "lambda_timeout" {
  type    = number
  default = 300
}

variable "logs_retention_in_days" {
  type    = number
  default = 30
}