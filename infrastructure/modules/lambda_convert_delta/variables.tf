variable "prefix" { type = string }
variable "glue_database_name" { type = list(string) }
variable "glue_catalog_id" { type = string }
variable "destination_s3_bucket_id" { type = string }
variable "destination_s3_bucket_arn" { type = string }

variable "lambda_runtime" {
  description = "Lambda Python runtime"
  type        = string
  default     = "python3.12"
}

variable "lambda_memory_size" {
  description = "Lambda memory in MB"
  type        = number
  default     = 1024
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 60
}

variable "logs_retention_in_days" {
  type        = number
  default     = 30
}



