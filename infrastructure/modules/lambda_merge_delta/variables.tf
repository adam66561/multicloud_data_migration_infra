variable "prefix" { type = string }
variable "s3_objects" { type = list(string) }
variable "source_s3_bucket_id" { type = string }
variable "target_s3_bucket_id" { type = string }
variable "config_s3_bucket_id" { type = string }
variable "config_key" { type = string }

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

variable "source_cdc_path" {
  type    = string
  default = "cdc"
}

variable "target_path" {
  type    = string
  default = ""
}

variable "audit_logs" {
  type    = bool
  default = false
}

variable "s3_prefixes_per_schema" {
  type = map(object({
    source = string
    target = string
  }))
}

variable "date_partition_subfolder_count" {
  description = "Number of subfolders used for date partitioning in the S3 path"
  type        = number
  default     = 1

  validation {
    condition     = var.date_partition_subfolder_count >= 0
    error_message = "date_partition_subfolder_count must be greater than or equal to 0."
  }
}