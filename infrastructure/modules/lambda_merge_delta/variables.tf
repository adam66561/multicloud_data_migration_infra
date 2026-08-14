variable "prefix" { type = string }

variable "glue_catalog_id" { type = string }
variable "glue_database_name" { type = list(string) }

# variable "s3_objects" { type        = list(string) }

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

variable "create_dynamodb" {
  type    = bool
  default = false
}

variable "type_of_event" {
  type    = string
  default = "fifo"

  validation {
    condition     = contains(["s3", "fifo"], var.type_of_event)
    error_message = "type_of_event must be either 's3' or 'fifo'"
  }
}