variable "prefix" { type = string }
variable "name" { type = string }
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
  type = string 
  default = "cdc"
}

variable "target_path" { 
  type = string 
  default = "" 
}

variable "type_of_event" {
  type    = string
  default = "fifo"

  validation {
    condition     = contains(["s3", "fifo"], var.type_of_event)
    error_message = "type_of_event must be either 's3' or 'fifo'"
  }
}

variable "audit_logs" {
  type    = bool
  default = false
}