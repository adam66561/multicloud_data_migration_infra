variable "prefix" { type = string }
variable "destination_s3_bucket_id" { type = string }
variable "destination_s3_bucket_arn" { type = string }

variable "lambda_runtime" {
  description = "Lambda Python runtime"
  type        = string
  default     = "python3.12"
}

variable "memory_size" {
  description = "Lambda memory in MB"
  type        = number
  default     = 1024
}

variable "timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 60
}



