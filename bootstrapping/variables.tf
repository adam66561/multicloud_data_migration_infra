variable "env" { type = string }
variable "region" { type = string }
variable "project" { type = string }
variable "repositories" {
  type        = list(string)
  description = "List of repositories that execute ci/cd pipelines and require access to the Terraform state bucket"
  sensitive   = false
}
variable "default_description" {
  type    = string
  default = "Terraform managed resources"
}
