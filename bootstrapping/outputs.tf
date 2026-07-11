output "terraform_state_bucket_name" {
  description = "Name of the Terraform state bucket"
  value       = module.terraform_state_bucket.s3_bucket_id
}

output "terraform_state_bucket_arn" {
  description = "ARN of the Terraform state bucket"
  value       = module.terraform_state_bucket.s3_bucket_arn
}

output "terraform_state_bucket_region" {
  description = "Region of the Terraform state bucket"
  value       = module.terraform_state_bucket.s3_bucket_region
}
