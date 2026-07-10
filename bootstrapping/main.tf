data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

module "terraform_state_bucket" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "5.14.1"

  bucket = join(local.default_separator, ["tfstate", "s3", local.prefix, local.account_id, local.region])

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  versioning = {
    enabled = true
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm = "AES256"
      }
    }
  }

  force_destroy = false

  attach_deny_insecure_transport_policy = true
  attach_require_latest_tls_policy      = true

  tags = {
    name      = join(local.default_separator, ["tfstate", "s3", local.prefix, local.account_id, local.region])
    env       = var.env
    managedBy = "Terraform"
    purpose   = "Terraform state"
  }
}
