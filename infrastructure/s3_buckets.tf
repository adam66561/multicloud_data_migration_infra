module "lambda_tests" {
  source                                 = "terraform-aws-modules/s3-bucket/aws"
  version                                = "5.14.1"
  bucket                                 = join(local.default_separator, [local.prefix, "lambda", "tests"])
  force_destroy                          = true
  attach_policy                          = true
  attach_deny_insecure_transport_policy  = true
  attach_deny_unencrypted_object_uploads = false
  attach_require_latest_tls_policy       = true

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  control_object_ownership = true
  object_ownership         = "BucketOwnerEnforced"

  versioning = { enabled = false }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm = "AES256"
      }
    }
  }
}

resource "aws_s3_object" "lambda_tests_config" {
  bucket = module.lambda_tests.s3_bucket_id
  key    = "primary_keys.json"
  source = "${path.module}/primary_keys.json"
  etag   = filemd5("./primary_keys.json")
}