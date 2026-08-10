module "lambda_create_parquet" {
  source                    = "./modules/lambda_create_parquet"
  prefix                    = join(local.default_separator, [local.prefix, "lambda", "create", "parquet"])
  destination_s3_bucket_id  = module.lambda_tests.s3_bucket_id
  destination_s3_bucket_arn = module.lambda_tests.s3_bucket_arn
}