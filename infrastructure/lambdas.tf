module "lambda_create_parquet" {
  source                    = "./modules/lambda_create_parquet"
  prefix                    = join(local.default_separator, [local.prefix, "lambda", "create", "parquet"])
  destination_s3_bucket_id  = module.lambda_tests.s3_bucket_id
  destination_s3_bucket_arn = module.lambda_tests.s3_bucket_arn
}

module "lambda_convert_delta" {
  source = "./modules/lambda_convert_delta"
  prefix = join(local.default_separator, [local.prefix, "lambda", "convert", "delta"])

  glue_database_name = ["pasx", "wltuser"]
  glue_catalog_id    = data.aws_caller_identity.current.account_id

  destination_s3_bucket_id  = module.lambda_tests.s3_bucket_id
  destination_s3_bucket_arn = module.lambda_tests.s3_bucket_arn
}

module "lambda_merge_delta" {
  source = "./modules/lambda_merge_delta"
  prefix = join(local.default_separator, [local.prefix, "test"])

  s3_prefixes_per_schema = {
    schema1 = {
      source = "schema1_cdc"
      target = "schema1_target"
    }
    schema2 = {
      source = "schema2_cdc"
      target = "schema2_target"
    }
  }

  source_s3_bucket_id = module.lambda_tests.s3_bucket_id
  target_s3_bucket_id = module.lambda_tests.s3_bucket_id

  config_s3_bucket_id = module.lambda_tests.s3_bucket_id
  config_key          = aws_s3_object.lambda_tests_config.key
  s3_objects          = distinct(sort(compact([for line in split("\n", file("./sources.txt")) : trimspace(line)])))

  audit_logs                     = true
  date_partition_subfolder_count = 1
}

# module "lambda_merge_delta_wp" {
#   source = "./modules/lambda_merge_delta_wp"
#   prefix = join(local.default_separator, [local.prefix, "lambda", "merge", "delta", "wp"])

#   glue_database_name = ["pasx", "wltuser"]
#   glue_catalog_id    = data.aws_caller_identity.current.account_id

#   destination_s3_bucket_id  = module.lambda_tests.s3_bucket_id
#   destination_s3_bucket_arn = module.lambda_tests.s3_bucket_arn

#   config_s3_bucket_id  = module.lambda_tests.s3_bucket_id
#   config_s3_bucket_arn = module.lambda_tests.s3_bucket_arn
# }