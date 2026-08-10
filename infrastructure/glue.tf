module "glue_crawler" {
  source = "./modules/glue_crawler"

  name          = "mock-data-crawler"
  database_name = "mock_data"

  s3_bucket_arn = module.lambda_tests.s3_bucket_arn
  s3_path       = "s3://${module.lambda_tests.s3_bucket_name}/"
}