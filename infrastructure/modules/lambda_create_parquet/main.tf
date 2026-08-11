terraform {
  required_version = ">= 1.10"

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.7"
    }

    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.12"
    }
  }
}

locals {
    default_separator = "-"
    account_id        = data.aws_caller_identity.this.account_id
    region            = data.aws_region.this.region
    partition = data.aws_partition.this.partition
}

data "aws_caller_identity" "this" {}
data "aws_region" "this" {}
data "aws_partition" "this" {}
data "aws_iam_policy" "AWSLambdaBasicExecutionRole" {arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"}

data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/src"
  output_path = "${path.module}/lambda_function.zip"
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

  }
}

resource "aws_iam_role" "this" {
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "this" {
  statement {
    sid = "AllowResourcesAccess"
    effect = "Allow"
    actions = [
      "s3:*Object",
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      "arn:${local.partition}:s3:::*",
      "arn:${local.partition}:s3:::*/*",
    ]
  }
}

resource "aws_iam_policy" "this" {
  policy = data.aws_iam_policy_document.this.json
}

resource "aws_iam_role_policy_attachment" "this" {
  role       = aws_iam_role.this.name
  policy_arn = aws_iam_policy.this.arn
}

resource "aws_lambda_function" "this" {
  function_name = join(local.default_separator, [var.prefix, "create", "parquet"])
  runtime = var.lambda_runtime
  handler = "lambda_function.lambda_handler"
  memory_size = var.memory_size
  timeout     = var.timeout
  package_type = "Zip"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  role    = aws_iam_role.this.arn

  layers = [ "arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python312:31" ]

  environment {
    variables = {
      S3_BUCKET  = var.destination_s3_bucket_id
    }
  }

}

resource "aws_cloudwatch_log_group" "this" {
  name = join("/", ["", "aws", "lambda", aws_lambda_function.this.function_name])
  log_group_class = "STANDARD"
  retention_in_days = 1
  skip_destroy = false
}