terraform {
  required_version = ">= 1.6"

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.7"
    }

    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0"
    }
  }
}

locals {
  default_separator = "-"
  default_desc      = "Managed by Terraform"
  account_id        = data.aws_caller_identity.this.account_id
  partition         = data.aws_partition.current.partition
  region            = data.aws_region.current.region
}

data "aws_caller_identity" "this" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}
data "aws_iam_policy" "AWSLambdaBasicExecutionRole" { arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" }
data "aws_lambda_layer_version" "deltalake" { layer_name = "deltalake" }

data "archive_file" "this" {
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
  description        = local.default_desc
}

resource "aws_iam_role_policy_attachment" "AWSLambdaBasicExecutionRole" {
  role       = aws_iam_role.this.name
  policy_arn = data.aws_iam_policy.AWSLambdaBasicExecutionRole.arn
}

data "aws_iam_policy_document" "this" {
  statement {
    sid    = "AllowResourceAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTables",
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:*Object",
    ]
    resources = [
      "arn:${local.partition}:glue:${local.region}:${local.account_id}:catalog",
      "arn:${local.partition}:glue:${local.region}:${local.account_id}:database/*",
      "arn:${local.partition}:glue:${local.region}:${local.account_id}:table/*/*",
      "arn:${local.partition}:s3:::*",
      "arn:${local.partition}:s3:::*/*",
    ]
  }
}

resource "aws_iam_policy" "this" {
  policy      = data.aws_iam_policy_document.this.json
  description = local.default_desc
}

resource "aws_iam_role_policy_attachment" "this" {
  role       = aws_iam_role.this.name
  policy_arn = aws_iam_policy.this.arn
}

resource "aws_lambda_function" "this" {
  description                    = local.default_desc
  function_name                  = join(local.default_separator, [var.prefix, "convert", "delta"])
  runtime                        = var.lambda_runtime
  handler                        = "lambda_function.lambda_handler"
  memory_size                    = var.lambda_memory_size
  timeout                        = var.lambda_timeout
  package_type                   = "Zip"
  filename                       = data.archive_file.this.output_path
  source_code_hash               = data.archive_file.this.output_base64sha256
  role                           = aws_iam_role.this.arn
  layers                         = [data.aws_lambda_layer_version.deltalake.arn]
  reserved_concurrent_executions = 1

  environment {
    variables = {
      GLUE_CATALOG_ID            = coalesce(var.glue_catalog_id, local.account_id)
      GLUE_DATABASE_NAME         = join(",", var.glue_database_name)
      AWS_S3_ALLOW_UNSAFE_RENAME = true
    }
  }
}

resource "aws_cloudwatch_log_group" "this" {
  name              = join("/", ["", "aws", "lambda", aws_lambda_function.this.function_name])
  log_group_class   = "STANDARD"
  retention_in_days = var.logs_retention_in_days
  skip_destroy      = false
}

