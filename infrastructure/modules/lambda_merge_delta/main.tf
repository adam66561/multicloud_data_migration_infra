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
  default_desc      = "Managed by Terraform"
  account_id        = data.aws_caller_identity.this.account_id
  partition         = data.aws_partition.current.partition
  region            = data.aws_region.this.region
}

data "aws_caller_identity" "this" {}
data "aws_region" "this" {}
data "aws_partition" "current" {}
data "aws_iam_policy" "AWSLambdaBasicExecutionRole" { arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" }

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

data "aws_iam_policy_document" "this" {
  statement {
    effect = "Allow"
    actions = [
      "s3:*Object",
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:DescribeTable",
      "dynamodb:Query",
      "glue:GetDatabase",
      "glue:GetTables",
      "dms:DescribeReplicationTasks",
      "dms:DescribeTableStatistics"
    ]
    resources = [
      "arn:${local.partition}:glue:${local.region}:${local.account_id}:catalog",
      "arn:${local.partition}:glue:${local.region}:${local.account_id}:database/*",
      "arn:${local.partition}:glue:${local.region}:${local.account_id}:table/*/*",
      "arn:${local.partition}:s3:::*",
      "arn:${local.partition}:s3:::*/*",
      "${aws_dynamodb_table.delta_lock.arn}"
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

resource "aws_iam_role_policy_attachment" "AWSLambdaBasicExecutionRole" {
  role       = aws_iam_role.this.name
  policy_arn = data.aws_iam_policy.AWSLambdaBasicExecutionRole.arn
}

resource "aws_ecr_repository" "this" {
  name                 = join(local.default_separator, [var.prefix, "lambda"])
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "this" {
  repository = aws_ecr_repository.this.name
  policy = jsonencode({
    "rules" = [{
      "rulePriority" = 1,
      "description"  = "Keep only one image, expire all others",
      "selection" = {
        "tagStatus"   = "any",
        "countType"   = "imageCountMoreThan",
        "countNumber" = 1
      },
      "action" = {
        "type" = "expire"
      }
    }] }
  )
}

data "aws_ecr_image" "latest" {
  repository_name = aws_ecr_repository.this.name
  image_tag       = "latest"
}

resource "aws_lambda_function" "this" {
  publish          = true
  description      = local.default_desc
  function_name    = var.prefix
  memory_size      = var.lambda_memory_size
  timeout          = var.lambda_timeout
  role             = aws_iam_role.this.arn
  package_type = "Image"
  image_uri    = "${aws_ecr_repository.this.repository_url}:latest"

  environment {
    variables = {
      S3_CONFIG_BUCKET                    = var.config_s3_bucket_id
      S3_CONFIG_KEY                       = var.config_key
      S3_SOURCE_BUCKET                    = var.source_s3_bucket_id
      S3_TARGET_BUCKET                    = var.target_s3_bucket_id

      AWS_S3_LOCKING_PROVIDER         = "dynamodb"
      DYNAMO_LOCK_PARTITION_KEY_VALUE = "key"
      DYNAMO_LOCK_TABLE_NAME          = aws_dynamodb_table.delta_lock.name
      
      # GLUE_CATALOG_ID            = coalesce(var.glue_catalog_id, local.account_id)
      # GLUE_DATABASE_NAME         = join(",", var.glue_database_name)
      # TASK_ARN = var.dms_task_arn
    }
  }

  lifecycle {
    ignore_changes = [source_code_hash]
  }
}

resource "aws_cloudwatch_log_group" "this" {
  name              = join("/", ["", "aws", "lambda", aws_lambda_function.this.function_name])
  log_group_class   = "STANDARD"
  retention_in_days = var.logs_retention_in_days
  skip_destroy      = false
}

resource "aws_lambda_permission" "s3_invoke" {
  statement_id  = "AllowS3InvokeBootstrapLambda"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.source_s3_bucket_id}"
}

resource "aws_s3_bucket_notification" "load_parquet_created" {
  bucket = var.source_s3_bucket_id

  dynamic "lambda_function" {
    for_each = {
      for v in ["pasx.batchrecord", "wltuser.lsvcharge"] : v => {
        load_prefix = "cdc/${lower(element(split(".", v), 0))}/${lower(element(split(".", v), 1))}/"
      }
    } 

    content {
      lambda_function_arn = aws_lambda_function.this.arn
      events              = ["s3:ObjectCreated:*"]

      filter_prefix = lambda_function.value.load_prefix
      filter_suffix = ".parquet"
    }
  }

  depends_on = [aws_lambda_permission.s3_invoke]
}

# resource "aws_dynamodb_table" "processed_files" {
#   name         = join(local.default_separator, [var.prefix, "cdc", "processed", "files"])
#   billing_mode = "PAY_PER_REQUEST"
  
#   attribute { 
#     name = "key" 
#     type = "S" 
#   }
#   hash_key     = "key"
# }

resource "aws_dynamodb_table" "delta_lock" {
  name         = join(local.default_separator, [var.prefix, "delta", "lock"])
  billing_mode = "PAY_PER_REQUEST"
  attribute {
    name = "key"
    type = "S"
  }
  hash_key = "key"

  tags = {
    Name = join(local.default_separator, ["bln", "prod", "lock", "deltatable"])
  }
}