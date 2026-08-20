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

  cdc_tables = toset(["pasx.batchrecord", "wltuser.lsvcharge"])
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
      "glue:GetDatabase",
      "glue:GetTables",
      "dms:DescribeReplicationTasks",
      "dms:DescribeTableStatistics",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:DescribeTable",
      "dynamodb:Query",
    ]
    resources = [
      "arn:${local.partition}:glue:${local.region}:${local.account_id}:catalog",
      "arn:${local.partition}:glue:${local.region}:${local.account_id}:database/*",
      "arn:${local.partition}:glue:${local.region}:${local.account_id}:table/*/*",
      "arn:${local.partition}:s3:::*",
      "arn:${local.partition}:s3:::*/*",
      "arn:${local.partition}:sqs:${local.region}:${local.account_id}:*",
      "arn:${local.partition}:dynamodb:${local.region}:${local.account_id}:table/*",
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
  image_uri = "${aws_ecr_repository.this.repository_url}@${data.aws_ecr_image.latest.image_digest}"

  environment {
    variables = {
      S3_CONFIG_BUCKET                    = var.config_s3_bucket_id
      S3_CONFIG_KEY                       = var.config_key
      S3_SOURCE_BUCKET                    = var.source_s3_bucket_id
      S3_SOURCE_CDC_PATH                  = var.source_cdc_path
      S3_TARGET_BUCKET                    = var.target_s3_bucket_id
      S3_TARGET_PATH                      = var.target_path
      EVENT_TYPE                          = var.type_of_event
      AUDIT_LOGS                          = var.audit_logs ? "true" : "false"
      AUDIT_TABLE_NAME                    = var.audit_logs ? aws_dynamodb_table.merge_audit[0].name : ""
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

  eventbridge = var.type_of_event == "fifo"

  dynamic "lambda_function" {
    for_each = var.type_of_event == "s3" ? {
      for v in local.cdc_tables : v => {
        load_prefix = "${var.source_cdc_path}/${lower(element(split(".", v), 0))}/${lower(element(split(".", v), 1))}/"
      }
    } : {}

    content {
      lambda_function_arn = aws_lambda_function.this.arn
      events              = ["s3:ObjectCreated:*"]

      filter_prefix = lambda_function.value.load_prefix
      filter_suffix = ".parquet"
    }
  }

  depends_on = [aws_lambda_permission.s3_invoke]
}

resource "aws_sqs_queue" "this" {
  count = var.type_of_event == "fifo" ? 1 : 0

  name       = "${join(local.default_separator, [var.prefix, "fifo"])}.fifo"
  fifo_queue = true

  visibility_timeout_seconds = 300
  message_retention_seconds  = 86400
  receive_wait_time_seconds  = 20

  content_based_deduplication = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq[count.index].arn
    maxReceiveCount     = 10
  })
}

resource "aws_sqs_queue" "dlq" {
  count = var.type_of_event == "fifo" ? 1 : 0
  name       = "${join(local.default_separator, [var.prefix, "dlq"])}.fifo"
  fifo_queue = true
}

resource "aws_cloudwatch_event_rule" "cdc_parquet_created" {
  for_each = var.type_of_event == "fifo" ? local.cdc_tables : toset([])
  name = join(local.default_separator, [var.prefix, replace(each.key, ".", "-")])

  event_pattern = jsonencode({
    source = [ "aws.s3" ]
    detail-type = [ "Object Created" ]
    detail = {
      bucket = { name = [var.source_s3_bucket_id] }
      object = { key = [{ wildcard = "${var.source_cdc_path}/${lower(element(split(".", each.value), 0))}/${lower(element(split(".", each.value), 1))}/*.parquet" }] }
    }
  })
}

resource "aws_cloudwatch_event_target" "sqs" {
  for_each = var.type_of_event == "fifo" ? local.cdc_tables : toset([])

  rule = aws_cloudwatch_event_rule.cdc_parquet_created[each.key].name
  arn  = aws_sqs_queue.this[0].arn

  sqs_target {
    message_group_id = each.key
  }
}

data "aws_iam_policy_document" "sqs_eventbridge" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    actions = [
      "sqs:SendMessage"
    ]

    resources = [
      aws_sqs_queue.this[0].arn
    ]

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"

      values = [
        for rule in aws_cloudwatch_event_rule.cdc_parquet_created :
        rule.arn
      ]
    }
  }
}

resource "aws_sqs_queue_policy" "eventbridge" {
  count = var.type_of_event == "fifo" ? 1 : 0

  queue_url = aws_sqs_queue.this[0].id
  policy    = data.aws_iam_policy_document.sqs_eventbridge.json
}

resource "aws_lambda_event_source_mapping" "sqs" {
  count = var.type_of_event == "fifo" ? 1 : 0

  event_source_arn = aws_sqs_queue.this[0].arn
  function_name    = aws_lambda_function.this.arn

  batch_size = 1
  enabled    = true

  depends_on = [
    aws_iam_role_policy_attachment.this
  ]
}

resource "aws_dynamodb_table" "merge_audit" {
  count = var.audit_logs ? 1 : 0
  name         = join(local.default_separator, [var.prefix, "merge", "audit"])
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "file_id"
  
  attribute { 
    name = "file_id" 
    type = "S" 
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}