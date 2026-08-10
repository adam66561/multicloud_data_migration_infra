resource "aws_glue_catalog_database" "pasx" {
  name = "pasx"
}

resource "aws_glue_catalog_database" "wltuser" {
  name = "wltuser"
}

resource "aws_iam_role" "crawler" {
  name = "${var.name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "glue.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "crawler" {
  name = "${var.name}-policy"
  role = aws_iam_role.crawler.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]

        Resource = [
          var.s3_bucket_arn,
          "${var.s3_bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"

        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateTable",
          "glue:GetTable",
          "glue:GetTables",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition"
        ]

        Resource = "*"
      }
    ]
  })
}

resource "aws_glue_crawler" "pasx" {
  name          = "${var.name}-pasx"
  role          = aws_iam_role.crawler.arn
  database_name = aws_glue_catalog_database.pasx.name

  s3_target {
    path = "s3://dev-multicloud-lambda-tests/pasx/batchrecord/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_crawler" "wltuser" {
  name          = "${var.name}-wltuser"
  role          = aws_iam_role.crawler.arn
  database_name = aws_glue_catalog_database.wltuser.name

  s3_target {
    path = "s3://dev-multicloud-lambda-tests/wltuser/lsvcharge/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}