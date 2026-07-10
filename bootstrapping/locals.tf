locals {
  default_separator = "-"
  account_id        = data.aws_caller_identity.current.account_id
  region            = data.aws_region.current.region
  prefix            = join(local.default_separator, [var.env, var.project])

  common_tags = {
    env        = var.env
    project    = var.project
    repository = var.repositories[0]
  }
}