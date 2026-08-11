terraform {
  backend "s3" {
    bucket       = "tfstate-s3-dev-multicloud-681053994560-eu-central-1"
    key          = "base/terraform.tfstate"
    region       = "eu-central-1"
    encrypt      = true
    use_lockfile = true
  }
  required_version = ">= 1.15.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0"
    }
  }
}

provider "aws" {
  region = var.region
}