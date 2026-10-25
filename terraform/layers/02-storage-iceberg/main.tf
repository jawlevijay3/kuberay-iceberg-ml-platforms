# JM Comment: Decoupled Layer 02: Storage & Apache Iceberg with S3 Remote State
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "s3" {
    bucket  = "odex-terraform-state-file"
    key     = "layers/02-storage-iceberg/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

provider "aws" {
  region = var.aws_region
}

module "s3_iceberg" {
  source             = "../../modules/s3_iceberg"
  bucket_name_prefix = "distributed-ai"
  environment        = "prod"
}

output "bucket_name" {
  value = module.s3_iceberg.bucket_name
}

output "bucket_arn" {
  value = module.s3_iceberg.bucket_arn
}

output "kms_key_arn" {
  value = module.s3_iceberg.kms_key_arn
}

output "glue_database_name" {
  value = module.s3_iceberg.glue_database_name
}
