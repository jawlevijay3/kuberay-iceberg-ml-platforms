# JM Comment: Terraform Version and Provider Locks with S3 Remote State
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.26"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.12"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  # JM Comment: Remote S3 backend with DynamoDB state locking to prevent overrides
  backend "s3" {
    bucket         = "odex-terraform-state-file"
    key            = "prod/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "odex-terraform-state-lock"
    encrypt        = true
  }
}
