# JM Comment: Decoupled Layer 03: Managed EKS Cluster (v1.31) & Zero-Trust IRSA with S3 Remote State
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  backend "s3" {
    bucket         = "odex-terraform-state-file"
    key            = "layers/03-eks-cluster/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "odex-terraform-state-lock"
    encrypt        = true
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "cluster_name" {
  type    = string
  default = "distributed-ai-eks"
}

variable "cluster_version" {
  type        = string
  description = "Kubernetes Version for EKS"
  default     = "1.34"
}

variable "vpc_id" {
  type    = string
  default = ""
}

variable "subnet_ids" {
  type    = list(string)
  default = []
}

provider "aws" {
  region = var.aws_region
}

# If vpc_id not explicitly provided, discover dynamically from tags
data "aws_vpc" "discovered" {
  count = var.vpc_id == "" ? 1 : 0
  tags = {
    Name = "${var.cluster_name}-vpc"
  }
}

data "aws_subnets" "private_discovered" {
  count = length(var.subnet_ids) == 0 ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [var.vpc_id != "" ? var.vpc_id : data.aws_vpc.discovered[0].id]
  }
  tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }
}

locals {
  target_vpc_id     = var.vpc_id != "" ? var.vpc_id : data.aws_vpc.discovered[0].id
  target_subnet_ids = length(var.subnet_ids) > 0 ? var.subnet_ids : data.aws_subnets.private_discovered[0].ids
}

module "eks" {
  source          = "../../modules/eks"
  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version
  vpc_id          = local.target_vpc_id
  subnet_ids      = local.target_subnet_ids
  environment     = "prod"
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "cluster_ca_data" {
  value = module.eks.cluster_certificate_authority_data
}

output "oidc_provider_arn" {
  value = module.eks.oidc_provider_arn
}

output "oidc_provider_url" {
  value = module.eks.oidc_provider_url
}
