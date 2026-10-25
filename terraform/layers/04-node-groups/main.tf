# JM Comment: Decoupled Layer 04: Head (On-Demand), Spot Workers, and Bottlerocket Resolver Node Groups with S3 Remote State
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
  }

  backend "s3" {
    bucket  = "odex-terraform-state-file"
    key     = "layers/04-node-groups/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
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

variable "subnet_ids" {
  type    = list(string)
  default = []
}

provider "aws" {
  region = var.aws_region
}

# Auto-discover private subnets if not passed
data "aws_subnets" "subnets" {
  count = length(var.subnet_ids) == 0 ? 1 : 0
  filter {
    name   = "tag:kubernetes.io/cluster/${var.cluster_name}"
    values = ["shared", "owned"]
  }
  tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }
}

locals {
  target_subnets = length(var.subnet_ids) > 0 ? var.subnet_ids : data.aws_subnets.subnets[0].ids
}

module "node_pools" {
  source       = "../../modules/node_pools"
  cluster_name = var.cluster_name
  subnet_ids   = local.target_subnets
  environment  = "prod"
}

output "head_node_group_arn" {
  value = module.node_pools.head_node_group_arn
}

output "workers_node_group_arn" {
  value = module.node_pools.workers_node_group_arn
}

output "resolver_bottlerocket_node_group_arn" {
  value = module.node_pools.resolver_bottlerocket_node_group_arn
}
