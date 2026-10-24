# JM Comment: VPC Module for EKS Ray Cluster with multi-AZ high availability
variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "cluster_name" {
  description = "EKS Cluster Name for subnet discovery tags"
  type        = string
  default     = "distributed-ai-eks"
}

variable "environment" {
  description = "Environment identifier"
  type        = string
  default     = "prod"
}

variable "azs" {
  description = "Availability zones"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}
