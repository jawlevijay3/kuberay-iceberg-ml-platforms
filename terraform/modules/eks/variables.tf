# JM Comment: EKS Module Variables
variable "cluster_name" {
  description = "EKS Cluster Name"
  type        = string
  default     = "distributed-ai-eks"
}

variable "cluster_version" {
  description = "Kubernetes Version for EKS"
  type        = string
  default     = "1.34"
}

variable "vpc_id" {
  description = "VPC ID where EKS is created"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for the EKS control plane"
  type        = list(string)
}

variable "environment" {
  description = "Environment identifier"
  type        = string
  default     = "prod"
}
