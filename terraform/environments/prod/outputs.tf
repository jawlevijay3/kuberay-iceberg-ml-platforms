# JM Comment: Production Root Outputs
output "eks_cluster_name" {
  description = "EKS Cluster Name"
  value       = module.eks.cluster_name
}

output "eks_cluster_endpoint" {
  description = "EKS API Server Endpoint"
  value       = module.eks.cluster_endpoint
}

output "s3_data_bucket" {
  description = "S3 bucket for datasets, Iceberg warehouse, and Ray spills"
  value       = module.s3_iceberg.bucket_name
}

output "iceberg_glue_database" {
  description = "AWS Glue Catalog Database for Iceberg"
  value       = module.s3_iceberg.glue_database_name
}

output "ray_irsa_role_arn" {
  description = "IAM Role ARN to attach to K8s Service Account for Zero-Trust pod authentication"
  value       = module.iam_irsa.ray_role_arn
}

output "update_kubeconfig_command" {
  description = "AWS CLI command to connect kubectl to the newly provisioned cluster"
  value       = "aws eks --region ${var.aws_region} update-kubeconfig --name ${module.eks.cluster_name}"
}
