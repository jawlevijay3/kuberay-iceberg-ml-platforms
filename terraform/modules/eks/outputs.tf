# JM Comment: EKS Module Outputs
output "cluster_id" {
  value = aws_eks_cluster.eks.id
}

output "cluster_name" {
  value = aws_eks_cluster.eks.name
}

output "cluster_endpoint" {
  value = aws_eks_cluster.eks.endpoint
}

output "cluster_certificate_authority_data" {
  value = aws_eks_cluster.eks.certificate_authority[0].data
}

output "oidc_provider_arn" {
  value = aws_iam_openid_connect_provider.oidc.arn
}

output "oidc_provider_url" {
  value = replace(aws_eks_cluster.eks.identity[0].oidc[0].issuer, "https://", "")
}
