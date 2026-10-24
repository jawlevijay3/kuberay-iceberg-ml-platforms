# JM Comment: IAM IRSA Outputs
output "ray_role_arn" {
  description = "IAM Role ARN to annotate on the K8s ServiceAccount"
  value       = aws_iam_role.ray_irsa_role.arn
}

output "ray_role_name" {
  value = aws_iam_role.ray_irsa_role.name
}
