# JM Comment: VPC Module Outputs
output "vpc_id" {
  description = "The ID of the VPC"
  value       = aws_vpc.main.id
}

output "private_subnets" {
  description = "IDs of the private subnets for EKS and Ray worker nodes"
  value       = aws_subnet.private[*].id
}

output "public_subnets" {
  description = "IDs of the public subnets for ingress/ELBs"
  value       = aws_subnet.public[*].id
}
