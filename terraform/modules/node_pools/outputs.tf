# JM Comment: Node Pools Outputs
output "head_node_group_arn" {
  value = aws_eks_node_group.ray_head_system.arn
}

output "workers_node_group_arn" {
  value = aws_eks_node_group.ray_workers_spot.arn
}
