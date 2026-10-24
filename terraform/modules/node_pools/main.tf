# JM Comment: Dedicated EKS Node Groups for Ray Head (On-Demand) and Ray Workers (Spot)

# IAM Role for EKS Node Groups
resource "aws_iam_role" "nodes" {
  name = "${var.cluster_name}-node-group-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "node_amazon_eks_worker_node_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.nodes.name
}

resource "aws_iam_role_policy_attachment" "node_amazon_eks_cni_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.nodes.name
}

resource "aws_iam_role_policy_attachment" "node_amazon_ec2_container_registry_read_only" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.nodes.name
}

# -------------------------------------------------------------------------
# Node Pool 1: Ray Head & System Services (Reliable On-Demand Instances)
# -------------------------------------------------------------------------
# JM Comment: Head node hosts GCS (Global Control Store), Dashboard, and API Server.
# Must remain stable and never run on Spot to avoid cluster-wide state loss.
resource "aws_eks_node_group" "ray_head_system" {
  cluster_name    = var.cluster_name
  node_group_name = "${var.cluster_name}-head-system-ondemand"
  node_role_arn   = aws_iam_role.nodes.arn
  subnet_ids      = var.subnet_ids
  capacity_type   = "ON_DEMAND"
  instance_types  = ["m5.xlarge"]

  scaling_config {
    desired_size = 1
    max_size     = 3
    min_size     = 1
  }

  update_config {
    max_unavailable = 1
  }

  labels = {
    "ray-node-type" = "head"
    "workload"      = "system"
  }

  tags = {
    "Name"                                          = "${var.cluster_name}-head-system"
    "k8s.io/cluster-autoscaler/enabled"             = "true"
    "k8s.io/cluster-autoscaler/${var.cluster_name}" = "owned"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_amazon_eks_worker_node_policy,
    aws_iam_role_policy_attachment.node_amazon_eks_cni_policy,
    aws_iam_role_policy_attachment.node_amazon_ec2_container_registry_read_only,
  ]
}

# -------------------------------------------------------------------------
# Node Pool 2: Ray Workers (Cost-Optimized Auto-scaling Spot Instances)
# -------------------------------------------------------------------------
# JM Comment: Spot worker pool with instance diversification (m5, m5a, c5, r5)
# to minimize simultaneous reclamation risks from AWS Spot capacity pools.
resource "aws_eks_node_group" "ray_workers_spot" {
  cluster_name    = var.cluster_name
  node_group_name = "${var.cluster_name}-workers-spot"
  node_role_arn   = aws_iam_role.nodes.arn
  subnet_ids      = var.subnet_ids
  capacity_type   = "SPOT"

  # Diversified instance types for Spot resilience
  instance_types = [
    "m5.xlarge",  # 4 vCPU, 16 GiB
    "m5a.xlarge", # AMD alternative
    "c5.xlarge",  # Compute-optimized fallback
    "r5.xlarge",  # Memory-optimized fallback
    "m4.xlarge"   # Previous gen fallback
  ]

  scaling_config {
    desired_size = 2
    max_size     = 10
    min_size     = 0
  }

  update_config {
    max_unavailable_percentage = 30
  }

  labels = {
    "ray-node-type"    = "worker"
    "ray.io/node-type" = "worker"
    "actor_node"       = "true"
    "lifecycle"        = "Ec2Spot"
  }

  taint {
    key    = "dedicated"
    value  = "ray-worker"
    effect = "NO_SCHEDULE"
  }

  tags = {
    "Name"                                          = "${var.cluster_name}-worker-spot"
    "k8s.io/cluster-autoscaler/enabled"             = "true"
    "k8s.io/cluster-autoscaler/${var.cluster_name}" = "owned"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_amazon_eks_worker_node_policy,
    aws_iam_role_policy_attachment.node_amazon_eks_cni_policy,
    aws_iam_role_policy_attachment.node_amazon_ec2_container_registry_read_only,
  ]
}
