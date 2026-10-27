# JM Comment: Dedicated EKS Node Groups for Ray Head, General Spot Workers, and Bottlerocket Resolver Workers

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

# JM Comment: Grants nodes permission to stream container stdout/stderr to CloudWatch Logs via Fluent Bit
resource "aws_iam_role_policy_attachment" "node_cloudwatch_agent_server_policy" {
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
  role       = aws_iam_role.nodes.name
}

# -------------------------------------------------------------------------
# Node Pool 1: Ray Head & System Services (Reliable On-Demand Instances)
# -------------------------------------------------------------------------
# JM Comment: Head node hosts GCS and API Server. Never placed on Spot.
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
# Node Pool 2: General Ray Workers (Auto-scaling Spot Instances)
# -------------------------------------------------------------------------
# JM Comment: Multi-family Spot diversification for general ingestion & tasks.
resource "aws_eks_node_group" "ray_workers_spot" {
  cluster_name    = var.cluster_name
  node_group_name = "${var.cluster_name}-general-workers-spot"
  node_role_arn   = aws_iam_role.nodes.arn
  subnet_ids      = var.subnet_ids
  capacity_type   = "SPOT"

  instance_types = [
    "m5.xlarge",  # 4 vCPU, 16 GiB Intel
    "m5a.xlarge", # AMD alternative
    "c5.xlarge",  # Compute-optimized fallback
    "m4.xlarge"   # Previous gen fallback
  ]

  scaling_config {
    desired_size = 2
    max_size     = 8
    min_size     = 1
  }

  update_config {
    max_unavailable_percentage = 30
  }

  labels = {
    "ray-node-type" = "worker"
    "workload"      = "general"
    "lifecycle"     = "Ec2Spot"
  }

  taint {
    key    = "dedicated"
    value  = "ray-worker"
    effect = "NO_SCHEDULE"
  }

  tags = {
    "Name"                                          = "${var.cluster_name}-general-worker-spot"
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
# Node Pool 3: Bottlerocket Specialized Resolver Workers (2 CPU, 16 GB RAM)
# -------------------------------------------------------------------------
# JM Comment: Dedicated Bottlerocket AMI node group provisioned specifically for
# the stateful EntityResolver actor requiring 2 CPU and 16 GB RAM configuration.
resource "aws_eks_node_group" "ray_resolver_bottlerocket" {
  cluster_name    = var.cluster_name
  node_group_name = "${var.cluster_name}-resolver-bottlerocket"
  node_role_arn   = aws_iam_role.nodes.arn
  subnet_ids      = var.subnet_ids
  capacity_type   = "ON_DEMAND"
  ami_type        = "BOTTLEROCKET_x86_64"

  # 2 vCPU and 16 GB RAM configuration: r5.large (2 vCPU, 16 GiB RAM) & r6i.large (2 vCPU, 16 GiB RAM)
  instance_types = [
    "r5.large", # 2 vCPU, 16 GiB RAM
    "r6i.large" # 2 vCPU, 16 GiB RAM
  ]

  scaling_config {
    desired_size = 1
    max_size     = 4
    min_size     = 1
  }

  update_config {
    max_unavailable = 1
  }

  labels = {
    "ray-node-type" = "specialized-worker"
    "workload"      = "entity-resolver"
    "os"            = "bottlerocket"
    "specs"         = "2cpu-16gb"
  }

  taint {
    key    = "dedicated"
    value  = "entity-resolver"
    effect = "NO_SCHEDULE"
  }

  tags = {
    "Name"                                          = "${var.cluster_name}-resolver-bottlerocket"
    "k8s.io/cluster-autoscaler/enabled"             = "true"
    "k8s.io/cluster-autoscaler/${var.cluster_name}" = "owned"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_amazon_eks_worker_node_policy,
    aws_iam_role_policy_attachment.node_amazon_eks_cni_policy,
    aws_iam_role_policy_attachment.node_amazon_ec2_container_registry_read_only,
  ]
}
