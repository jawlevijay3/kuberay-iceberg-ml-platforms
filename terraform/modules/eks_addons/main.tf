# JM Comment: Post-NodeGroup EKS Managed Add-ons (CoreDNS & EBS CSI Driver)
# CRITICAL: These addons run as compute pods and MUST only be provisioned AFTER
# worker nodes are Ready to prevent 15-minute pending/hang timeouts.

variable "cluster_name" {
  description = "EKS Cluster Name"
  type        = string
}

variable "oidc_provider_arn" {
  description = "OIDC Provider ARN for EBS CSI IRSA role"
  type        = string
}

variable "oidc_provider_url" {
  description = "OIDC Provider URL for EBS CSI IRSA role"
  type        = string
}

# 1. IAM Role for EBS CSI Driver
data "aws_iam_policy_document" "ebs_csi_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    effect  = "Allow"

    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider_url}:sub"
      values   = ["system:serviceaccount:kube-system:ebs-csi-controller-sa"]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider_url}:aud"
      values   = ["sts.amazonaws.com"]
    }

    principals {
      identifiers = [var.oidc_provider_arn]
      type        = "Federated"
    }
  }
}

resource "aws_iam_role" "ebs_csi_role" {
  name               = "${var.cluster_name}-ebs-csi-role"
  assume_role_policy = data.aws_iam_policy_document.ebs_csi_trust.json
}

resource "aws_iam_role_policy_attachment" "ebs_csi_policy" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
  role       = aws_iam_role.ebs_csi_role.name
}

# 2. CoreDNS Add-on (Schedules onto running worker nodes immediately)
resource "aws_eks_addon" "coredns" {
  cluster_name                = var.cluster_name
  addon_name                  = "coredns"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "PRESERVE"
}

# 3. AWS EBS CSI Driver Add-on (Enables persistent volume claims for Iceberg & Ray)
resource "aws_eks_addon" "aws_ebs_csi_driver" {
  cluster_name                = var.cluster_name
  addon_name                  = "aws-ebs-csi-driver"
  service_account_role_arn    = aws_iam_role.ebs_csi_role.arn
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "PRESERVE"

  depends_on = [
    aws_iam_role_policy_attachment.ebs_csi_policy,
    aws_eks_addon.coredns
  ]
}

output "ebs_csi_role_arn" {
  value = aws_iam_role.ebs_csi_role.arn
}

output "coredns_addon_arn" {
  value = aws_eks_addon.coredns.arn
}

output "ebs_csi_addon_arn" {
  value = aws_eks_addon.aws_ebs_csi_driver.arn
}
