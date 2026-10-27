# JM Comment: Zero-Trust AWS IAM Role for Service Accounts (IRSA) for Ray Pods

# Trust policy restricting role assumption exclusively to the specific K8s Service Account
data "aws_iam_policy_document" "irsa_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    effect  = "Allow"

    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider_url}:sub"
      values = [
        "system:serviceaccount:${var.k8s_namespace}:${var.service_account_name}",
        "system:serviceaccount:${var.k8s_namespace}:ray-head-sa"
      ]
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

resource "aws_iam_role" "ray_irsa_role" {
  name               = "${var.cluster_name}-ray-pod-irsa-role"
  assume_role_policy = data.aws_iam_policy_document.irsa_trust.json

  tags = {
    Name    = "${var.cluster_name}-ray-pod-role"
    Purpose = "Ray Zero-Trust Pod Access to S3 KMS and Glue Iceberg"
  }
}

# Least-privilege IAM Policy
resource "aws_iam_policy" "ray_scoped_policy" {
  name        = "${var.cluster_name}-ray-scoped-policy"
  description = "Scoped Zero-Trust access for Ray Pods to S3 bucket, KMS, and Glue Catalog"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # 1. Scoped S3 permissions for data ingestion, Iceberg warehouse, and object spilling
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          var.s3_bucket_arn,
          "${var.s3_bucket_arn}/*",
          "arn:aws:s3:::distributed-ai-*",
          "arn:aws:s3:::distributed-ai-*/*"
        ]
      },
      # 2. KMS Decrypt / GenerateDataKey for SSE-KMS encryption
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = [var.kms_key_arn]
      },
      # 3. AWS Glue Catalog permissions for Iceberg transactional metadata commits
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartitions",
          "glue:BatchCreatePartition"
        ]
        Resource = [
          "arn:aws:glue:*:*:catalog",
          "arn:aws:glue:*:*:database/${var.glue_database_name}",
          "arn:aws:glue:*:*:table/${var.glue_database_name}/*"
        ]
      },
      # 4. Centralized CloudWatch logging
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = ["arn:aws:logs:*:*:log-group:/aws/ray/*"]
      },
      # 5. JM Comment: AWS Secrets Manager permissions for Secrets Store CSI Driver
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [
          "arn:aws:secretsmanager:*:*:secret:distributed-ai/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_scoped" {
  role       = aws_iam_role.ray_irsa_role.name
  policy_arn = aws_iam_policy.ray_scoped_policy.arn
}
