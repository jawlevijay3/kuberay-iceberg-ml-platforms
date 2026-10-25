# JM Comment: Decoupled Multi-Layer Production Terraform Architecture for Ray AI Foundation
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "Distributed-AI-Foundation"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# 1. Multi-AZ Decoupled Networking Layer
module "vpc" {
  source       = "../../modules/vpc"
  cluster_name = var.cluster_name
  vpc_cidr     = var.vpc_cidr
  azs          = var.azs
  environment  = var.environment
}

# 2. Managed Kubernetes EKS Layer (Control Plane v1.34 + OIDC Provider)
module "eks" {
  source          = "../../modules/eks"
  cluster_name    = var.cluster_name
  cluster_version = "1.34"
  vpc_id          = module.vpc.vpc_id
  subnet_ids      = module.vpc.private_subnets
  environment     = var.environment
}

# Configure Kubernetes and Helm providers
data "aws_eks_cluster_auth" "cluster" {
  name = module.eks.cluster_name
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  token                  = data.aws_eks_cluster_auth.cluster.token
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    token                  = data.aws_eks_cluster_auth.cluster.token
  }
}

# 3. Decoupled Node Pools: Head (On-Demand), Spot Workers, and Bottlerocket Resolver (2 CPU 16GB)
module "node_pools" {
  source       = "../../modules/node_pools"
  cluster_name = module.eks.cluster_name
  subnet_ids   = module.vpc.private_subnets
  environment  = var.environment
  depends_on   = [module.eks]
}

# 4. Storage & Apache Iceberg Layer (S3, KMS, AWS Glue Catalog)
module "s3_iceberg" {
  source             = "../../modules/s3_iceberg"
  bucket_name_prefix = "distributed-ai"
  environment        = var.environment
}

# 5. Zero-Trust Identity Layer (AWS IRSA for Ray Pods)
module "iam_irsa" {
  source               = "../../modules/iam_irsa"
  cluster_name         = module.eks.cluster_name
  oidc_provider_arn    = module.eks.oidc_provider_arn
  oidc_provider_url    = module.eks.oidc_provider_url
  s3_bucket_arn        = module.s3_iceberg.bucket_arn
  kms_key_arn          = module.s3_iceberg.kms_key_arn
  glue_database_name   = module.s3_iceberg.glue_database_name
  k8s_namespace        = "kuberay"
  service_account_name = "ray-worker-sa"
}

# 6. KubeRay Operator and Gateway API Controller Layer
module "kuberay_gateway" {
  source     = "../../modules/kuberay"
  depends_on = [module.node_pools]
}
