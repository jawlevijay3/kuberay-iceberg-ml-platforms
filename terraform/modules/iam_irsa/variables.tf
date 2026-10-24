# JM Comment: IAM IRSA Module Variables
variable "cluster_name" {
  type = string
}

variable "oidc_provider_arn" {
  type = string
}

variable "oidc_provider_url" {
  type = string
}

variable "s3_bucket_arn" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "glue_database_name" {
  type    = string
  default = "corporate_iceberg_db"
}

variable "k8s_namespace" {
  type    = string
  default = "kuberay"
}

variable "service_account_name" {
  type    = string
  default = "ray-worker-sa"
}
