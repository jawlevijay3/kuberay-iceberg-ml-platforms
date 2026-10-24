# JM Comment: S3 and Iceberg Module Variables
variable "bucket_name_prefix" {
  type    = string
  default = "distributed-ai"
}

variable "environment" {
  type    = string
  default = "prod"
}
