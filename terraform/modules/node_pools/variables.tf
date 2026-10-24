# JM Comment: Node Pools Module Variables
variable "cluster_name" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "environment" {
  type    = string
  default = "prod"
}
