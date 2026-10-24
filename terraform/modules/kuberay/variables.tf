# JM Comment: KubeRay Helm Module Variables
variable "kuberay_version" {
  description = "KubeRay Operator Helm chart version"
  type        = string
  default     = "1.1.1"
}

variable "namespace" {
  description = "Namespace for KubeRay Operator"
  type        = string
  default     = "kuberay-system"
}
