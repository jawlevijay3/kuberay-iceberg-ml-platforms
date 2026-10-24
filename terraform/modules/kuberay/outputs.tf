# JM Comment: KubeRay Outputs
output "release_name" {
  value = helm_release.kuberay_operator.name
}

output "chart_version" {
  value = helm_release.kuberay_operator.version
}
