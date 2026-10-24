# JM Comment: KubeRay Operator Deployment via Helm
resource "helm_release" "kuberay_operator" {
  name             = "kuberay-operator"
  repository       = "https://ray-project.github.io/kuberay-helm/"
  chart            = "kuberay-operator"
  version          = var.kuberay_version
  namespace        = var.namespace
  create_namespace = true

  set {
    name  = "watchNamespace"
    value = "" # Watch all namespaces
  }

  set {
    name  = "logging.level"
    value = "info"
  }
}
