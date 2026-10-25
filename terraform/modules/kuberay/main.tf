# JM Comment: KubeRay Operator and Gateway API Controller Deployment via Helm

# 1. KubeRay Operator Controller
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

# 2. JM Comment: Modern Kubernetes Gateway API Controller (Envoy Gateway)
resource "helm_release" "gateway_api_controller" {
  name             = "envoy-gateway"
  repository       = "oci://docker.io/envoyproxy"
  chart            = "gateway-helm"
  version          = "v1.1.0"
  namespace        = "envoy-gateway-system"
  create_namespace = true

  # Ensure CRDs are created automatically
  set {
    name  = "deployment.replicas"
    value = "1"
  }
}
