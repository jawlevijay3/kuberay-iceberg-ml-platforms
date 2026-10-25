# JM Comment: KubeRay Operator, Gateway API Controller, and Secrets Store CSI Driver Helm Releases
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

  set {
    name  = "deployment.replicas"
    value = "1"
  }
}

# 3. JM Comment: Kubernetes Secrets Store CSI Driver for Zero-Trust AWS Secrets Manager mounting
resource "helm_release" "secrets_store_csi_driver" {
  name             = "secrets-store-csi-driver"
  repository       = "https://kubernetes-sigs.github.io/secrets-store-csi-driver/charts"
  chart            = "secrets-store-csi-driver"
  version          = "1.4.3"
  namespace        = "kube-system"

  set {
    name  = "syncSecret.enabled"
    value = "true"
  }

  set {
    name  = "enableSecretRotation"
    value = "true"
  }
}

# 4. JM Comment: AWS Secrets and Configuration Provider (ASCP) DaemonSet
resource "helm_release" "aws_secrets_provider" {
  name       = "secrets-store-csi-driver-provider-aws"
  repository = "https://aws.github.io/secrets-store-csi-driver-provider-aws"
  chart      = "secrets-store-csi-driver-provider-aws"
  version    = "1.0.9"
  namespace  = "kube-system"

  depends_on = [helm_release.secrets_store_csi_driver]
}
