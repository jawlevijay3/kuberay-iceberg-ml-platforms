#!/usr/bin/env bash
# JM Comment: Automated End-to-End Deployment Script for Distributed AI Foundation
set -euo pipefail

echo "=========================================================================="
echo "  DISTRIBUTED AI FOUNDATION: AUTOMATED DEPLOYMENT SCRIPT                 "
echo "=========================================================================="

REGION=${AWS_REGION:-"us-east-1"}
CLUSTER_NAME=${CLUSTER_NAME:-"distributed-ai-eks"}
NAMESPACE="kuberay"

echo "[1/5] Validating & Initializing Terraform..."
cd terraform/environments/prod
terraform fmt -check -recursive ../..
terraform init
terraform validate
echo "Terraform validation succeeded."

echo "[2/5] (Optional) Applying Terraform Infrastructure..."
# terraform apply -auto-approve
cd ../../..

echo "[3/5] Updating Local Kubeconfig..."
# aws eks --region "$REGION" update-kubeconfig --name "$CLUSTER_NAME"

echo "[4/5] Applying Kubernetes Resources..."
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-secrets.yaml
kubectl apply -f k8s/02-irsa-serviceaccount.yaml
kubectl apply -f k8s/03-raycluster.yaml
kubectl apply -f k8s/05-prometheus-monitoring.yaml
kubectl apply -f k8s/06-ray-dashboard-ingress.yaml

echo "[5/5] Submitting RayJob to Cluster..."
kubectl apply -f k8s/04-rayjob.yaml
echo "RayJob submitted successfully! Checking status:"
kubectl get rayjob -n "$NAMESPACE"

echo "Deployment complete! Ray Dashboard is exposed via ingress or port-forward:"
echo "kubectl port-forward svc/corporate-ai-cluster-head-svc 8265:8265 -n $NAMESPACE"
