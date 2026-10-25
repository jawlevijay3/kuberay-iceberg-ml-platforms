# JM Comment: Automated End-to-End Deployment PowerShell Script for Windows
param(
    [string]$Region = "us-east-1",
    [string]$ClusterName = "distributed-ai-eks",
    [string]$Namespace = "kuberay"
)

Write-Host "==========================================================================" -ForegroundColor Cyan
Write-Host "  DISTRIBUTED AI FOUNDATION: AUTOMATED DEPLOYMENT (POWERSHELL)           " -ForegroundColor Cyan
Write-Host "==========================================================================" -ForegroundColor Cyan

$CurrentDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $CurrentDir

# 1. Terraform Verification
Write-Host "`n[1/5] Validating Terraform..." -ForegroundColor Yellow
Set-Location -Path "$ProjectRoot\terraform\environments\prod"
terraform fmt -check -recursive ..\..
terraform init -backend=false
terraform validate
if ($LASTEXITCODE -ne 0) {
    Write-Error "Terraform validation failed."
    exit 1
}
Write-Host "Terraform validated successfully." -ForegroundColor Green
Set-Location -Path $ProjectRoot

# 2. Dataset Verification
Write-Host "`n[2/5] Checking Synthetic Datasets..." -ForegroundColor Yellow
python "$ProjectRoot\scripts\generate_datasets.py"

# 3. Unit Test Verification
Write-Host "`n[3/5] Running Local Unit Tests..." -ForegroundColor Yellow
python "$ProjectRoot\tests\test_ray_components.py"
if ($LASTEXITCODE -ne 0) {
    Write-Error "Unit tests failed."
    exit 1
}

# 4. Pipeline Dry-Run
Write-Host "`n[4/5] Executing Pipeline Dry-Run..." -ForegroundColor Yellow
python "$ProjectRoot\scripts\run_local_pipeline.py"

# 5. Kubernetes Manifest Guidance
Write-Host "`n[5/5] Kubernetes Manifests Ready for Deployment:" -ForegroundColor Yellow
Write-Host "  kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml" -ForegroundColor White
Write-Host "  kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/secrets-store-csi-driver/main/charts/secrets-store-csi-driver/crds/secrets-store.csi.x-k8s.io_secretproviderclasses.yaml" -ForegroundColor White
Write-Host "  kubectl apply -f k8s/00-namespace.yaml" -ForegroundColor White
Write-Host "  kubectl apply -f k8s/01-secrets.yaml" -ForegroundColor White
Write-Host "  kubectl apply -f k8s/02-irsa-serviceaccount.yaml" -ForegroundColor White
Write-Host "  kubectl apply -f k8s/03-raycluster.yaml" -ForegroundColor White
Write-Host "  kubectl apply -f k8s/06-gateway-api.yaml" -ForegroundColor White
Write-Host "  kubectl apply -f k8s/04-rayjob.yaml" -ForegroundColor White
Write-Host "`nTo view the Ray Dashboard locally:" -ForegroundColor Cyan
Write-Host "  kubectl port-forward svc/corporate-ai-cluster-head-svc 8265:8265 -n $Namespace" -ForegroundColor Green

Write-Host "`nDeployment verification complete!" -ForegroundColor Green
