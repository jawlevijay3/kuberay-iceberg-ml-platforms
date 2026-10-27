# Environment Variables & Secrets Configuration Guide

> **Distributed AI Foundation: Master Reference for Variables, Secrets Store CSI Driver & Bitbucket Execution**  
> Complete inventory of all environment variables, cloud secrets, repository variables, and the 2-stage execution process.

<!-- # JM Comment: Master environment variables and secrets configuration guide -->

---

## 📑 Two-Stage Infrastructure & Deployment Workflow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: Infrastructure Provisioning (Terraform Pipeline)                   │
│ Run Custom Pipeline: '1-deploy-infrastructure'                              │
│ 1. Deploys VPC, Subnets, NAT Gateways across 3 AZs                          │
│ 2. Deploys S3 Data Lake, Iceberg Warehouse, and AWS Glue Catalog            │
│ 3. Deploys EKS Cluster v1.34 with managed addons (VPC-CNI, CoreDNS, EBS CSI)│
│ 4. Deploys Head Node, Spot Workers, and Bottlerocket (2CPU 16GB) Node Group │
│ 5. Deploys KubeRay Operator, Gateway API (Envoy), and Secrets Store CSI     │
│ 6. Automated verification confirms all AWS resources are ready.             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 2: Pipeline Execution (RayJob Deployment Pipeline)                    │
│ Run Custom Pipeline: '2-build-and-deploy-ray-pipeline'                      │
│ 1. Executes Unit Tests on data ingestion and canonicalization               │
│ 2. Builds Docker container image and pushes to Amazon ECR                   │
│ 3. Injects SecretProviderClass for AWS Secrets Store CSI Driver             │
│ 4. Applies RayCluster, Monitoring, Logging, and RayJob CRD execution        │
│ 5. Executes Ingestion -> Entity Resolution -> Iceberg Commit -> Ray Train   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Bitbucket Repository Variables (Step-by-Step Instructions)

### How to Add in Bitbucket:
1. Open your Bitbucket repository in the browser: `https://bitbucket.org/odexglobal/odex-iac-bootstrap`
2. In the left navigation menu, click **Repository Settings** (gear icon).
3. Under the **Pipelines** heading, click **Repository Variables**.
4. Add the following variables:

| Variable Name | Description | Is Secured? | Example Value | Where & How Used |
| :--- | :--- | :---: | :--- | :--- |
| `AWS_ACCESS_KEY_ID` | IAM User access key with permissions to provision VPC, EKS, EC2, S3, and Glue | **YES** (Check Secured) | `AKIAIOSFODNN7EXAMPLE` | Authenticates Terraform, AWS CLI, and Docker ECR login during CI/CD steps. |
| `AWS_SECRET_ACCESS_KEY` | IAM User secret access key matching the Access Key ID | **YES** (Check Secured) | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` | Authenticates all AWS API calls in the Bitbucket pipeline runner. |
| `AWS_DEFAULT_REGION` | Primary AWS region for all infrastructure resources | NO | `us-east-1` | Sets the deployment region for Terraform, EKS cluster, and S3 data lakes. |
| `EKS_CLUSTER_NAME` | Name of the primary EKS cluster managed by Terraform | NO | `distributed-ai-eks` | Target cluster for Terraform provisioning, `update-kubeconfig`, and RayJob dispatch. |
| `ECR_REGISTRY_URL` | AWS Elastic Container Registry base URL | NO | `123456789012.dkr.ecr.us-east-1.amazonaws.com` | Docker tag and push destination for the `distributed-ai-ray` container image. |
| `LLM_API_KEY` | Secret token for LLM-based Entity Resolution service | **YES** (Check Secured) | `sk-enterprise-token-v2` | Used by CI/CD and injected into Secrets Store CSI Driver. |

---

## 2. AWS Secrets Store CSI Driver (Zero-Trust Secret Injection)

The platform uses the **Kubernetes Secrets Store CSI Driver** with the **AWS Secrets and Configuration Provider (ASCP)** to eliminate static secrets from Git:

### How to Create the Secret in AWS Secrets Manager:
1. Navigate to **AWS Secrets Manager** -> **Store a new secret**.
2. Select **Other type of secret**.
3. Under Key/value pairs:
   * **Key**: `api_key`
   * **Value**: Paste your secret token (e.g. `sk-live-corporate-key-2026`).
4. Set **Secret name**: `distributed-ai/llm-api-key`.
5. Click **Store**.

### How Kubernetes Mounts and Consumes the Secret:
1. `k8s/01-secrets.yaml` defines a **`SecretProviderClass`** pointing to `distributed-ai/llm-api-key`.
2. The `ray-resolver-worker` container in `k8s/03-raycluster.yaml` mounts the CSI volume to `/mnt/secrets-store`.
3. The Secrets Store CSI Driver automatically reads the secret from AWS Secrets Manager using the pod's IRSA identity (`ray-worker-sa`) and:
   * Writes the file to `/mnt/secrets-store/LLM_API_KEY`.
   * Automatically synchronizes a native Kubernetes Secret `ray-llm-secret`.
4. In `ray_pipeline.py`, the `EntityResolver` actor reads `LLM_API_KEY` seamlessly with zero plain-text leaks!

---

## 3. Kubernetes Runtime Environment Variables (In-Cluster)

These environment variables are consumed directly by the `ray_pipeline.py` script running inside worker pods:

| Variable Name | Stored In | Value / Purpose |
| :--- | :--- | :--- |
| `LLM_API_KEY` | Kubernetes Secret `ray-llm-secret` or `/mnt/secrets-store/LLM_API_KEY` | Mounted via Secrets Store CSI Driver. Consumed by the `EntityResolver` actor. |
| `ICEBERG_CATALOG_TYPE` | Container Env in `k8s/04-rayjob.yaml` | `glue` (default in AWS) or `sql` (for local tests). Tells PyIceberg which catalog to commit transactions to. |
| `ICEBERG_S3_BUCKET` | Container Env in `k8s/04-rayjob.yaml` | S3 bucket name (e.g. `distributed-ai-lake-d14ec094`). Destination for Iceberg metadata and parquet data files. |
| `GLUE_DATABASE_NAME` | Container Env in `k8s/04-rayjob.yaml` | AWS Glue database name (`corporate_iceberg_db`). Holds the Iceberg table catalogs. |
| `RAY_ADDRESS` | Auto-set by KubeRay | Connection string to the Ray Head service (e.g. `corporate-ai-cluster-head-svc:6379`). |
| `PYTHONUNBUFFERED` | Container Env | Set to `1` so Ray task stdout/stderr streams immediately without log buffering. |
