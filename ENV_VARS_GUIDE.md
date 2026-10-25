# Environment Variables & Secrets Configuration Guide

> **Distributed AI Foundation: Master Reference for Variables & Secrets Storage**  
> Complete inventory of all environment variables, cloud secrets, repository variables, and where each must be stored.

<!-- # JM Comment: Master environment variables and secrets configuration guide -->

---

## 📑 Summary: Where to Store Each Variable

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. Bitbucket Repository Variables (CI/CD Pipeline Execution)                │
│    Location: Repository Settings -> Pipelines -> Repository Variables       │
│    - AWS_ACCESS_KEY_ID (Secured)                                            │
│    - AWS_SECRET_ACCESS_KEY (Secured)                                        │
│    - AWS_DEFAULT_REGION                                                     │
│    - EKS_CLUSTER_NAME                                                       │
│    - EXISTING_EKS_CLUSTER_NAME (Optional for existing cluster deployment)  │
│    - ECR_REGISTRY_URL                                                       │
│    - LLM_API_KEY (Secured)                                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ 2. AWS Secrets Manager (Production Runtime Zero-Trust Secret)               │
│    Location: AWS Console -> AWS Secrets Manager -> Store New Secret         │
│    - Secret Name: distributed-ai/llm-api-key                                │
│    - Secret Property: api_key                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│ 3. Kubernetes In-Cluster Secrets & ConfigMaps (Ray Runtime)                 │
│    Location: EKS Cluster Namespace 'kuberay' via External Secrets / Manifest│
│    - Secret: ray-llm-secret (Key: LLM_API_KEY)                              │
│    - Env: ICEBERG_CATALOG_TYPE ("glue" or "sql")                            │
│    - Env: ICEBERG_S3_BUCKET ("distributed-ai-lake")                         │
│    - Env: RAY_ADDRESS (Auto-discovered by KubeRay)                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Bitbucket Repository Variables (Step-by-Step Instructions)

### How to Add in Bitbucket:
1. Open your Bitbucket repository in the browser: `https://bitbucket.org/odexglobal/odex-iac-bootstrap`
2. In the left navigation menu, click **Repository Settings** (gear icon).
3. Under the **Pipelines** heading, click **Repository Variables**.
4. Add the following variables one by one:

| Variable Name | Description | Is Secured? | Example Value | Where & How Used |
| :--- | :--- | :---: | :--- | :--- |
| `AWS_ACCESS_KEY_ID` | IAM User access key with permissions to provision VPC, EKS, EC2, S3, and Glue | **YES** (Check Secured) | `AKIAIOSFODNN7EXAMPLE` | Authenticates Terraform, AWS CLI, and Docker ECR login during CI/CD steps. |
| `AWS_SECRET_ACCESS_KEY` | IAM User secret access key matching the Access Key ID | **YES** (Check Secured) | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` | Authenticates all AWS API calls in the Bitbucket pipeline runner. |
| `AWS_DEFAULT_REGION` | Primary AWS region for all infrastructure resources | NO | `us-east-1` | Sets the deployment region for Terraform, EKS cluster, and S3 data lakes. |
| `EKS_CLUSTER_NAME` | Name of the primary EKS cluster managed by Terraform | NO | `distributed-ai-eks` | Target cluster for Terraform provisioning, `update-kubeconfig`, and RayJob dispatch. |
| `EXISTING_EKS_CLUSTER_NAME` | **(Optional)** Name of an already-existing cluster | NO | `my-production-cluster` | Used exclusively by custom pipeline `4-deploy-rayjob-existing-cluster`. |
| `ECR_REGISTRY_URL` | AWS Elastic Container Registry base URL | NO | `123456789012.dkr.ecr.us-east-1.amazonaws.com` | Docker tag and push destination for the `distributed-ai-ray` container image. |
| `LLM_API_KEY` | Secret token for LLM-based Entity Resolution service | **YES** (Check Secured) | `sk-enterprise-token-v2` | Injected into the container and used by `EntityResolver` actor. |

---

## 2. AWS Secrets Manager (Production Zero-Trust Storage)

In production, to eliminate hardcoded secrets in Git, `LLM_API_KEY` is stored in **AWS Secrets Manager** and synchronized dynamically to Kubernetes via the **External Secrets Operator (ESO)**:

### How to Create in AWS Console:
1. Navigate to **AWS Secrets Manager** -> **Store a new secret**.
2. Select **Other type of secret**.
3. Under Key/value pairs:
   * **Key**: `api_key`
   * **Value**: Paste your secret token (e.g. `sk-live-corporate-key-2026`).
4. Set **Secret name**: `distributed-ai/llm-api-key`.
5. Click **Store**.
6. The External Secrets Operator configured in `k8s/01-secrets.yaml` will automatically read this secret using the pod's IRSA identity and create the Kubernetes secret `ray-llm-secret`.

---

## 3. Kubernetes Runtime Environment Variables (In-Cluster)

These environment variables are consumed directly by the `ray_pipeline.py` script running inside worker pods:

| Variable Name | Stored In | Value / Purpose |
| :--- | :--- | :--- |
| `LLM_API_KEY` | Kubernetes Secret `ray-llm-secret` | Injected via `valueFrom.secretKeyRef`. Consumed by the `EntityResolver` actor in `ray_pipeline.py`. |
| `ICEBERG_CATALOG_TYPE` | Container Env in `k8s/04-rayjob.yaml` | `glue` (default in AWS) or `sql` (for local tests). Tells PyIceberg which catalog to commit transactions to. |
| `ICEBERG_S3_BUCKET` | Container Env in `k8s/04-rayjob.yaml` | S3 bucket name (e.g. `distributed-ai-lake`). Destination for Iceberg metadata and parquet data files. |
| `RAY_ADDRESS` | Auto-set by KubeRay | Connection string to the Ray Head service (e.g. `corporate-ai-cluster-head-svc:6379`). |
| `PYTHONUNBUFFERED` | Container Env | Set to `1` so Ray task stdout/stderr streams immediately without log buffering. |

---

## 4. Terraform Local Variables (`terraform.tfvars`)

Stored locally or passed via CI/CD for infrastructure configuration:
* Location: `terraform/environments/prod/terraform.tfvars`
```hcl
aws_region   = "us-east-1"
environment  = "prod"
cluster_name = "distributed-ai-eks"
vpc_cidr     = "10.0.0.0/16"
azs          = ["us-east-1a", "us-east-1b", "us-east-1c"]
```
