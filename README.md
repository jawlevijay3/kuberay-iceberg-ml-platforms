# Distributed AI Foundation: Enterprise Ray Platform on Kubernetes

> **DevOps / Platform Engineer Assignment: The Distributed AI Foundation**  
> Complete Production Solution: Decoupled Terraform State Layers, Bottlerocket 2CPU/16GB Node Group, KubeRay Orchestration, Gateway API, and Bitbucket CI/CD Pipelines.

<!-- # JM Comment: Master architectural documentation for the Distributed AI Foundation -->

---

## 📑 Table of Contents
1. [Executive Summary & Purpose](#1-executive-summary--purpose)
2. [Why Ray? (Distributed Platform Philosophy)](#2-why-ray-distributed-platform-philosophy)
3. [High-Level Architecture Diagram](#3-high-level-architecture-diagram)
4. [Modular Decoupled Terraform State Layers](#4-modular-decoupled-terraform-state-layers)
5. [Bottlerocket Node Group & Spot Cost-Reduction Strategy](#5-bottlerocket-node-group--spot-cost-reduction-strategy)
6. [Specialized Actor Pinning & Ray Train (ScalingConfig)](#6-specialized-actor-pinning--ray-train-scalingconfig)
7. [Zero-Trust Security & Cloud Integration (IRSA, ESO & Iceberg)](#7-zero-trust-security--cloud-integration-irsa-eso--iceberg)
8. [Gateway API (Modern Ingress Replacement)](#8-gateway-api-modern-ingress-replacement)
9. [Bitbucket CI/CD Custom Pipelines (Deploy, Teardown & Audit)](#9-bitbucket-cicd-custom-pipelines-deploy-teardown--audit)
10. [Serverless Alternative to Kubernetes](#10-serverless-alternative-to-kubernetes)
11. [Step-by-Step Setup & Verification Guide](#11-step-by-step-setup--verification-guide)

---

## 1. Executive Summary & Purpose

Modern enterprise AI platforms face a fundamental architectural divide:
* **Data Engineering workloads** (ETL, batch ingestion, catalog upserts) traditionally run on JVM-heavy engines like Apache Spark.
* **LLM & Deep Learning workloads** (fine-tuning, embedding generation, GPU batch inference, distributed training) run natively in Python ecosystems (PyTorch, Hugging Face, vLLM).

**The Mission:** Build a unified **"Paved Road"** using **Ray** and **Kubernetes (EKS v1.31)**. This project provides a production-grade platform where:
1. Parallel data ingestion loads over 2,400 records from distinct corporate datasets.
2. A stateful **EntityResolver actor** normalizes company naming variations into canonical IDs on a dedicated **Bottlerocket** node group.
3. Records are committed transactionally into an **Apache Iceberg** table (`corporate_registry`) with ACID snapshot isolation.
4. Harmonized data is used to train a Machine Learning revenue prediction model via **Ray Train** (`ScalingConfig`).
5. All infrastructure is deployed in decoupled, reproducible Terraform state layers and automated via Bitbucket Pipelines with custom teardown and verification audits.

---

## 2. Why Ray? (Distributed Platform Philosophy)

| Capability | Apache Spark | Ray (Our Architecture) | Standalone Python / Celery |
| :--- | :--- | :--- | :--- |
| **Execution Paradigm** | Pure Dataflow (BSP). Stateless tasks only. | **Unified Task + Actor Model**. Supports both stateless tasks and stateful long-lived actors. | Task queue only (Celery) or single-node process. |
| **Python / ML Native** | Python code is marshaled across Py4J bridge to JVM workers (heavy serialization penalty). | **C++ Plasma Object Store with zero-copy shared memory (`/dev/shm`)**; 100% native Python. | Native Python, but limited by GIL on a single node. |
| **Stateful GPU / LLM Services** | Very difficult to hold deep learning models or LLMs in GPU memory across iterations. | **Ray Actors (`@ray.remote`)** load weights into GPU/VRAM once and process continuous streaming batches. | Hard to scale across multi-node clusters dynamically. |
| **Heterogeneous Scheduling** | Coarse-grained executor slots (uniform CPU/memory across worker pods). | **Fine-grained custom resources** (e.g. `resources={"actor_node": 1}`) per task/actor. | Manual process management. |
| **Unified Workload Pipeline** | Requires Spark for ETL + Airflow for dispatch + PyTorch on Kubeflow for training. | **Single distributed engine**: Ingestion (Ray Data) -> Entity Resolution (Ray Actors) -> Iceberg Sink -> Training (Ray Train). | Multiple disconnected services. |

---

## 3. High-Level Architecture Diagram

```mermaid
flowchart TB
    subgraph VCS_CICD["Bitbucket CI/CD Custom Pipelines"]
        CUSTOM_DEPLOY["1-terraform-deploy-infra (Segmented Layers)"]
        CUSTOM_DESTROY["2-terraform-destroy-infra (With Audit Report)"]
        CUSTOM_JOB["3-deploy-rayjob-only"]
        CUSTOM_EXISTING["4-deploy-rayjob-existing-cluster"]
    end

    subgraph AWS_INFRA["AWS Cloud Infrastructure (us-east-1)"]
        subgraph NETWORKING["Layer 01: VPC & Subnets (Multi-AZ)"]
            PUB_SUB["Public Subnets (NAT Gateway / ALBs)"]
            PRIV_SUB["Private Subnets (EKS Compute Nodes)"]
        end

        subgraph STORAGE_ICEBERG["Layer 02: Storage & Apache Iceberg"]
            S3_RAW["S3: Raw Datasets (CSV)"]
            S3_ICEBERG["S3: Iceberg Table Warehouse"]
            S3_SPILL["S3: Ray Plasma Object Spill"]
            GLUE_CAT["AWS Glue Catalog (corporate_iceberg_db)"]
            KMS["AWS KMS Encryption Key"]
        end

        subgraph IAM_SECURITY["Layer 03: EKS Cluster 1.31 & Zero-Trust IRSA"]
            OIDC["EKS OIDC Provider"]
            IRSA["AWS IRSA Scoped IAM Role"]
            ESO["External Secrets Operator (AWS Secrets Manager)"]
        end

        subgraph COMPUTE_POOLS["Layer 04: Decoupled EKS Node Groups"]
            POOL_HEAD["Node Pool 1: On-Demand (m5.xlarge)\nLabel: workload=system"]
            POOL_SPOT["Node Pool 2: Auto-scaling Spot (m5, m5a, c5, m4)\nLabel: workload=general"]
            POOL_BOTTLEROCKET["Node Pool 3: Bottlerocket (2 CPU, 16 GB RAM)\nLabel: workload=entity-resolver\nTaint: dedicated=entity-resolver:NoSchedule"]
        end

        subgraph GATEWAY_CTRL["Layer 05: KubeRay & Gateway API"]
            KUBERAY["KubeRay Operator Controller"]
            GW_API["Kubernetes Gateway API (GatewayClass, Gateway, HTTPRoute)"]
        end
    end

    subgraph EXECUTION_FLOW["Ray Distributed Execution Lifecycle"]
        INGEST["1. Parallel Ingest Tasks (Spot Workers)"]
        RESOLVE["2. EntityResolver Actor (actor_node=1 on Bottlerocket)"]
        ICEBERG["3. Transactional Upsert into Iceberg (Glue Catalog)"]
        TRAIN["4. Distributed ML Training via Ray Train (ScalingConfig)"]

        INGEST --> RESOLVE --> ICEBERG --> TRAIN
    end

    GW_API -->|Port 8265 Routing| POOL_HEAD
    IRSA -.->|Zero-Trust Auth| POOL_SPOT
    IRSA -.->|Zero-Trust Auth| POOL_BOTTLEROCKET
    POOL_SPOT --> INGEST
    POOL_BOTTLEROCKET --> RESOLVE
```

---

## 4. Modular Decoupled Terraform State Layers

To eliminate monolithic state files, the infrastructure is decoupled into isolated, segmented layers in `terraform/layers/`:

* **`01-vpc/`**: Provisions the 3-AZ VPC, public subnets for NAT Gateways, private subnets for compute, and Kubernetes internal ELB tags.
* **`02-storage-iceberg/`**: Provisions S3 data lake bucket, KMS encryption key, Ray object spill lifecycle rules, and AWS Glue Data Catalog database (`corporate_iceberg_db`).
* **`03-eks-cluster/`**: Provisions managed AWS EKS v1.31 control plane, OIDC provider, and IAM IRSA trust policies.
* **`04-node-groups/`**: Provisions three decoupled compute pools:
  1. **Head System Pool**: On-Demand `m5.xlarge`.
  2. **General Spot Workers**: Diversified Spot instances for data tasks.
  3. **Bottlerocket Resolver Pool**: Bottlerocket AMI with **2 CPU and 16 GB RAM** (`r5.large` / `r6i.large`).
* **`05-kuberay-gateway/`**: Deploys KubeRay Operator and modern Kubernetes Gateway API controllers.

---

## 5. Bottlerocket Node Group & Spot Cost-Reduction Strategy

### 1. Dedicated Bottlerocket Node Group (2 CPU, 16 GB RAM)
* **Operating System**: AWS Bottlerocket AMI (`BOTTLEROCKET_x86_64`)—a minimal, container-optimized OS with a read-only root filesystem and no shell, drastically reducing security attack surface.
* **Hardware Specs**: `r5.large` / `r6i.large` providing **2 vCPUs and 16 GiB of RAM**.
* **Taint & Toleration Isolation**: Tainted with `dedicated=entity-resolver:NoSchedule`. Only the `EntityResolver` pod holds the toleration.

### 2. Spot Worker Resilience Strategy
* **Head Node on On-Demand**: Never placed on Spot to guarantee Global Control Store (GCS) stability.
* **Instance Family Diversification**: Spot pool uses 4 families (`m5.xlarge`, `m5a.xlarge`, `c5.xlarge`, `m4.xlarge`) to avoid simultaneous reclamation.
* **Graceful Drain via PreStop Hook**:
  ```yaml
  lifecycle:
    preStop:
      exec:
        command: ["/bin/sh", "-c", "ray stop --grace-period 60"]
  ```
* **Task Retries & Object Spilling**: `@ray.remote(max_retries=3, retry_exceptions=True)` on tasks, `max_restarts=5` on actors, and Plasma object spilling to local disk/S3.

---

## 6. Specialized Actor Pinning & Ray Train (ScalingConfig)

### 1. Strict Actor Pinning to Bottlerocket Pool
1. The Bottlerocket node group sets label `workload=entity-resolver` and taint `dedicated=entity-resolver:NoSchedule`.
2. In `k8s/03-raycluster.yaml`, the `specialized-resolver-workers` group advertises custom Ray resource `actor_node: 1`.
3. In `ray_pipeline.py`, the actor definition declares:
   ```python
   @ray.remote(resources={"actor_node": 1})
   class EntityResolver:
       ...
   ```
4. **Result:** Ray's scheduler strictly pins the `EntityResolver` to the Bottlerocket node group.

### 2. Pipeline Execution Order & Ray Train
The pipeline executes strictly in the sequence required by the assignment:
1. **Parallel Ingestion**: Loads SupplyChain and Financial records.
2. **Entity Resolution**: Normalizes corporate variants into canonical IDs.
3. **Apache Iceberg Commit**: Harmonized data is committed transactionally to `corporate_registry`.
4. **Ray Train ML Training**: Harmonized data from Iceberg is passed to Ray Train using:
   ```python
   scaling_config = ScalingConfig(num_workers=2, use_gpu=False)
   ```
   Predicts corporate revenues with evaluation metrics (R² = 0.88+, RMSE) and model checkpointing.

---

## 7. Zero-Trust Security & Cloud Integration (IRSA, ESO & Iceberg)

* **AWS IRSA**: Kubernetes ServiceAccount `ray-worker-sa` is mapped to an AWS IAM role via OIDC federation.
* **External Secrets Operator (ESO)**: `k8s/01-secrets.yaml` queries AWS Secrets Manager (`distributed-ai/llm-api-key`) dynamically. No plain-text API keys in Git!
* **Apache Iceberg**: Transactional table with ACID guarantees, snapshot isolation, and metadata registration in AWS Glue.

---

## 8. Gateway API (Modern Ingress Replacement)

Modern Kubernetes workloads use **Gateway API** (`gateway.networking.k8s.io/v1`) instead of legacy Ingress:
* **`GatewayClass`**: Managed by Envoy Gateway / AWS Gateway API Controller.
* **`Gateway`**: Listens on HTTP (80) and HTTPS (443) with TLS termination.
* **`HTTPRoute`**: Routes traffic arriving at `/` to `corporate-ai-cluster-head-svc` port 8265 (Ray Dashboard).

---

## 9. Bitbucket CI/CD Custom Pipelines (Deploy, Teardown & Audit)

Configured in `bitbucket-pipelines.yml`:

| Pipeline Option | Trigger Type | What It Does |
| :--- | :--- | :--- |
| **`1-terraform-deploy-infra`** | Custom | Deploys Terraform segment-by-segment (`01-vpc` -> `02-storage` -> `03-eks` -> `04-nodes` -> `05-kuberay-gateway`) and verifies creation via AWS CLI. |
| **`2-terraform-destroy-infra`** | Custom | Destroys infrastructure in reverse order and **runs an automated audit**. If any VPC, EKS cluster, or S3 bucket remains, it prints an alert listing exact IDs for manual console deletion. |
| **`3-deploy-rayjob-only`** | Custom | Runs unit tests, builds container image, pushes to ECR, and deploys RayJob CRD to EKS. |
| **`4-deploy-rayjob-existing-cluster`** | Custom | Deploys RayJob to an **already existing EKS cluster** (`EXISTING_EKS_CLUSTER_NAME`), automatically checking and installing missing CRDs and prerequisites first! |
| **`5-full-end-to-end-deployment`** | Custom | Runs complete automated deployment: Terraform segments -> verification -> container build -> RayJob deploy. |
| **Pull Requests & Main** | Automatic | PRs validate Terraform syntax and run tests; merges deploy to EKS. |

---

## 10. Serverless Alternative to Kubernetes

1. **Anyscale Platform (Managed Serverless Ray)**: Zero K8s management, native Ray optimizations, on-demand cloud scaling.
2. **AWS Glue 4.0 (Ray Engine)**: Serverless Ray execution with built-in Glue catalog integration and per-second DPU billing.
3. **AWS Batch with ECS Fargate**: Serverless container execution without node management.

---

## 11. Step-by-Step Setup & Verification Guide

1. **Configure Environment Variables**: Read [`ENV_VARS_GUIDE.md`](file:///C:/Users/Vijay.K/Music/odex-iac-bootstrap/ENV_VARS_GUIDE.md).
2. **Run Local Unit Tests**:
   ```bash
   python tests/test_ray_components.py
   ```
3. **Run Local Pipeline Dry-Run**:
   ```bash
   python scripts/run_local_pipeline.py
   ```
4. **Trigger Deployment in Bitbucket**:
   Go to Bitbucket -> Pipelines -> Run Pipeline -> Select `1-terraform-deploy-infra` or `5-full-end-to-end-deployment`.
