# AWS Infrastructure Cost Estimation: 12-Hour Operational Run

> **Distributed AI Foundation: Comprehensive 12-Hour AWS Pricing Breakdown**  
> Region: `us-east-1` (N. Virginia) | Cluster: EKS v1.31 | Workload: Ray Data, Bottlerocket Resolver & Iceberg Sink

<!-- # JM Comment: Granular 12-hour AWS billing analysis for platform engineering review -->

---

## 💰 12-Hour Total Cost Summary

| Cost Category | With Spot Workers (Our Architecture) | Without Spot (100% On-Demand) | Savings with Spot |
| :--- | :---: | :---: | :---: |
| **Total 12-Hour Cost** | **$7.61 USD** | **$12.25 USD** | **~$4.64 saved (38% overall)** |
| **Hourly Run Rate** | **~$0.63 / hour** | **~$1.02 / hour** | **-** |

---

## 📊 Granular Component Breakdown (12 Hours)

### 1. Compute & Kubernetes Management (EKS)
* **AWS EKS Control Plane**:
  * Rate: $0.10 per cluster / hour
  * 12-Hour Cost: `12 * $0.10` = **$1.20 USD**
* **Head Node Pool (1x On-Demand `m5.xlarge`)**:
  * Specs: 4 vCPU, 16 GiB RAM
  * Rate: $0.192 per hour
  * 12-Hour Cost: `12 * $0.192` = **$2.30 USD**
* **General Ray Workers (2x Spot `m5.xlarge` average)**:
  * Specs: 2 nodes x (4 vCPU, 16 GiB RAM)
  * Spot Rate: ~$0.057 per instance / hour (70% discount off $0.192)
  * 12-Hour Cost: `2 * 12 * $0.057` = **$1.37 USD**  
    *(On-Demand would have cost: 2 * 12 * $0.192 = $4.61 USD)*
* **Bottlerocket Resolver Node (1x `r5.large`)**:
  * Specs: 2 vCPU, 16 GiB RAM (Bottlerocket AMI)
  * Rate: $0.126 per hour
  * 12-Hour Cost: `12 * $0.126` = **$1.51 USD**

### 2. Networking & Traffic Routing
* **AWS NAT Gateway**:
  * Hourly Rate: $0.045 per hour
  * 12-Hour Base Cost: `12 * $0.045` = **$0.54 USD**
  * Data Processing (~10 GB for container pulls & S3 reads): `10 GB * $0.045` = **$0.45 USD**
  * Subtotal: **$0.99 USD**

### 3. Persistent Storage & EBS
* **Amazon EBS (gp3 Root Volumes)**:
  * 4 nodes x 50 GiB gp3 = 200 GiB
  * Rate: $0.08 per GiB-month
  * 12-Hour Cost: `(200 * $0.08 / 720 hrs) * 12 hrs` = **$0.27 USD**

### 4. Storage & Lakehouse Catalog (S3 & Glue)
* **S3 Data Lake & Ray Object Spill**:
  * Storage (~15 GB raw CSVs + Iceberg Parquet + temporary spills): `< $0.01 USD`
  * S3 API Operations (PUT/GET requests for 2,400 records & checkpoints): **~$0.02 USD**
* **AWS Glue Data Catalog**:
  * First 1,000,000 metadata requests/month are **FREE**.
  * 12-Hour Cost: **$0.00 USD**

### 5. Security & Secret Management
* **AWS Secrets Manager**:
  * Rate: $0.40 per secret / month
  * 12-Hour Cost: `(1 * $0.40 / 720 hrs) * 12 hrs` = **$0.01 USD**
* **AWS KMS (Customer Managed Key)**:
  * Rate: $1.00 per key / month
  * 12-Hour Cost: `($1.00 / 720 hrs) * 12 hrs` = **$0.02 USD**

---

## 🛡️ Cost Control & Auto-Teardown Safeguards

1. **Immediate Teardown**:
   When testing is complete, trigger the Bitbucket custom pipeline **`2-terraform-destroy-infra`**. This cleanly deletes all EC2 instances, EKS control plane, and NAT Gateways, dropping the ongoing hourly burn rate to **$0.00**.
2. **Ray Object Spill Expiry**:
   The S3 bucket lifecycle configuration automatically expires temporary spill files in `ray-spill/` after 3 days to avoid silent storage accumulation.
3. **Cluster Autoscaler Scaling down to 0**:
   The Spot worker pool scaling bounds are set to `min_size = 0, max_size = 8`. When no Ray jobs are running, the worker nodes terminate automatically.
