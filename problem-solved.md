# Distributed AI Foundation: Problem Statement & Value Delivered

> [!NOTE]
> This document details the architectural motivations, cost optimizations, and business/technical problems resolved by transitioning to a unified **Ray on Kubernetes (KubeRay)** platform.

---

## 1. Short Project Overview

This project builds a unified **"Paved Road" Distributed AI Platform** on AWS EKS using **Ray (KubeRay)**, **Apache Iceberg**, and **Terraform**.

It eliminates the traditional architectural divide where organizations run **Apache Spark** for Data Engineering (ETL) and separate, disconnected clusters (PyTorch / Airflow / Kubeflow) for AI & Machine Learning. Instead, it runs the entire pipeline—from parallel data ingestion and LLM entity resolution to transactional Iceberg storage and distributed ML training—on a **single distributed engine**.

---

## 2. What Problems Does It Solve?

### 1. Eliminating Apache Spark's Cost & JVM Penalty
* **The Problem with Spark**: Apache Spark runs on the Java Virtual Machine (JVM). When Python data science code (PySpark, Pandas, PyTorch) runs on Spark, data must cross the **Py4J serialization bridge** between the JVM and Python processes. This creates massive memory bloat, high CPU overhead, and slow processing times.
* **How Ray Resolves It**: Ray is written in C++ with a native **zero-copy shared-memory object store (Plasma `/dev/shm`)**. Python workers read dataframes directly from shared memory without serialization penalties, drastically slashing compute time and memory requirements.

### 2. Slashing Cloud Compute Costs by 60% – 80%
* **Spot Instance Auto-Scaling**: Worker nodes run on AWS Spot instances with automatic multi-family diversification (`m5.xlarge`, `c5.xlarge`, `m5a.xlarge`). Spot instances cost **~70% less** than On-Demand instances.
* **Preemption Resilience**: If AWS reclaims a spot node, Ray automatically re-executes lost tasks via `@ray.remote(max_retries=3)` and recovers object state from S3/disk spills without crashing the job.
* **Resource Pinning**: Heavy, stateful tasks (like the `EntityResolver`) are strictly pinned to a specialized, secure **Bottlerocket** node pool using custom Ray resources (`actor_node: 1`), preventing expensive over-provisioning across the rest of the cluster.

### 3. Slashing Latency & Time-to-Train
* **No Multi-Cluster Staging**: In traditional architectures, Spark writes data to S3, an Airflow DAG triggers a second cluster, and a training framework re-reads all files from S3 into GPU/CPU memory. In Ray, the ingested data is handed directly to **Ray Train** in-memory, cutting end-to-end pipeline runtime from hours to minutes.
* **Stateful Actor Model**: Instead of reloading ML model weights or API connections for every batch of records, the `EntityResolver` actor stays warm in memory across batches, saving substantial initialization latency.

### 4. ACID Reliability with Apache Iceberg
* **The Problem**: Raw CSV/Parquet data in S3 suffers from dirty reads, race conditions, and lack of rollback capabilities when jobs fail mid-way.
* **How It Resolves It**: PyIceberg commits transactional snapshots directly to the **AWS Glue Data Catalog**. Machine learning training only reads validated, consistent snapshots, guaranteeing zero data corruption.

---

## 3. Architecture Comparison at a Glance

| Factor | Traditional Stack (Spark + Airflow + PyTorch) | Our Ray Platform (This Project) |
| :--- | :--- | :--- |
| **Compute Engines** | 2–3 separate clusters (Spark EMR + EKS + Airflow) | **1 unified EKS cluster** with KubeRay |
| **Data Transfer** | S3 write &rarr; read &rarr; transform &rarr; S3 write &rarr; read | **Zero-copy shared memory** (`/dev/shm`) |
| **Infrastructure Cost** | High (idle On-Demand JVM nodes + multi-cluster bills) | **60–80% lower** (Spot node groups + auto-scaling) |
| **Pipeline Latency** | High (JVM serialization + inter-job polling delays) | **Low** (in-memory handoff directly to Ray Train) |
| **Security Model** | Fragmented AWS keys per tool | **Unified Zero-Trust IRSA** (No static cloud keys) |

---

> [!TIP]
> **Key Takeaway**: This platform replaces an expensive, fragmented 3-engine data & AI pipeline with **one distributed Python-native engine**, reducing AWS compute bills, cutting end-to-end runtimes, and providing transactional data guarantees.

---

## 4. Key Artifacts in this Repository
* **Project Workflow & Guide**: [project-workflow.md](file:///c:/Users/Vijay.K/Music/odex-iac-bootstrap/project-workflow.md)
* **Master Technical README**: [README.md](file:///c:/Users/Vijay.K/Music/odex-iac-bootstrap/README.md)
* **Pipeline Implementation**: [ray_pipeline.py](file:///c:/Users/Vijay.K/Music/odex-iac-bootstrap/ray_pipeline.py)
* **Kubernetes Manifests**: [k8s/](file:///c:/Users/Vijay.K/Music/odex-iac-bootstrap/k8s/)
* **CI/CD Automation**: [bitbucket-pipelines.yml](file:///c:/Users/Vijay.K/Music/odex-iac-bootstrap/bitbucket-pipelines.yml) & [.github/workflows/ci-cd.yaml](file:///c:/Users/Vijay.K/Music/odex-iac-bootstrap/.github/workflows/ci-cd.yaml)
