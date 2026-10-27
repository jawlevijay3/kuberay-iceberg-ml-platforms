# Distributed AI Platform — Access Endpoints & Monitoring Guide

This document contains quick-reference access URLs, Gateway API routes, local port-forwarding commands, and credentials for the Ray cluster and its observability stack.

---

## 1. AWS Gateway API / Load Balancer Endpoints

**AWS ELB Base Hostname:**
```
ad529736115b943669001553121ab1ca-1845380098.us-east-1.elb.amazonaws.com
```

### Direct Browser Access (Default Fallback)
| Service | URL | Protocol | Port | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Ray Dashboard (Default)** | [http://ad529736115b943669001553121ab1ca-1845380098.us-east-1.elb.amazonaws.com](http://ad529736115b943669001553121ab1ca-1845380098.us-east-1.elb.amazonaws.com) | HTTP | `80` | Direct access routes to Ray Dashboard |

---

## 2. Host-Based Gateway API Endpoints

The Kubernetes Gateway API is configured with host-based routing for each platform component:

| Service | Hostname | Internal Target |
| :--- | :--- | :--- |
| **Ray Dashboard** | `ray.corporate-ai.internal` or `ray.local` | `corporate-ai-cluster-head-svc.kuberay:8265` |
| **Grafana** | `grafana.corporate-ai.internal` or `grafana.local` | `grafana.monitoring:3000` |
| **Prometheus** | `prometheus.corporate-ai.internal` or `prometheus.local` | `prometheus.monitoring:9090` |

### Setting Up Local Host Resolution
To browse via domain names on your workstation, add these entries to your local hosts file:
- **Windows**: `C:\Windows\System32\drivers\etc\hosts` (Run Notepad as Administrator)
- **Linux/macOS**: `/etc/hosts`

```text
# Distributed AI Platform Endpoints
ad529736115b943669001553121ab1ca-1845380098.us-east-1.elb.amazonaws.com ray.corporate-ai.internal
ad529736115b943669001553121ab1ca-1845380098.us-east-1.elb.amazonaws.com grafana.corporate-ai.internal
ad529736115b943669001553121ab1ca-1845380098.us-east-1.elb.amazonaws.com prometheus.corporate-ai.internal
```

### Quick Verification via `curl`
```bash
# Verify Ray Dashboard
curl -I -H "Host: ray.corporate-ai.internal" http://ad529736115b943669001553121ab1ca-1845380098.us-east-1.elb.amazonaws.com

# Verify Grafana
curl -I -H "Host: grafana.corporate-ai.internal" http://ad529736115b943669001553121ab1ca-1845380098.us-east-1.elb.amazonaws.com

# Verify Prometheus
curl -I -H "Host: prometheus.corporate-ai.internal" http://ad529736115b943669001553121ab1ca-1845380098.us-east-1.elb.amazonaws.com
```

---

## 3. Local Port-Forwarding (Instant Access)

If accessing directly without DNS mapping, port-forward using `kubectl`:

### A. Grafana
```bash
kubectl port-forward -n monitoring svc/grafana 3000:3000
```
- **URL**: [http://localhost:3000](http://localhost:3000)
- **Username**: `admin`
- **Password**: `admin` (Anonymous Viewer is also enabled)

### B. Ray Dashboard
```bash
kubectl port-forward -n kuberay svc/corporate-ai-cluster-head-svc 8265:8265
```
- **URL**: [http://localhost:8265](http://localhost:8265)
- **Embedded Metrics Tab**: Direct native embedding of Grafana metrics panels.

### C. Prometheus UI
```bash
kubectl port-forward -n monitoring svc/prometheus 9090:9090
```
- **URL**: [http://localhost:9090](http://localhost:9090)
- **Targets Page**: [http://localhost:9090/targets](http://localhost:9090/targets)

---

## 4. Preloaded Grafana Dashboards

Grafana is provisioned with 3 dashboards located under the **`Distributed AI`** folder:

1. **`Ray - Default Dashboard`** (Official Ray Core Dashboard):
   - Comprehensive Ray cluster view (CPU, Memory, Disk, Network).
   - Task throughput, actor lifecycles, and scheduling latency.
   - Plasma Object Store memory capacity, active allocations, and S3 object spills.
   - Raylet, GCS, and Dashboard internal process diagnostics.
2. **`Ray - Data Dashboard`** (Official Ray Data Pipeline Dashboard):
   - Ingestion throughput, dataset block generation, and dataset stage durations.
3. **`Ray Distributed AI Cluster & Task Metrics`** (Tailored Operational Dashboard):
   - Top-level stat cards (Active Nodes, Tasks Status, Active Instances, Plasma Memory).
   - Bottom-level scheduling, spilling, and container working-set memory breakdown.

---

## 5. Checking RayJob Output & Execution Status

```bash
# 1. View overall RayJob state
kubectl get rayjob -n kuberay

# 2. View execution logs
kubectl logs corporate-data-pipeline-job-nwvbb -n kuberay

# 3. View live cluster resource allocation
kubectl exec -n kuberay corporate-ai-cluster-head-22q4d -- ray status
```

---

## 6. Centralized Logging (AWS CloudWatch & Fluent Bit)

Container stdout/stderr logs across all Ray Head, Worker, and RayJob pods are automatically aggregated in real-time by the Fluent Bit DaemonSet and streamed to CloudWatch Logs:

- **Workload Log Group:** `/aws/eks/distributed-ai-eks/workloads`
- **System Log Group:** `/aws/eks/distributed-ai-eks/system`

### Query Logs via AWS CLI
```bash
# Tail live Ray pod logs from CloudWatch
aws logs tail /aws/eks/distributed-ai-eks/workloads --follow --format short --region us-east-1

# Filter for error logs across all Ray workers
aws logs filter-log-events \
  --log-group-name /aws/eks/distributed-ai-eks/workloads \
  --filter-pattern "ERROR" \
  --region us-east-1
```

