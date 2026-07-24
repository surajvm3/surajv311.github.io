# The Evolution of Apache Airflow Deployments: From VMs to Docker to Helm on Kubernetes

> How the industry went from manually installing Airflow on bare metal to shipping it as a Helm chart on Kubernetes — and why each transition happened.

---

## What Is Apache Airflow?

Apache Airflow is an open-source workflow orchestration platform. You define data pipelines as DAGs (Directed Acyclic Graphs) in Python — each DAG describes what tasks to run, in what order, and on what schedule. Airflow handles scheduling, execution, retries, logging, and a web UI for visibility.

It has a few core components:

| Component | Role |
|---|---|
| **Scheduler** | Parses DAGs, schedules task runs, submits tasks for execution |
| **Webserver** | Hosts the Airflow UI for monitoring and triggering DAGs |
| **Workers** | Execute the actual task code |
| **Metadata DB** | PostgreSQL (or MySQL) — stores DAG runs, task states, connections, variables |
| **Executor** | Determines how tasks are distributed (LocalExecutor, CeleryExecutor, KubernetesExecutor) |
| **Message Broker** | Redis or RabbitMQ — used by CeleryExecutor to queue tasks for workers |

The deployment model for all of these components has changed dramatically over the years.

---

## Era 1: The VM Days — Manual Installation on a Server

### How It Worked

In the early years of Airflow adoption (roughly pre-2018 in most orgs), teams would spin up one or more virtual machines and install Airflow directly on the OS.

```bash
# Typical setup steps on Ubuntu/Debian
sudo apt-get install python3-pip
pip install apache-airflow
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --role Admin ...
airflow webserver --port 8080 &
airflow scheduler &
```

Workers would be started similarly, either on the same machine or additional VMs. The metadata database (Postgres) was either installed on the same VM or pointed to an external managed instance.

### What Managing It Actually Looked Like

- **DAGs** were deployed by SSHing into the VM and copying `.py` files into `$AIRFLOW_HOME/dags/`
- **Dependencies** were installed with `pip install` directly on the VM, shared across all DAGs
- **Config changes** required editing `airflow.cfg` and restarting processes via `systemctl` or screen sessions
- **Scaling workers** meant provisioning a new VM, installing everything again manually, and hoping nothing drifted
- **Upgrades** were painful: `pip install --upgrade apache-airflow` could break existing DAGs or dependencies

```
[VM]
├── airflow-scheduler (process)
├── airflow-webserver (process)
├── airflow-worker (process)
├── ~/airflow/dags/  ← DAGs copied here manually
├── airflow.cfg      ← edited by hand
└── Python packages  ← shared global environment
```

### Pros

- Simple mental model — it's just a process on a server
- No container or orchestration knowledge required
- Easy to debug: SSH in, look at logs, check processes

### Cons

- **No isolation:** all DAGs share the same Python environment; one DAG's `pip install` can break another's
- **Manual scaling:** adding a worker means provisioning and configuring a new VM by hand
- **Config drift:** servers diverge over time; "works on my VM" is a real problem
- **No HA out of the box:** if the scheduler process crashes, nothing restarts it automatically
- **Dependency hell:** conflicting package versions across DAGs are a constant headache
- **Upgrades are risky:** a bad `pip upgrade` can take down the entire Airflow installation

---

## Era 2: Docker — Packaging Airflow in a Container

### The Shift

As Docker became mainstream (~2017–2020), teams realized that containerizing Airflow solved many of the VM-era problems. Instead of installing Airflow on a host OS, you'd pull (or build) a Docker image containing Airflow and all its dependencies, then run it as a container.

```bash
# Running Airflow webserver in Docker
docker run -d \
  -p 8080:8080 \
  -e AIRFLOW__CORE__EXECUTOR=LocalExecutor \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow \
  -v $(pwd)/dags:/opt/airflow/dags \
  apache/airflow:2.7.0 webserver
```

The Airflow project eventually provided an official `docker-compose.yaml` that brought up the entire stack:

```yaml
# docker-compose.yaml (simplified)
services:
  postgres:
    image: postgres:13
    ...
  redis:
    image: redis:latest
    ...
  airflow-webserver:
    image: apache/airflow:2.7.0
    command: webserver
    ...
  airflow-scheduler:
    image: apache/airflow:2.7.0
    command: scheduler
    ...
  airflow-worker:
    image: apache/airflow:2.7.0
    command: celery worker
    ...
  airflow-triggerer:
    image: apache/airflow:2.7.0
    command: triggerer
    ...
```

### What Managing It Looked Like

- **DAGs** were mounted as a volume into the container, or baked into a custom Docker image
- **Custom dependencies** went into a custom `Dockerfile` extending the base Airflow image
- **Config** was passed as environment variables (`AIRFLOW__SECTION__KEY=value`)
- **Scaling** meant adjusting `docker-compose.yaml` to add more worker replicas
- **Upgrades** meant pulling a new image tag and running `docker-compose up -d`

```dockerfile
# Custom Airflow image with extra packages
FROM apache/airflow:2.7.0
RUN pip install pandas boto3 google-cloud-bigquery
COPY dags/ /opt/airflow/dags/
```

### Pros

- **Reproducibility:** same image runs the same way on any machine
- **Isolation:** Airflow's Python environment is sealed inside the container
- **Easier upgrades:** swap image tags, rebuild, redeploy
- **Local dev:** any engineer can run the full Airflow stack on their laptop with `docker-compose up`
- **Simpler dependency management:** pip installs go into the Dockerfile, not the host

### Cons

- **Still manual scaling:** docker-compose is single-host; scaling across machines requires extra tooling (Docker Swarm, or moving to Kubernetes)
- **Not production-grade HA:** docker-compose has no self-healing, no rolling restarts, no health-based rescheduling
- **DAG deployment is still a concern:** you either remount volumes (operational complexity) or rebuild and redeploy images (slow CI loop)
- **No cloud-native integration:** no automatic secrets injection, no auto-scaling, no native logging to cloud log sinks
- **CeleryExecutor needs Redis:** an extra service to manage and keep healthy

Docker was a major step forward for local development and small deployments, but for production at scale, teams quickly outgrew it.

---

## Era 3: Helm on Kubernetes — The Modern Standard

### Why Kubernetes?

Kubernetes (K8s) became the industry's de facto container orchestration platform. It provides:
- **Self-healing:** crashed containers are automatically restarted
- **Horizontal scaling:** add more pods with a single command or HPA rule
- **Rolling deployments:** update without downtime
- **Resource management:** CPU and memory limits per component
- **Native secrets/config management:** Kubernetes Secrets and ConfigMaps
- **Cloud integration:** persistent volumes, load balancers, IAM, logging — all first-class

But deploying a complex multi-component application like Airflow on Kubernetes by hand (writing Deployments, Services, PVCs, ConfigMaps, Secrets for each component) is repetitive and error-prone. That's where **Helm** comes in.

### What Is Helm?

Helm is the package manager for Kubernetes. A **Helm chart** is a collection of templated Kubernetes manifests packaged together with configurable values.

Think of it like `apt` or `pip` but for Kubernetes applications.

```bash
# Install Airflow on Kubernetes with Helm
helm repo add apache-airflow https://airflow.apache.org
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  -f values.yaml
```

One command. Airflow's entire stack — scheduler, webserver, workers, triggerer, metadata DB connection, ingress, RBAC — deployed on Kubernetes.

### The values.yaml: Your Entire Airflow Config in One File

The `values.yaml` file is where you customize everything about your Airflow deployment without touching Kubernetes manifests directly.

```yaml
# values.yaml (key sections)

# Airflow image — use official or custom
images:
  airflow:
    repository: your-registry/custom-airflow
    tag: "2.9.0-custom"
    pullPolicy: IfNotPresent

# Executor choice
executor: KubernetesExecutor   # or CeleryExecutor, LocalExecutor

# Webserver
webserver:
  replicas: 2
  resources:
    requests:
      memory: "1Gi"
      cpu: "500m"
    limits:
      memory: "2Gi"
      cpu: "1000m"

# Scheduler
scheduler:
  replicas: 1
  resources:
    requests:
      memory: "2Gi"
      cpu: "1000m"

# Workers (CeleryExecutor)
workers:
  replicas: 3
  resources:
    requests:
      memory: "4Gi"
      cpu: "2000m"

# Database — point to external Postgres
data:
  metadataConnection:
    user: airflow
    pass: ~          # injected from Kubernetes Secret
    host: postgres.internal
    port: 5432
    db: airflow

# Git-sync: auto-pull DAGs from a repo
dags:
  gitSync:
    enabled: true
    repo: https://github.com/your-org/your-dags.git
    branch: main
    subPath: "dags"
    period: 60s
    credentialsSecret: git-credentials

# Environment variables
env:
  - name: AIRFLOW__CORE__LOAD_EXAMPLES
    value: "False"
  - name: AIRFLOW__LOGGING__REMOTE_LOGGING
    value: "True"

# Ingress for webserver
ingress:
  web:
    enabled: true
    host: airflow.your-domain.com
```

### How DAGs Are Deployed in the Helm World

Two common patterns:

**1. Git-sync sidecar (recommended):**
A sidecar container runs alongside the scheduler and workers, continuously polling a Git repo and syncing DAGs to a shared volume. No manual file copying, no image rebuilds for DAG changes.

```
[Scheduler Pod]
├── airflow-scheduler container
└── git-sync sidecar  ← pulls from GitHub every 60s
    └── shared volume /opt/airflow/dags
```

**2. GCS bucket mounted via GKE FUSE (Cloud-native alternative):**
On GKE, you can mount a Google Cloud Storage bucket directly into Airflow pods as a filesystem using **GCSFuse** (via the GKE Cloud Storage FUSE CSI driver). DAGs live in GCS, are mounted at `/opt/airflow/dags` inside the pod, and updates to the bucket are visible to pods without any restart or re-sync delay.

```
[Scheduler Pod]
├── airflow-scheduler container
│   └── /opt/airflow/dags  ← mounted from GCS bucket via FUSE
└── (no sidecar needed)

GCS Bucket: gs://your-org-airflow-dags/
  └── my_dag.py
  └── another_dag.py
```

How it works: the GKE Cloud Storage FUSE CSI driver is enabled on the node pool, and you configure a `PersistentVolume` + `PersistentVolumeClaim` backed by the GCS bucket. All Airflow pods (scheduler, workers, webserver) mount the same PVC — they all see the same DAG files from the same bucket simultaneously.

**Kubernetes manifests:**
```yaml
# PersistentVolume backed by GCS bucket
apiVersion: v1
kind: PersistentVolume
metadata:
  name: airflow-dags-pv
spec:
  capacity:
    storage: 10Gi
  accessModes:
    - ReadWriteMany
  persistentVolumeReclaimPolicy: Retain
  csi:
    driver: gcsfuse.csi.storage.gke.io
    volumeHandle: your-org-airflow-dags   # GCS bucket name
    volumeAttributes:
      mountOptions: "implicit-dirs,uid=50000,gid=50000"
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: airflow-dags-pvc
  namespace: airflow
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 10Gi
  volumeName: airflow-dags-pv
  storageClassName: ""
```

**values.yaml wiring:**
```yaml
dags:
  persistence:
    enabled: true
    existingClaim: airflow-dags-pvc
```

**Deploying a DAG update** is then just an `gsutil` copy or a CI pipeline step — no Helm upgrade, no git-sync polling lag:
```bash
gsutil cp my_dag.py gs://your-org-airflow-dags/
# Pods see the new file within seconds via FUSE
```

**IAM:** The Airflow pods need GCS read access. Use Workload Identity to bind the Kubernetes service account to a GCP service account with `roles/storage.objectViewer` on the bucket — no credentials in the pod.

```bash
gcloud iam service-accounts add-iam-policy-binding \
  airflow-sa@your-project.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:your-project.svc.id.goog[airflow/airflow-scheduler]"
```

**Trade-offs vs. git-sync:**

| | git-sync | GCS FUSE |
|---|---|---|
| **Sync latency** | Configurable poll interval (e.g. 60s) | Near-instant (FUSE reads on access) |
| **History / rollback** | Full Git history | No built-in versioning (use GCS versioning) |
| **Auth** | Git credentials secret | Workload Identity (GKE-native) |
| **GCP dependency** | No | Yes — GKE only |
| **Operational simplicity** | Self-contained in Helm values | Requires CSI driver + PV/PVC setup |
| **DAG deploy workflow** | `git push` → sync | `gsutil cp` or CI step |

GCS FUSE is a strong choice when your team already works heavily with GCP, you want to avoid a Git dependency for DAG syncing, or you need near-instant DAG updates without polling intervals.

**3. Bake DAGs into custom image:**
Build a custom Docker image that includes your DAGs. Every DAG change requires a new image build and Helm upgrade. Slower iteration but more reproducible.

```dockerfile
FROM apache/airflow:2.9.0
COPY dags/ /opt/airflow/dags/
RUN pip install your-custom-packages
```

### KubernetesExecutor: Tasks as Pods

With `KubernetesExecutor`, each Airflow task runs as its own Kubernetes Pod — spawned on demand, cleaned up after completion. No persistent worker pool needed.

```
Scheduler → Kubernetes API → Pod per Task
                                ├── Task A pod (running)
                                ├── Task B pod (running)
                                └── Task C pod (pending)
```

Benefits:
- True isolation per task — no shared environment between tasks
- Each task can have its own resource requests, Docker image, and node selector
- Scales to zero between tasks — no idle workers burning compute

### Upgrading Airflow with Helm

Going from Airflow 2.8 to 2.9:

```bash
# 1. Update your custom image tag in values.yaml
# 2. Helm upgrade — rolling restart with no downtime
helm upgrade airflow apache-airflow/airflow \
  -n airflow \
  -f values.yaml

# Helm shows you the diff before applying:
helm diff upgrade airflow apache-airflow/airflow -f values.yaml
```

Helm handles the rolling update — new pods come up, old ones drain, Kubernetes routes traffic only to healthy pods.

### What the Modern Setup Looks Like

```
[Kubernetes Cluster]
│
├── namespace: airflow
│   ├── Deployment: airflow-webserver (2 replicas)
│   ├── Deployment: airflow-scheduler (1 replica)
│   ├── Deployment: airflow-triggerer (1 replica)
│   ├── DaemonSet / StatefulSet: airflow-worker (3 replicas, CeleryExecutor)
│   ├── ConfigMap: airflow-config
│   ├── Secret: airflow-metadata-db-credentials
│   ├── Secret: git-credentials
│   ├── PVC: logs-volume
│   ├── Service: airflow-webserver (ClusterIP + Ingress)
│   └── ServiceAccount + RBAC (for KubernetesExecutor pod creation)
│
└── External dependencies:
    ├── PostgreSQL (Cloud SQL / RDS / self-hosted)
    ├── Redis (if CeleryExecutor)
    └── Object Storage (GCS / S3 for remote logs)
```

### Pros of Helm on Kubernetes

- **Self-healing:** Kubernetes restarts crashed pods automatically
- **Rolling updates:** zero-downtime upgrades via Helm upgrade
- **GitOps-friendly:** `values.yaml` in Git; CD pipelines trigger `helm upgrade`
- **Per-task isolation:** KubernetesExecutor gives each task its own pod and Python environment
- **Auto-scaling:** HPA can scale workers based on queue depth
- **Cloud-native:** native secrets, PVCs, service accounts, IAM integration
- **Resource governance:** CPU/memory limits per component prevent one runaway DAG from killing the scheduler
- **Multi-environment parity:** same Helm chart, different `values.yaml` per env (dev/staging/prod)

### Cons and Operational Considerations

- **Kubernetes knowledge required:** your team must understand pods, PVCs, RBAC, ingress
- **Complexity of values.yaml:** the official Airflow Helm chart has hundreds of configurable values
- **Cold start latency (KubernetesExecutor):** each task incurs pod scheduling latency (~5–30s) vs. a warm Celery worker
- **Spot/preemptible nodes need care:** if the scheduler pod is on a Spot node, eviction causes a brief scheduling pause — use node affinity or priority classes to keep the scheduler on stable nodes
- **Log management:** logs are ephemeral in pods; must configure remote logging (GCS, S3, Elasticsearch) from day one

---

## Side-by-Side Comparison

| Dimension | VM | Docker / Compose | Helm on Kubernetes |
|---|---|---|---|
| **Setup complexity** | Low | Medium | High |
| **Ops knowledge needed** | Linux admin | Docker | Kubernetes + Helm |
| **Scaling workers** | Manual (new VM) | Semi-manual | `helm upgrade` or HPA |
| **DAG deployment** | SSH + file copy | Volume mount or image rebuild | git-sync or image rebuild |
| **Dependency isolation** | None | Container-level | Pod-level (K8s Executor) |
| **Self-healing** | Manual / systemd | Docker restart policy | Kubernetes native |
| **Upgrade safety** | Risky | Medium | Rolling, zero-downtime |
| **Local dev** | Hard | Easy (docker-compose) | Medium (kind / minikube) |
| **Production readiness** | Low | Medium | High |
| **GitOps / CD** | Hard | Possible | Native |

---

## The Trajectory

The evolution follows a clear pattern: each era solved the failure mode of the previous one.

**VM era failure:** Configuration drift, no isolation, manual everything.
→ **Docker fixed:** Reproducible environments, portable images.

**Docker era failure:** Single-host, no self-healing, poor scaling story for production.
→ **Helm/K8s fixed:** Cluster-aware scheduling, self-healing, rolling deploys, cloud-native.

Today, the **Helm + Kubernetes** path is the industry standard for any Airflow deployment that needs to be reliable, scalable, and maintainable by a team. The official Apache Airflow Helm chart is actively maintained by the Airflow project and tracks every Airflow release closely.

For teams just getting started, running the official `docker-compose.yaml` locally and the Helm chart in production is the most pragmatic approach: simple local iteration, production-grade deployment.
