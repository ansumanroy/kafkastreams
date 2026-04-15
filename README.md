# Kind + KRaft Kafka + Ingest router (Kafka Streams)

Apache Kafka runs on Kubernetes in **KRaft** mode via [Strimzi](https://strimzi.io/) **0.48.0**. A small **Kafka Streams** app reads routing rules from a **JSON file** (ConfigMap): ingest topic, DLQ topic, the JSON field used as the routing key (default `target`), and a map from that field’s value to a **Kafka topic name**. Example: `"target":"ACDW"` routes to topic **`ACDW`** when the config lists that key. Unknown keys, invalid JSON, or invalid topic names in config go to the configured DLQ.

## Prerequisites

- [kind](https://kind.sigs.k8s.io/) (some Docker Desktop setups still break `kind load`; `make deploy-router` falls back to `docker save | ctr import -`)
- `kubectl`
- Docker (for building the router image)
- JDK 17 and Maven (optional; only for local builds without Docker)

## 1. Create a Kind cluster

```bash
kind create cluster --name kind
```

If Kafka pods go out-of-memory, recreate the cluster with more resources (see optional [`kind-config.yaml`](kind-config.yaml)).

## 2. Deploy Strimzi + Kafka + topics

```bash
make strimzi-install
make kafka-apply
make kafka-wait
```

This installs the operator into namespace **`kafka`** (override with `STRIMZI_NAMESPACE` if needed), applies a single-node **KRaft** cluster (`kind-kafka`) with ephemeral storage, and creates topics **`Ingest`**, **`ACDW`**, **`MULESOFT`**, **`GDW`**, **`SIEBEL`**, and **`Ingest-dlq`**. Auto-creation of topics is disabled on the broker; add more `KafkaTopic` manifests if you introduce new routing targets.

The Strimzi release YAML hard-codes ServiceAccount subjects as `namespace: myproject`. `make strimzi-install` rewrites those lines to your target namespace before `kubectl apply`, then restarts the operator so leader election and reconciliation work.

## 3. Build and deploy the Kafka Streams router

```bash
make deploy-router
```

This builds image `ingest-router:local`, loads it into Kind, and applies [`k8s/app/10-router-configmap.yaml`](k8s/app/10-router-configmap.yaml) plus [`k8s/app/deployment.yaml`](k8s/app/deployment.yaml) (router config is mounted at `/etc/router/config.json`).

Equivalent one-shot script: [`scripts/deploy.sh`](scripts/deploy.sh) (honours `CLUSTER_NAME` and `IMAGE`).

## 4. Smoke test

Print copy-paste commands:

```bash
make smoke-producer-help
```

**Producer** (paste a single-line JSON, then Ctrl+D):

```json
{"eventId":"evt-1","target":"ACDW","eventType":"PROVIDER_UPSERT"}
```

**Consumer** on `ACDW` should show the same line. Send `not-json` to **`Ingest`** and read **`Ingest-dlq`** to verify the dead-letter path.

## Configuration

### Router JSON ([`k8s/app/router-config.json`](k8s/app/router-config.json))

The pod loads **`ROUTER_CONFIG_PATH`** (default `/etc/router/config.json`). The same content is shipped as ConfigMap key `config.json` in [`k8s/app/10-router-configmap.yaml`](k8s/app/10-router-configmap.yaml).

| Field | Required | Description |
|-------|----------|-------------|
| `ingestTopic` | yes | Topic to consume (must exist; Strimzi `KafkaTopic`) |
| `dlqTopic` | yes | Topic for unroutable / invalid payloads |
| `targetField` | no | JSON field to read for the route key (default `target`) |
| `routes` | yes | Object: **route key** (string in the message) → **Kafka topic name** (must exist). Use this for aliases, e.g. `"SFDC": "SIEBEL"`. |

Topic names must match `[a-zA-Z0-9._-]+` and length ≤ 249. The broker has **`auto.create.topics.enable: false`** — add a **`KafkaTopic`** CR for **`ingestTopic`**, **`dlqTopic`**, and **every value** in `routes`.

To change routing without rebuilding the image: edit the ConfigMap (or `router-config.json` and re-apply), then restart the Deployment:

```bash
kubectl apply -f k8s/app/10-router-configmap.yaml
kubectl rollout restart deployment/ingest-router -n ingest-router
```

### Environment variables (Deployment)

| Variable | Purpose |
|----------|---------|
| `BOOTSTRAP_SERVERS` | Kafka bootstrap (in-cluster DNS) |
| `APPLICATION_ID` | Kafka Streams application id |
| `ROUTER_CONFIG_PATH` | Path to JSON file inside the container (default `/etc/router/config.json`) |
| `NUM_STREAM_THREADS` | Optional Streams threads (default `1`) |

Local runs (outside Kubernetes): point `ROUTER_CONFIG_PATH` at a file such as [`k8s/app/router-config.json`](k8s/app/router-config.json).

## Optional: Parallel ksqlDB router on MSK (separate namespace)

This repository also includes an **additive** ksqlDB implementation under [`ksql/`](ksql/) and [`k8s/ksqldb/`](k8s/ksqldb/). It does **not** replace the Java router in [`streams-router/`](streams-router/); run it in a separate environment/namespace.

### 1) Prepare namespace and ksqlDB manifests

```bash
kubectl apply -f k8s/ksqldb/00-namespace.yaml
kubectl apply -f k8s/ksqldb/10-ksqldb-configmap.yaml
```

Create a real Secret from the example (do not commit real credentials):

```bash
cp k8s/ksqldb/20-ksqldb-secret.example.yaml /tmp/20-ksqldb-secret.yaml
# edit /tmp/20-ksqldb-secret.yaml with real SCRAM credentials
kubectl apply -f /tmp/20-ksqldb-secret.yaml
```

Deploy ksqlDB:

```bash
kubectl apply -f k8s/ksqldb/30-deployment.yaml
kubectl apply -f k8s/ksqldb/40-service.yaml
```

Before applying, update `KSQL_BOOTSTRAP_SERVERS` in [`k8s/ksqldb/10-ksqldb-configmap.yaml`](k8s/ksqldb/10-ksqldb-configmap.yaml) with your MSK SASL_SSL brokers (typically `:9096`).

### 2) Apply routing SQL

Port-forward the ksqlDB API:

```bash
kubectl -n ingest-router-ksqldb port-forward svc/ksqldb-server 8088:8088
```

In another terminal, run the statements in order:

```bash
python3 - <<'PY'
import json
import pathlib
import urllib.request

url = "http://localhost:8088/ksql"
headers = {"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"}

for file_name in ["ksql/01-streams.sql", "ksql/02-routing.sql", "ksql/03-dlq.sql"]:
    ksql = pathlib.Path(file_name).read_text()
    payload = json.dumps({"ksql": ksql, "streamsProperties": {}}).encode()
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    with urllib.request.urlopen(req) as resp:
        print(f"{file_name}: HTTP {resp.status}")
PY
```

### 3) Verify behavior

- Valid payloads route by `target` to topics `ACDW`, `MULESOFT`, `GDW`, `SIEBEL`.
- Unknown, blank, missing `target`, and malformed JSON route to `Ingest-dlq`.
- Check ksqlDB query status:

```bash
curl -sS http://localhost:8088/info
curl -sS -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
  --data '{"ksql":"SHOW QUERIES;","streamsProperties":{}}'
```

## Troubleshooting

**`make kafka-wait` reports `kafkas.kafka.strimzi.io "kind-kafka" not found`**

Strimzi only reconciles `Kafka` resources in the **`kafka`** namespace (where the operator runs). Older copies of this repo applied manifests without a namespace, which put the CRs in **`default`** instead. Re-apply with the current manifests (`make kafka-apply`), then optionally remove stray objects:

```bash
kubectl delete kafka,kafkanodepool,kafkatopic --all -n default --ignore-not-found
```

**`kubectl wait` and the wrong cluster**

Ensure your context points at Kind: `kubectl config use-context kind-kind` (for `kind create cluster --name kind`).

**Operator logs: `leases ... is forbidden` / leader election errors**

The cluster operator’s ServiceAccount must be bound with subjects in the **same** namespace where the operator runs. If Strimzi was installed from the raw GitHub YAML without rewriting `myproject`, re-run:

```bash
make strimzi-install
```

That reapplies RBAC with corrected subjects and restarts the deployment.

**`make deploy-router` / `kind load`: `failed to detect containerd snapshotter`**

This still appears for some **Docker Desktop + kindest/node** combinations even with a current Kind CLI. **`make kind-load`** falls back to **`docker save … | docker exec -i … ctr -n=k8s.io images import -`** (stdin), which avoids copying a tarball into `/tmp` inside the node (that path can fail with “no such file” on Desktop).

Upgrading Kind or Docker Desktop may restore native `kind load`; the pipe fallback is the reliable fix used here.

## Layout

- [`k8s/strimzi/`](k8s/strimzi/) — namespace, KRaft `Kafka` + `KafkaNodePool`, `KafkaTopic` resources
- [`streams-router/`](streams-router/) — Java 17 Maven app
- [`Dockerfile`](Dockerfile) — multi-stage build for the router
- [`k8s/app/`](k8s/app/) — router `Deployment`, ConfigMap, sample [`router-config.json`](k8s/app/router-config.json)
- [`k8s/msk/`](k8s/msk/) — MSK-focused Java router manifests (SASL/SCRAM env-based)
- [`k8s/ksqldb/`](k8s/ksqldb/) — dedicated namespace + ksqlDB server manifests for MSK
- [`ksql/`](ksql/) — persistent query SQL files (source stream, routing, DLQ)
