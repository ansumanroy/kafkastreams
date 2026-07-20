# Kind + KRaft Kafka + Ingest router (Kafka Streams)

Apache Kafka runs on Kubernetes in **KRaft** mode via [Strimzi](https://strimzi.io/) **0.48.0**. A small **Kafka Streams** app reads routing rules from a **JSON file** (ConfigMap): ingest topic, DLQ topic, the JSON field used as the routing key (default `target`), and a map from that field’s value to a **Kafka topic name**. Example: `"target":"ACDW"` routes to topic **`ACDW`** when the config lists that key. Unknown keys, invalid JSON, or invalid topic names in config go to the configured DLQ.

The same router binary now supports both:
- local in-cluster Kafka (Strimzi plaintext) via `k8s/app/`
- Amazon MSK with SASL/SCRAM via `k8s/msk-app/`

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

## 3. Build and deploy the Kafka Streams router (local Strimzi)

```bash
make deploy-router
```

This builds image `ingest-router:local`, loads it into Kind, and applies [`k8s/app/10-router-configmap.yaml`](k8s/app/10-router-configmap.yaml) plus [`k8s/app/deployment.yaml`](k8s/app/deployment.yaml) (router config is mounted at `/etc/router/config.json`).

Equivalent one-shot script: [`scripts/deploy.sh`](scripts/deploy.sh) (honours `CLUSTER_NAME` and `IMAGE`).

## 4. Deploy a separate MSK router app (SASL/SCRAM)

MSK deployment manifests live in [`k8s/msk-app/`](k8s/msk-app/), in a separate namespace (`ingest-router-msk`) so you can run and observe it independently from the local router.

Before deploying, update:
- [`k8s/msk-app/05-msk-bootstrap-configmap.yaml`](k8s/msk-app/05-msk-bootstrap-configmap.yaml): `BOOTSTRAP_SERVERS` to your MSK broker list (comma-separated host:port).
- [`k8s/msk-app/secrets.yaml`](k8s/msk-app/secrets.yaml): SCRAM `username` and `password` (placeholders only in git; use a real Secret source in production if you prefer not to commit credentials).

Deploy:

```bash
make deploy-router-msk
```

This applies [`k8s/msk-app/05-msk-bootstrap-configmap.yaml`](k8s/msk-app/05-msk-bootstrap-configmap.yaml), [`k8s/msk-app/secrets.yaml`](k8s/msk-app/secrets.yaml), [`k8s/msk-app/10-router-configmap.yaml`](k8s/msk-app/10-router-configmap.yaml), and [`k8s/msk-app/deployment.yaml`](k8s/msk-app/deployment.yaml), and configures Kafka client auth with:
- `KAFKA_SECURITY_PROTOCOL=SASL_SSL`
- `KAFKA_SASL_MECHANISM=SCRAM-SHA-512`
- `BOOTSTRAP_SERVERS` from ConfigMap `msk-bootstrap`
- `KAFKA_SASL_USERNAME`/`KAFKA_SASL_PASSWORD` from Secret `msk-scram-credentials`

## 5. Smoke test

Print copy-paste commands:

```bash
make smoke-producer-help
```

**Producer** (paste a single-line JSON, then Ctrl+D):

```json
{"eventId":"evt-1","target":"ACDW","eventType":"PROVIDER_UPSERT"}
```

**Consumer** on `ACDW` should show the same line. Send `not-json` to **`Ingest`** and read **`Ingest-dlq`** to verify the dead-letter path.

### Header router (record header routing)

Print copy-paste commands:

```bash
make smoke-header-help
```

The route key is a **Kafka record header** (default name `target`), not a JSON field. Body stays opaque — any bytes work.

**Payload** (example JSON body; routing uses the header only):

```json
{"eventId":"evt-1","eventType":"PROVIDER_UPSERT"}
```

**Producer** (requires `kcat` / `kafkacat` to set headers; host must reach Kafka bootstrap, e.g. via port-forward):

```bash
echo '{"eventId":"evt-1","eventType":"PROVIDER_UPSERT"}' | kcat -b localhost:9092 -t Ingest -P -H target=ACDW
```

**Consumer** on `ACDW` should show the same JSON line. Omit the header or send an unknown value and read **`Ingest-dlq`**.

Do not run ingest-router and header-router against the same **`Ingest`** topic at once unless you intentionally want both consumer groups to process every record.

#### Domain events (`domainType` routing)

For EventXchange-style payloads, set the routing header to **`domainType`** (not `target`). Extra headers (`correlationId`, `transactionId`, `sourceSystem`) are ignored for routing but forwarded with the record.

Generate random payloads:

```bash
make generate-domain-payload DOMAIN_TYPE=user-role PRINT_KCAT=1
# or: python3 scripts/generate-domain-payload.py --domain-type user-role --print-kcat
```

Allowed `domainType` values: `programpayment-entity`, `associated-provider`, `service-delivery`, `associated-person`, `user-role`.

**Headers example:**

```text
correlationId:fce60900-7f19-11f1-b484-0685e12e0df9
transactionId:9dc02aa3-ca18-46a0-b836-8bfdd1c157dc
domainType:user-role
sourceSystem:SF_PRV2_2
```

**Payload sample** (`domain_key` and IDs match the headers):

```json
{
  "_meta": {
    "transaction_metadata": {
      "transaction_id": "9dc02aa3-ca18-46a0-b836-8bfdd1c157dc",
      "timestamp": "2026-07-14T00:21:43.589Z",
      "sub_domain_record_id": null,
      "sub_domain_key": null,
      "source_system": "EVENTXCHANGE",
      "source_entity_id": "07k9200000WiQR1AAN",
      "last_modified_date_time": null,
      "domain_record_id": "07k9200000WiQR1AAN",
      "domain_key": "user-role",
      "created_date_time": null,
      "correlation_id": "fce60900-7f19-11f1-b484-0685e12e0df9"
    }
  },
  "data": {
    "IsActive": true,
    "ACRId": "07k9200000WiQR1AAN",
    "AccountId": "0019200001ATihBAAT",
    "ContactId": "003920000189ajvAAA",
    "Roles": "Provider Staff (Registered Provider)",
    "StartDate": "2026-07-14",
    "EndDate": null,
    "LastModifiedDate": "2026-07-14T00:21:39.000Z"
  }
}
```

**Producer:**

```bash
echo '<payload-json>' | kcat -b localhost:9092 -t Ingest -P \
  -H correlationId=fce60900-7f19-11f1-b484-0685e12e0df9 \
  -H transactionId=9dc02aa3-ca18-46a0-b836-8bfdd1c157dc \
  -H domainType=user-role \
  -H sourceSystem=SF_PRV2_2
```

**Header-router config** for this contract ([`k8s/header-app/router-config-domain.json`](k8s/header-app/router-config-domain.json)) — edit the `routes` map to match your destination systems:

```json
{
  "ingestTopic": "Ingest",
  "dlqTopic": "Ingest-dlq",
  "targetHeader": "domainType",
  "routes": {
    "programpayment-entity": "GDW",
    "associated-provider": "ACDW",
    "service-delivery": "MULESOFT",
    "associated-person": "SIEBEL",
    "user-role": "ACDW"
  }
}
```

To apply: copy those fields into [`k8s/header-app/10-router-configmap.yaml`](k8s/header-app/10-router-configmap.yaml) (or Helm `routerConfig`), then restart the Deployment. Missing or unknown `domainType` → **`Ingest-dlq`**.

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
| `KAFKA_SECURITY_PROTOCOL` | Optional Kafka client protocol (for MSK set `SASL_SSL`) |
| `KAFKA_SASL_MECHANISM` | Optional SASL mechanism (for MSK SCRAM set `SCRAM-SHA-512`) |
| `KAFKA_SASL_USERNAME` | Optional SASL username (must be set together with password) |
| `KAFKA_SASL_PASSWORD` | Optional SASL password (must be set together with username) |

Local runs (outside Kubernetes): point `ROUTER_CONFIG_PATH` at a file such as [`k8s/app/router-config.json`](k8s/app/router-config.json).

For the separate MSK app:
- Bootstrap brokers: ConfigMap [`k8s/msk-app/05-msk-bootstrap-configmap.yaml`](k8s/msk-app/05-msk-bootstrap-configmap.yaml) (`msk-bootstrap`, key `BOOTSTRAP_SERVERS`)
- SCRAM credentials: Secret [`k8s/msk-app/secrets.yaml`](k8s/msk-app/secrets.yaml) (`msk-scram-credentials`, keys `username`, `password`)

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
- [`streams-router/`](streams-router/) — Java 17 Maven app (JSON body field routing)
- [`streams-header-router/`](streams-header-router/) — Java 17 app that routes by Kafka record header; see its [README](streams-header-router/README.md) for design and gotchas
- [`Dockerfile`](Dockerfile) — multi-stage build for the ingest router
- [`Dockerfile.header-router`](Dockerfile.header-router) — multi-stage build for the header router
- [`k8s/app/`](k8s/app/) — local Strimzi router `Deployment`, ConfigMap, sample [`router-config.json`](k8s/app/router-config.json)
- [`k8s/msk-app/`](k8s/msk-app/) — separate MSK router `Deployment` with SASL/SCRAM env wiring
- [`k8s/header-app/`](k8s/header-app/) — local Strimzi header-router `Deployment` and ConfigMap; domain routing sample [`router-config-domain.json`](k8s/header-app/router-config-domain.json)
- [`scripts/generate-domain-payload.py`](scripts/generate-domain-payload.py) — random domain-event payloads for header-router smoke tests
- [`k8s/header-msk-app/`](k8s/header-msk-app/) — MSK header-router `Deployment` with SASL/SCRAM env wiring
- [`helm/charts/`](helm/charts/) — Helm charts for Strimzi Kafka, ingest-router, and header-router (see [`helm/README.md`](helm/README.md))
