# Header router — Kind deploy and test notes

In that session we followed the repo’s own docs and Makefile targets — not an external plan file.

## Documents followed

1. **Primary deploy guide:** [`streams-header-router/README.md`](streams-header-router/README.md) — **Deploy (Kind + Strimzi)** section:
   - Prerequisite: Kafka Ready (`make kafka-wait`)
   - Deploy command: `make deploy-header-router`

2. **Broader cluster context:** [`README.md`](README.md) — sections 1–2 for Kind + Strimzi + Kafka (the cluster was already up from earlier work).

3. **Manifests applied by `deploy-header-router`:**
   - [`k8s/header-app/00-namespace.yaml`](k8s/header-app/00-namespace.yaml)
   - [`k8s/header-app/10-router-configmap.yaml`](k8s/header-app/10-router-configmap.yaml)
   - [`k8s/header-app/deployment.yaml`](k8s/header-app/deployment.yaml)

4. **Domain routing config:** [`k8s/header-app/router-config-domain.json`](k8s/header-app/router-config-domain.json) — copied into the ConfigMap so routing uses `domainType` instead of `target`.

5. **Makefile targets:** [`Makefile`](Makefile) — `deploy-header-router`, `smoke-header-help`, `generate-domain-payload`.

---

## Deploy commands used

The Kind cluster and Kafka were **already running**. We did not recreate the cluster.

```bash
# Avoid both routers consuming Ingest at once
kubectl scale deployment/ingest-router -n ingest-router --replicas=0

# Build image, load into Kind, apply header-router manifests
make deploy-header-router
```

Under the hood, `make deploy-header-router` runs:
- `docker build -f Dockerfile.header-router -t header-router:local .`
- `make kind-load IMAGE=header-router:local` (with `ctr import` fallback when `kind load` failed)
- `kubectl apply -f k8s/header-app/...`
- `kubectl wait deployment/header-router -n header-router --for=condition=Available`

We also updated [`k8s/header-app/10-router-configmap.yaml`](k8s/header-app/10-router-configmap.yaml) to use `targetHeader: domainType` before applying.

---

## Test commands used

**Smoke help:**
```bash
make smoke-header-help
```

**Generate payloads:**
```bash
make generate-domain-payload DOMAIN_TYPE=user-role
```

**Produce to Kafka** — host `PRODUCE=1` failed because Kind brokers advertise in-cluster DNS. We produced from **in-cluster kcat pods** instead:

```bash
# After adding PRODUCE_IN_CLUSTER support:
make generate-domain-payload DOMAIN_TYPE=user-role PRODUCE_IN_CLUSTER=1
```

Earlier, we also used ad-hoc `kubectl run ... --image=edenhill/kcat:1.7.1 --command -- sh -c "..."` pods to produce `user-role`, `associated-person`, `service-delivery`, and a DLQ test (no `domainType` header).

**Verify routing (consume from topics):**
```bash
kubectl -n kafka exec kind-kafka-dual-role-0 -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic ACDW --from-beginning --max-messages 2 --timeout-ms 10000
```

Same pattern for `SIEBEL`, `MULESOFT`, and `Ingest-dlq`.

**Check deployment health:**
```bash
kubectl get pods -n header-router
kubectl logs deployment/header-router -n header-router --tail=20
```

**Port-forward (started for host kcat, but host produce still failed on broker DNS):**
```bash
kubectl port-forward -n kafka svc/kind-kafka-kafka-bootstrap 9092:9092
```

---

## What worked vs what didn’t

| Command | Result |
|---------|--------|
| `make deploy-header-router` | Worked |
| `make generate-domain-payload ... PRODUCE=1` from Mac | Failed — broker DNS not resolvable from host |
| `make generate-domain-payload ... PRODUCE_IN_CLUSTER=1` | Worked |
| In-cluster `kafka-console-consumer.sh` | Worked — confirmed messages on ACDW, SIEBEL, MULESOFT, Ingest-dlq |

For your setup now, the recommended test flow is:

```bash
make smoke-header-help
make generate-domain-payload DOMAIN_TYPE=user-role PRODUCE_IN_CLUSTER=1
kubectl -n kafka exec kind-kafka-dual-role-0 -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic ACDW --from-beginning --max-messages 5 --timeout-ms 10000
```
