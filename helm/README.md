# Helm charts

Parallel install path for the apps under [`k8s/`](../k8s/). Raw manifests remain supported via existing Make `*-apply` targets.

## Charts

| Chart | Path | Purpose |
|-------|------|---------|
| `strimzi-kafka` | [`charts/strimzi-kafka`](charts/strimzi-kafka) | KRaft `Kafka` + `KafkaNodePool` + topics |
| `ingest-router` | [`charts/ingest-router`](charts/ingest-router) | JSON body-field router (Kind or MSK via values) |
| `header-router` | [`charts/header-router`](charts/header-router) | Header-based router (Kind or MSK via values) |

The Strimzi **operator** is not packaged here. Install it first with `make strimzi-install`.

## Install (Make)

```bash
make strimzi-install
make helm-install-kafka
make kafka-wait

# Build/load images first for Kind:
make docker-build && make kind-load
make helm-install-router

make docker-build-header
make kind-load IMAGE=header-router:local
make helm-install-header-router
```

MSK overlays (edit credentials/bootstrap in `values-msk.yaml` first):

```bash
make helm-install-router-msk
make helm-install-header-router-msk
```

Uninstall:

```bash
make helm-uninstall-router
make helm-uninstall-header-router
make helm-uninstall-kafka
```

## Install (helm CLI)

```bash
helm upgrade --install strimzi-kafka ./helm/charts/strimzi-kafka -n kafka --create-namespace

helm upgrade --install ingest-router ./helm/charts/ingest-router -n ingest-router --create-namespace

helm upgrade --install ingest-router-msk ./helm/charts/ingest-router \
  -n ingest-router-msk --create-namespace \
  -f ./helm/charts/ingest-router/values-msk.yaml

helm upgrade --install header-router ./helm/charts/header-router -n header-router --create-namespace

helm upgrade --install header-router-msk ./helm/charts/header-router \
  -n header-router-msk --create-namespace \
  -f ./helm/charts/header-router/values-msk.yaml
```

## Values notes

- Router Kind defaults: plaintext `bootstrapServers` to in-cluster Strimzi.
- Router MSK: set `kafka.auth.enabled: true` (via `values-msk.yaml`), replace username/password and broker list.
- `routerConfig` is rendered into the ConfigMap `config.json` key.
- Do not run ingest-router and header-router against the same Ingest topic unless you intend both consumer groups to process every record.
