# streams-header-router

Kafka Streams app that routes records by a **Kafka record header** (not a JSON body field). Sibling to [`streams-router`](../streams-router/), with its own image, consumer group (`APPLICATION_ID`), and Kubernetes namespaces.

## Why a separate app

- Different contract: producers set a header (default name `target`) instead of embedding a route key in JSON.
- Body stays opaque — no Jackson parse of the value — so binary or non-JSON payloads work.
- Own `APPLICATION_ID` / Deployment so it does not clash with the body-based ingest-router.

## Routing contract

Config (mounted at `ROUTER_CONFIG_PATH`, default `/etc/router/config.json`):

```json
{
  "ingestTopic": "Ingest",
  "dlqTopic": "Ingest-dlq",
  "targetHeader": "target",
  "routes": {
    "ACDW": "ACDW",
    "MULESOFT": "MULESOFT",
    "GDW": "GDW",
    "SIEBEL": "SIEBEL"
  }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `ingestTopic` | yes | Topic to consume |
| `dlqTopic` | yes | Topic for unroutable records |
| `targetHeader` | no | Header name to read (default `target`) |
| `routes` | yes | Header value → Kafka topic name |

Resolver rules:

1. Read `headers().lastHeader(targetHeader)`.
2. Decode value as **UTF-8** (strict); trim whitespace.
3. Lookup in `routes`; if missing / blank / bad UTF-8 / unknown key → **`dlqTopic`**.
4. `TopicNameExtractor` **never returns null** — always a real topic name.

## Topology

Single consume → dynamic sink (no `split` / `branch`):

```text
Ingest ──► HeaderRouterApp ──► routes[header] or dlqTopic
```

Kafka Streams `KStream.split().branch((key, value) -> …)` predicates **cannot see headers**. Routing uses `TopicNameExtractor` with `RecordContext.headers()` instead.

## Gotchas

1. **DSL predicates ignore headers** — do not try to branch on `(k, v)` for header checks; use `TopicNameExtractor` (or Processor API).
2. **Header values are `byte[]`** — this app assumes UTF-8; malformed bytes go to DLQ.
3. **Duplicate header keys** — Kafka allows multiples; this app uses **last header wins** (`lastHeader`).
4. **Header names are case-sensitive** — `target` ≠ `Target`.
5. **Destination topics must already exist** — Streams does not auto-create topics extracted by name (same as ingest-router; this cluster has `auto.create.topics.enable: false`).
6. **Never return null from the extractor** — unresolved traffic must map to DLQ explicitly.
7. **Smoke tests need a header-capable producer** — `kafka-console-producer` is awkward for custom headers; use `kcat` / `kafkacat` (see `make smoke-header-help`).
8. **Do not run both routers on the same ingest topic** unless you intentionally want competing consumers with different `APPLICATION_ID`s (each gets a copy only if they are in different groups — they will both process every record independently). Prefer one owner of `Ingest` at a time in Kind demos.

## Build

```bash
make build-header          # Maven (default)
make build-header-maven
make build-header-gradle
make docker-build-header   # image header-router:local
```

## Deploy (Kind + Strimzi)

Kafka must already be Ready (`make kafka-wait`).

```bash
make deploy-header-router
```

MSK twin (update bootstrap + SCRAM secrets first):

```bash
make deploy-header-router-msk
```

## Smoke

```bash
make smoke-header-help
```

Example produce with header (host must reach Kafka bootstrap, e.g. via port-forward):

```bash
echo '{"eventId":"evt-1"}' | kcat -b localhost:9092 -t Ingest -P -H target=ACDW
```

Missing header → `Ingest-dlq`.

## Layout

- Java sources under `src/main/java/com/example/headerrouter/`
- Local Kind manifests: [`../k8s/header-app/`](../k8s/header-app/)
- MSK manifests: [`../k8s/header-msk-app/`](../k8s/header-msk-app/)
- Image build: [`../Dockerfile.header-router`](../Dockerfile.header-router)

## Env vars

Same pattern as ingest-router: `BOOTSTRAP_SERVERS`, `APPLICATION_ID`, `ROUTER_CONFIG_PATH`, `NUM_STREAM_THREADS`, plus optional `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SASL_*` for MSK.
