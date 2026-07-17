STRIMZI_VERSION   := 0.48.0
STRIMZI_NAMESPACE ?= kafka
KAFKA_IMAGE       := quay.io/strimzi/kafka:$(STRIMZI_VERSION)-kafka-4.1.0
CLUSTER_NAME      ?= kind
IMAGE             ?= ingest-router:local
HEADER_IMAGE      ?= header-router:local
STREAMS_ROUTER_DIR := streams-router
STREAMS_HEADER_ROUTER_DIR := streams-header-router
MVN               ?= mvn
GRADLE            ?= gradle

HELM_CHARTS_DIR := helm/charts

.DEFAULT_GOAL := help

.PHONY: help build build-maven build-gradle docker-build \
	build-header build-header-maven build-header-gradle docker-build-header \
	strimzi-install kafka-apply kafka-wait kind-load kind-load-ctr \
	app-apply app-wait deploy-router \
	msk-app-apply msk-app-wait deploy-router-msk \
	header-app-apply header-app-wait deploy-header-router \
	header-msk-app-apply header-msk-app-wait deploy-header-router-msk \
	helm-install-kafka helm-uninstall-kafka \
	helm-install-router helm-uninstall-router \
	helm-install-router-msk helm-uninstall-router-msk \
	helm-install-header-router helm-uninstall-header-router \
	helm-install-header-router-msk helm-uninstall-header-router-msk \
	smoke-producer-help smoke-header-help

# Default: list phony targets (make with no arguments).
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Phony targets:"
	@echo "  help                          Show this help (default)"
	@echo ""
	@echo "  Build"
	@echo "    build                       Maven package streams-router (default Java build)"
	@echo "    build-maven                 Maven package streams-router"
	@echo "    build-gradle                Gradle build streams-router"
	@echo "    docker-build                Build ingest-router image (\$$IMAGE)"
	@echo "    build-header                Maven package streams-header-router"
	@echo "    build-header-maven          Maven package streams-header-router"
	@echo "    build-header-gradle         Gradle build streams-header-router"
	@echo "    docker-build-header         Build header-router image (\$$HEADER_IMAGE)"
	@echo ""
	@echo "  Kafka / Kind"
	@echo "    strimzi-install             Install Strimzi operator"
	@echo "    kafka-apply                 Apply Kafka + topics (kubectl)"
	@echo "    kafka-wait                  Wait for Kafka Ready"
	@echo "    kind-load                   Load \$$IMAGE into Kind"
	@echo "    kind-load-ctr               Fallback image load via ctr"
	@echo ""
	@echo "  Deploy (kubectl)"
	@echo "    deploy-router               Build/load/apply ingest-router"
	@echo "    deploy-router-msk           Build/load/apply ingest-router MSK"
	@echo "    deploy-header-router        Build/load/apply header-router"
	@echo "    deploy-header-router-msk    Build/load/apply header-router MSK"
	@echo "    app-apply / app-wait        ingest-router Kind manifests"
	@echo "    msk-app-apply / msk-app-wait"
	@echo "    header-app-apply / header-app-wait"
	@echo "    header-msk-app-apply / header-msk-app-wait"
	@echo ""
	@echo "  Helm"
	@echo "    helm-install-kafka / helm-uninstall-kafka"
	@echo "    helm-install-router / helm-uninstall-router"
	@echo "    helm-install-router-msk / helm-uninstall-router-msk"
	@echo "    helm-install-header-router / helm-uninstall-header-router"
	@echo "    helm-install-header-router-msk / helm-uninstall-header-router-msk"
	@echo ""
	@echo "  Smoke"
	@echo "    smoke-producer-help         Print ingest-router smoke commands"
	@echo "    smoke-header-help           Print header-router smoke commands"

# Default local Java build uses Maven (matches the Dockerfile).
build: build-maven

build-maven:
	$(MVN) -B -f $(STREAMS_ROUTER_DIR)/pom.xml package -DskipTests

build-gradle:
	cd $(STREAMS_ROUTER_DIR) && $(GRADLE) build -x test

# Multi-stage image: Maven package inside the build stage, JRE runtime.
docker-build:
	docker build -t $(IMAGE) .

build-header: build-header-maven

build-header-maven:
	$(MVN) -B -f $(STREAMS_HEADER_ROUTER_DIR)/pom.xml package -DskipTests

build-header-gradle:
	cd $(STREAMS_HEADER_ROUTER_DIR) && $(GRADLE) build -x test

docker-build-header:
	docker build -f Dockerfile.header-router -t $(HEADER_IMAGE) .

# Upstream YAML uses "namespace: myproject" on ServiceAccount subjects in RoleBindings.
# kubectl -n kafka does not rewrite those; leader election then 403s on leases. Rewrite subjects to match STRIMZI_NAMESPACE.
strimzi-install:
	kubectl create namespace $(STRIMZI_NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	curl -fsSL "https://github.com/strimzi/strimzi-kafka-operator/releases/download/$(STRIMZI_VERSION)/strimzi-cluster-operator-$(STRIMZI_VERSION).yaml" \
		| sed 's/^    namespace: myproject$$/    namespace: $(STRIMZI_NAMESPACE)/' \
		| kubectl apply -n $(STRIMZI_NAMESPACE) -f -
	kubectl rollout restart deployment/strimzi-cluster-operator -n $(STRIMZI_NAMESPACE)
	kubectl rollout status deployment/strimzi-cluster-operator -n $(STRIMZI_NAMESPACE) --timeout=120s

kafka-apply:
	kubectl apply -f k8s/strimzi/00-namespace.yaml
	kubectl apply -n $(STRIMZI_NAMESPACE) -f k8s/strimzi/10-kafka-kraft.yaml
	kubectl apply -n $(STRIMZI_NAMESPACE) -f k8s/strimzi/20-kafka-topics.yaml

kafka-wait:
	kubectl wait kafka/kind-kafka -n $(STRIMZI_NAMESPACE) --for=condition=Ready --timeout=600s

# kind load can fail on some Docker Desktop + node combos ("failed to detect containerd snapshotter").
# Fallback: stream image tar into ctr import stdin (docker cp to /tmp is unreliable inside kindest/node).
KIND_CONTROL_PLANE := $(CLUSTER_NAME)-control-plane

kind-load:
	@kind load docker-image "$(IMAGE)" --name "$(CLUSTER_NAME)" || \
		( echo "kind load failed; piping docker save into ctr images import -" && \
		  $(MAKE) kind-load-ctr )

kind-load-ctr:
	docker save "$(IMAGE)" | docker exec -i $(KIND_CONTROL_PLANE) ctr -n=k8s.io images import -

app-apply:
	kubectl apply -f k8s/app/00-namespace.yaml
	kubectl apply -f k8s/app/10-router-configmap.yaml
	kubectl apply -f k8s/app/deployment.yaml

app-wait:
	kubectl wait deployment/ingest-router -n ingest-router --for=condition=Available --timeout=180s

# Build image, load into Kind, apply router Deployment (Kafka must already be Ready).
deploy-router: docker-build kind-load app-apply app-wait

msk-app-apply:
	kubectl apply -f k8s/msk-app/00-namespace.yaml
	kubectl apply -f k8s/msk-app/05-msk-bootstrap-configmap.yaml
	kubectl apply -f k8s/msk-app/secrets.yaml
	kubectl apply -f k8s/msk-app/10-router-configmap.yaml
	kubectl apply -f k8s/msk-app/deployment.yaml

msk-app-wait:
	kubectl wait deployment/ingest-router -n ingest-router-msk --for=condition=Available --timeout=180s

# Build image, load into Kind, apply MSK router Deployment.
deploy-router-msk: docker-build kind-load msk-app-apply msk-app-wait

header-app-apply:
	kubectl apply -f k8s/header-app/00-namespace.yaml
	kubectl apply -f k8s/header-app/10-router-configmap.yaml
	kubectl apply -f k8s/header-app/deployment.yaml

header-app-wait:
	kubectl wait deployment/header-router -n header-router --for=condition=Available --timeout=180s

# Build header-router image, load into Kind, apply Deployment (Kafka must already be Ready).
deploy-header-router: docker-build-header
	$(MAKE) kind-load IMAGE=$(HEADER_IMAGE)
	$(MAKE) header-app-apply header-app-wait

header-msk-app-apply:
	kubectl apply -f k8s/header-msk-app/00-namespace.yaml
	kubectl apply -f k8s/header-msk-app/05-msk-bootstrap-configmap.yaml
	kubectl apply -f k8s/header-msk-app/secrets.yaml
	kubectl apply -f k8s/header-msk-app/10-router-configmap.yaml
	kubectl apply -f k8s/header-msk-app/deployment.yaml

header-msk-app-wait:
	kubectl wait deployment/header-router -n header-router-msk --for=condition=Available --timeout=180s

# Build header-router image, load into Kind, apply MSK header-router Deployment.
deploy-header-router-msk: docker-build-header
	$(MAKE) kind-load IMAGE=$(HEADER_IMAGE)
	$(MAKE) header-msk-app-apply header-msk-app-wait

# Helm parallel install path (raw k8s/ kubectl apply targets remain available).
helm-install-kafka:
	helm upgrade --install strimzi-kafka $(HELM_CHARTS_DIR)/strimzi-kafka \
		-n $(STRIMZI_NAMESPACE) --create-namespace

helm-uninstall-kafka:
	helm uninstall strimzi-kafka -n $(STRIMZI_NAMESPACE) --ignore-not-found

helm-install-router:
	helm upgrade --install ingest-router $(HELM_CHARTS_DIR)/ingest-router \
		-n ingest-router --create-namespace

helm-uninstall-router:
	helm uninstall ingest-router -n ingest-router --ignore-not-found

helm-install-router-msk:
	helm upgrade --install ingest-router-msk $(HELM_CHARTS_DIR)/ingest-router \
		-n ingest-router-msk --create-namespace \
		-f $(HELM_CHARTS_DIR)/ingest-router/values-msk.yaml

helm-uninstall-router-msk:
	helm uninstall ingest-router-msk -n ingest-router-msk --ignore-not-found

helm-install-header-router:
	helm upgrade --install header-router $(HELM_CHARTS_DIR)/header-router \
		-n header-router --create-namespace

helm-uninstall-header-router:
	helm uninstall header-router -n header-router --ignore-not-found

helm-install-header-router-msk:
	helm upgrade --install header-router-msk $(HELM_CHARTS_DIR)/header-router \
		-n header-router-msk --create-namespace \
		-f $(HELM_CHARTS_DIR)/header-router/values-msk.yaml

helm-uninstall-header-router-msk:
	helm uninstall header-router-msk -n header-router-msk --ignore-not-found

smoke-producer-help:
	@echo "Producer (interactive paste JSON), same namespace as Kafka:"
	@echo '  kubectl -n kafka run kafka-producer -it --rm --restart=Never --image=$(KAFKA_IMAGE) -- \\'
	@echo '    bin/kafka-console-producer.sh --bootstrap-server kind-kafka-kafka-bootstrap:9092 --topic Ingest'
	@echo "Consumer ACDW:"
	@echo '  kubectl -n kafka run kafka-consume -it --rm --restart=Never --image=$(KAFKA_IMAGE) -- \\'
	@echo '    bin/kafka-console-consumer.sh --bootstrap-server kind-kafka-kafka-bootstrap:9092 --topic ACDW --from-beginning'
	@echo "DLQ:"
	@echo '  kubectl -n kafka run kafka-dlq -it --rm --restart=Never --image=$(KAFKA_IMAGE) -- \\'
	@echo '    bin/kafka-console-consumer.sh --bootstrap-server kind-kafka-kafka-bootstrap:9092 --topic Ingest-dlq --from-beginning'

smoke-header-help:
	@echo "Header router needs a producer that can set record headers (kcat/kafkacat)."
	@echo "Example (from a host that can reach the Kind Kafka bootstrap, or via port-forward):"
	@echo '  echo '"'"'{"eventId":"evt-1","eventType":"PROVIDER_UPSERT"}'"'"' | kcat -b localhost:9092 -t Ingest -P -H target=ACDW'
	@echo "Missing/unknown header goes to DLQ:"
	@echo '  echo '"'"'opaque-payload'"'"' | kcat -b localhost:9092 -t Ingest -P'
	@echo "Consumer ACDW:"
	@echo '  kubectl -n kafka run kafka-consume -it --rm --restart=Never --image=$(KAFKA_IMAGE) -- \\'
	@echo '    bin/kafka-console-consumer.sh --bootstrap-server kind-kafka-kafka-bootstrap:9092 --topic ACDW --from-beginning'
	@echo "DLQ:"
	@echo '  kubectl -n kafka run kafka-dlq -it --rm --restart=Never --image=$(KAFKA_IMAGE) -- \\'
	@echo '    bin/kafka-console-consumer.sh --bootstrap-server kind-kafka-kafka-bootstrap:9092 --topic Ingest-dlq --from-beginning'
	@echo "Note: do not run ingest-router and header-router against the same Ingest topic at once"
	@echo "unless you intentionally want competing consumers."
