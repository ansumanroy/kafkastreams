STRIMZI_VERSION   := 0.48.0
STRIMZI_NAMESPACE ?= kafka
KAFKA_IMAGE       := quay.io/strimzi/kafka:$(STRIMZI_VERSION)-kafka-4.1.0
CLUSTER_NAME      ?= kind
IMAGE             ?= ingest-router:local

.PHONY: strimzi-install kafka-apply kafka-wait docker-build kind-load kind-load-ctr app-apply app-wait deploy-router msk-app-apply msk-app-wait deploy-router-msk smoke-producer-help

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

docker-build:
	docker build -t $(IMAGE) .

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
