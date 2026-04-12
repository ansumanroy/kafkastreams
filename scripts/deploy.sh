#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CLUSTER_NAME="${CLUSTER_NAME:-kind}"
IMAGE="${IMAGE:-ingest-router:local}"
STRIMZI_VERSION="${STRIMZI_VERSION:-0.48.0}"
STRIMZI_NAMESPACE="${STRIMZI_NAMESPACE:-kafka}"

kubectl create namespace "$STRIMZI_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
curl -fsSL "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml" \
  | sed "s/^    namespace: myproject\$/    namespace: ${STRIMZI_NAMESPACE}/" \
  | kubectl apply -n "$STRIMZI_NAMESPACE" -f -
kubectl rollout restart deployment/strimzi-cluster-operator -n "$STRIMZI_NAMESPACE"
kubectl rollout status deployment/strimzi-cluster-operator -n "$STRIMZI_NAMESPACE" --timeout=120s

kubectl apply -f k8s/strimzi/00-namespace.yaml
kubectl apply -n "$STRIMZI_NAMESPACE" -f k8s/strimzi/10-kafka-kraft.yaml
kubectl apply -n "$STRIMZI_NAMESPACE" -f k8s/strimzi/20-kafka-topics.yaml

kubectl wait kafka/kind-kafka -n "$STRIMZI_NAMESPACE" --for=condition=Ready --timeout=600s

docker build -t "$IMAGE" .

load_image_into_kind() {
  local cluster="${1:?}"
  local image="${2:?}"
  local node="${cluster}-control-plane"
  if kind load docker-image "$image" --name "$cluster"; then
    return 0
  fi
  echo "kind load failed; piping docker save into ctr images import -"
  docker save "$image" | docker exec -i "$node" ctr -n=k8s.io images import -
}
load_image_into_kind "$CLUSTER_NAME" "$IMAGE"

kubectl apply -f k8s/app/00-namespace.yaml
kubectl apply -f k8s/app/10-router-configmap.yaml
kubectl apply -f k8s/app/deployment.yaml
kubectl wait deployment/ingest-router -n ingest-router --for=condition=Available --timeout=180s

echo "Done. Run: make smoke-producer-help"
