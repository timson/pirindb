#!/usr/bin/env bash
set -euo pipefail

cluster_name="${KIND_CLUSTER_NAME:-pirindb-e2e}"
source hack/kind-client.sh
server_image="ghcr.io/timson/pirindb:v0.1.0"
operator_image="ghcr.io/timson/pirindb-operator:v0.1.0"


cluster_redis_command() {
  local replicas="$1"
  shift
  local ordinal response
  for ((ordinal = 0; ordinal < replicas; ordinal++)); do
    response="$(redis_command_on_pod "demo-${ordinal}" "$@")"
    case "${response}" in
      -MOVED*|-ASK*) ;;
      -*)
        printf 'Redis command failed on demo-%d for %s: %s\n' "${ordinal}" "$*" "${response}" >&2
        return 1
        ;;
      *)
        printf '%s' "${response}"
        return 0
        ;;
    esac
  done
  printf 'no shard accepted Redis command: %s\n' "$*" >&2
  return 1
}

assert_redis_positive_integer() {
  local replicas="$1"
  shift
  local response
  response="$(cluster_redis_command "${replicas}" "$@")"
  response="${response//$'\r'/}"
  if [[ ! "${response}" =~ ^:[1-9][0-9]*$ ]]; then
    printf 'Redis assertion failed for %s: expected a positive integer, got %q\n' "$*" "${response}" >&2
    return 1
  fi
}

assert_cluster_data() {
  local replicas="$1"
  assert_redis_contains "${replicas}" ready GET e2e:string
  assert_redis_positive_integer "${replicas}" PTTL e2e:string
  assert_redis_contains "${replicas}" one LRANGE e2e:list 0 -1
  assert_redis_contains "${replicas}" value HGET e2e:hash field
  assert_redis_contains "${replicas}" 2 ZSCORE e2e:zset member
  assert_redis_contains "${replicas}" :1 BF.EXISTS e2e:bloom present
  assert_redis_contains "${replicas}" :1 TOPK.QUERY e2e:topk hot
}

assert_redis_contains() {
  local replicas="$1" expected="$2"
  shift 2
  local response
  response="$(cluster_redis_command "${replicas}" "$@")"
  if [[ "${response}" != *"${expected}"* ]]; then
    printf 'Redis assertion failed for %s: expected %q in %q\n' "$*" "${expected}" "${response}" >&2
    return 1
  fi
}

cleanup() {
  if [[ "${KEEP_KIND_CLUSTER:-false}" != "true" ]]; then
    kind delete cluster --name "${cluster_name}"
  fi
}
trap cleanup EXIT

if ! kind get clusters | grep -Fxq "${cluster_name}"; then
  kind create cluster --name "${cluster_name}" --wait 120s
fi
docker build --target pirindb -t "${server_image}" .
docker build --target operator -t "${operator_image}" .
kind load docker-image --name "${cluster_name}" "${server_image}" "${operator_image}"

kubectl apply -k operator/manifests
kubectl -n pirindb-system rollout restart deployment/pirindb-operator
kubectl -n pirindb-system rollout status deployment/pirindb-operator --timeout=180s
kubectl -n default delete pirindbcluster/demo --ignore-not-found --wait=true --timeout=120s
kubectl -n default delete statefulset/demo service/demo-headless configmap/demo-topology --ignore-not-found --wait=true
kubectl -n default delete persistentvolumeclaim \
  -l pirindb.timson.dev/cluster=demo --ignore-not-found --wait=true
kubectl -n default create secret generic demo-cluster-admin \
  --from-literal=token="kind-e2e-control-plane-token" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f operator/manifests/sample-cluster.yaml
kubectl -n default wait --for=create statefulset/demo --timeout=180s
kubectl -n default rollout status statefulset/demo --timeout=300s
kubectl -n default wait pirindbcluster/demo --for=jsonpath='{.status.currentReplicas}'=3 --timeout=600s
kubectl -n default wait pirindbcluster/demo --for=jsonpath='{.status.phase}'=Ready --timeout=600s

ensure_test_client
assert_cluster_authentication

cluster_redis_command 3 SET e2e:string ready >/dev/null
cluster_redis_command 3 PEXPIRE e2e:string 600000 >/dev/null
cluster_redis_command 3 RPUSH e2e:list one two three >/dev/null
cluster_redis_command 3 HSET e2e:hash field value >/dev/null
cluster_redis_command 3 ZADD e2e:zset 2 member >/dev/null
cluster_redis_command 3 BF.RESERVE e2e:bloom 0.01 100 >/dev/null
cluster_redis_command 3 BF.ADD e2e:bloom present >/dev/null
cluster_redis_command 3 TOPK.RESERVE e2e:topk 2 100 5 0.9 >/dev/null
cluster_redis_command 3 TOPK.ADD e2e:topk hot cold hot >/dev/null
assert_cluster_data 3

kubectl -n default patch pirindbcluster demo --type merge -p '{"spec":{"replicas":4}}'
kubectl -n pirindb-system rollout restart deployment/pirindb-operator
kubectl -n pirindb-system rollout status deployment/pirindb-operator --timeout=180s
kubectl -n default rollout status statefulset/demo --timeout=600s
kubectl -n default wait pirindbcluster/demo --for=jsonpath='{.status.currentReplicas}'=4 --timeout=600s
kubectl -n default wait pirindbcluster/demo --for=jsonpath='{.status.phase}'=Ready --timeout=600s
assert_cluster_data 4

kubectl -n default patch pirindbcluster demo --type merge -p '{"spec":{"replicas":2}}'
kubectl -n pirindb-system rollout restart deployment/pirindb-operator
kubectl -n pirindb-system rollout status deployment/pirindb-operator --timeout=180s
kubectl -n default wait pirindbcluster/demo --for=jsonpath='{.status.currentReplicas}'=2 --timeout=900s
kubectl -n default wait pirindbcluster/demo --for=jsonpath='{.status.phase}'=Ready --timeout=900s
kubectl -n default rollout status statefulset/demo --timeout=300s

assert_cluster_data 2

kubectl -n default patch pirindbcluster demo --type merge -p '{"spec":{"replicas":3}}'
kubectl -n pirindb-system rollout restart deployment/pirindb-operator
kubectl -n pirindb-system rollout status deployment/pirindb-operator --timeout=180s
kubectl -n default rollout status statefulset/demo --timeout=600s
kubectl -n default wait pirindbcluster/demo --for=jsonpath='{.status.currentReplicas}'=3 --timeout=600s
kubectl -n default wait pirindbcluster/demo --for=jsonpath='{.status.phase}'=Ready --timeout=600s

assert_cluster_data 3

kubectl -n default get pirindbcluster demo -o yaml
