#!/usr/bin/env bash
set -euo pipefail

cluster_name="${KIND_CLUSTER_NAME:-pirindb-soak}"
source hack/kind-client.sh
duration_seconds="${SOAK_DURATION_SECONDS:-14400}"
cycle=0
writer_pid=""

cleanup() {
  if [[ -n "${writer_pid}" ]]; then
    kill "${writer_pid}" >/dev/null 2>&1 || true
    wait "${writer_pid}" >/dev/null 2>&1 || true
  fi
  if [[ "${KEEP_KIND_CLUSTER:-false}" != "true" ]]; then
    kind delete cluster --name "${cluster_name}"
  fi
}
trap cleanup EXIT

KEEP_KIND_CLUSTER=true KIND_CLUSTER_NAME="${cluster_name}" bash hack/kind-e2e.sh >/dev/null


cluster_redis_command() {
  local response pod
  while read -r pod; do
    response="$(redis_command_on_pod "${pod}" "$@" 2>/dev/null || true)"
    case "${response}" in
      ""|-MOVED*|-ASK*) ;;
      -*) return 1 ;;
      *)
        printf '%s' "${response}"
        return 0
        ;;
    esac
  done < <(kubectl -n default get pods -l pirindb.timson.dev/cluster=demo -o name | sed 's#pod/##' | sort)
  return 1
}

continuous_writes() {
  local sequence=0
  while true; do
    cluster_redis_command SET "soak:key:${sequence}" "value-${sequence}" >/dev/null || true
    sequence=$((sequence + 1))
    sleep 1
  done
}

assert_clean_status() {
  local pod status
  while read -r pod; do
    status="$(http_on_pod "${pod}" /api/v1/cluster/status)"
    if [[ "${status}" != *'"import_staging_keys":0'* ]]; then
      printf 'staging leak reported by %s: %s\n' "${pod}" "${status}" >&2
      return 1
    fi
  done < <(kubectl -n default get pods -l pirindb.timson.dev/cluster=demo -o name | sed 's#pod/##' | sort)
}

cluster_redis_command SET soak:sentinel ready >/dev/null
continuous_writes &
writer_pid="$!"
deadline=$((SECONDS + duration_seconds))

while ((SECONDS < deadline)); do
  cycle=$((cycle + 1))
  kubectl -n default patch pirindbcluster demo --type merge -p '{"spec":{"replicas":2}}' >/dev/null
  kubectl -n default wait pirindbcluster/demo --for=jsonpath='{.status.currentReplicas}'=2 --timeout=900s >/dev/null
  kubectl -n default wait pirindbcluster/demo --for=jsonpath='{.status.phase}'=Ready --timeout=900s >/dev/null

  kubectl -n default delete pod demo-0 --wait=false >/dev/null
  kubectl -n default rollout status statefulset/demo --timeout=300s >/dev/null

  kubectl -n default patch pirindbcluster demo --type merge -p '{"spec":{"replicas":3}}' >/dev/null
  kubectl -n default rollout status statefulset/demo --timeout=600s >/dev/null
  kubectl -n default wait pirindbcluster/demo --for=jsonpath='{.status.currentReplicas}'=3 --timeout=600s >/dev/null
  kubectl -n default wait pirindbcluster/demo --for=jsonpath='{.status.phase}'=Ready --timeout=600s >/dev/null

  cluster_redis_command GET soak:sentinel | grep -q ready
  assert_clean_status
  printf 'completed soak cycle %d at %s\n' "${cycle}" "$(date -u +%FT%TZ)"
done

printf 'soak completed: cycles=%d duration_seconds=%d\n' "${cycle}" "${duration_seconds}"
