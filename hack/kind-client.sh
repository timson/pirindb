#!/usr/bin/env bash
# Shared client pod: the production images intentionally contain no test tools.
client_pod=pirindb-test-client

# Keep every command on the disposable cluster, regardless of current context.
kubectl() {
  command kubectl --context "kind-${cluster_name}" "$@"
}

ensure_test_client() {
  kubectl -n default delete pod "${client_pod}" --ignore-not-found --wait=true >/dev/null
  kubectl -n default run "${client_pod}" --image=python:3.13-alpine \
    --restart=Never --labels=pirindb.timson.dev/redis-client=true \
    --command -- python3 -c 'import time; time.sleep(86400)' >/dev/null
  kubectl -n default wait "pod/${client_pod}" --for=condition=Ready --timeout=180s >/dev/null
}

redis_command_on_pod() {
  local pod="$1"
  shift
  kubectl -n default exec -i "${client_pod}" -- python3 - "${pod}.demo-headless" "$@" <<'PY'
import socket, sys
args = [arg.encode() for arg in sys.argv[2:]]
request = b'*%d\r\n' % len(args)
for arg in args:
    request += b'$%d\r\n' % len(arg) + arg + b'\r\n'
with socket.create_connection((sys.argv[1], 6379), timeout=5) as conn:
    conn.sendall(request)
    with conn.makefile('rb') as stream:
        def read_reply():
            line = stream.readline()
            if not line.endswith(b'\r\n'):
                raise RuntimeError('incomplete RESP reply')
            result = line
            if line[:1] == b'$':
                length = int(line[1:-2])
                if length >= 0:
                    payload = stream.read(length + 2)
                    if len(payload) != length + 2 or not payload.endswith(b'\r\n'):
                        raise RuntimeError('incomplete bulk reply')
                    result += payload
            elif line[:1] == b'*':
                for _ in range(int(line[1:-2])):
                    result += read_reply()
            elif line[:1] not in (b'+', b'-', b':'):
                raise RuntimeError('invalid RESP reply')
            return result
        sys.stdout.buffer.write(read_reply())
PY
}

http_on_pod() {
  # mode is either body (requires HTTP 200) or status (returns exact status).
  local pod="$1" path="$2" mode="${3:-body}" method="${4:-GET}"
  kubectl -n default exec -i "${client_pod}" -- python3 - \
    "http://${pod}.demo-headless:4321${path}" "${mode}" "${method}" <<'PY'
import sys, urllib.error, urllib.request
url, mode, method = sys.argv[1:]
request = urllib.request.Request(url, data=b'{}' if method == 'POST' else None, method=method)
try:
    response = urllib.request.urlopen(request, timeout=10)
except urllib.error.HTTPError as error:
    response = error
with response:
    if mode == 'status':
        print(response.code)
    elif response.code == 200:
        sys.stdout.buffer.write(response.read())
    else:
        raise RuntimeError(f'unexpected HTTP status: {response.code}')
PY
}

assert_cluster_authentication() {
  local status
  status="$(http_on_pod demo-0 /api/v1/cluster/reconcile-topology status POST)" || return
  if [[ "${status}" != "401" ]]; then
    printf 'expected HTTP 401 for unauthenticated mutation, got %s\n' "${status}" >&2
    return 1
  fi
}
