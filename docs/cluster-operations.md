# PirinDB Cluster Operations

## Failure model

PirinDB cluster mode is sharding, not high availability. Each slot has exactly one durable owner. There is currently no shard replication, leader election, automatic failover, or zero-data-loss recovery after loss of a shard volume. Use retained persistent volumes and external backups; do not describe this release as an HA database.

## Control-plane credential

Every mutating cluster API and every `/api/v1/cluster/internal/*` endpoint requires the shared bearer token when `cluster.require_auth=true` (the default for file/env configuration). Read-only status can remain public with `cluster.public_status=true`.

Create the operator-managed credential before creating the sample cluster:

```bash
export PIRINDB_CLUSTER_ADMIN_TOKEN=local-cluster-admin-token
kubectl -n default create secret generic demo-cluster-admin \
  --from-literal=token="$PIRINDB_CLUSTER_ADMIN_TOKEN"
```

The required `PirinDBCluster.spec.adminSecretRef` selects this Secret. Admission and reconciliation reject operator-managed clusters without it. The operator reads the Secret through an uncached, get-only API call and injects it into server pods through `PIRINDB_CLUSTER_ADMIN_TOKEN`; it is never copied into the topology ConfigMap or CR status.

For a standalone cluster, use `PIRINDB_CLUSTER_ADMIN_TOKEN` or a root-readable token file:

```toml
[cluster]
require_auth = true
admin_token_file = "/run/secrets/pirindb/cluster-token"
```

Send `Authorization: Bearer <token>` on administrative requests. PirinDB serves this API over plain HTTP only: there are no certificate/key settings or certificate-generation steps, and every cluster `http_address` must use `http://`.

### Safe token rotation

The first release loads credentials at process start and accepts one token. Treat rotation as a maintenance operation:

1. wait until no rebalance or drain job is active;
2. block external administrative traffic;
3. update the Secret;
4. restart the operator and all PirinDB pods in one maintenance window;
5. verify `/api/v1/cluster/status` on every node using the new token;
6. restore administrative traffic.

Do not perform a rolling token change: old-token and new-token pods intentionally reject one another.

## Installation

Generate and verify the CRD, then install the base:

```bash
make generate-crd
kubectl apply -k operator/manifests
kubectl apply -f operator/manifests/sample-cluster.yaml
```

`network-policy.yaml` and `pod-disruption-budget.yaml` are opt-in examples. Adjust namespaces, client labels, ports, and the API-server egress rule for the target CNI before applying them.

## Scaling

Change `spec.replicas`. Scale-up publishes the larger topology, waits for every server to observe it, gathers strict cluster-wide slot metrics, and moves one range at a time. Scale-down drains the highest ordinal, publishes the smaller topology only after it owns zero slots, waits for topology convergence, removes runtime membership, and finally shrinks the StatefulSet.

Operations carry deterministic idempotency keys in CR status. Operator and server restarts resume the persisted operation. A failed transfer does not change slot ownership.

Base-copy requests target 32 MiB, use a 64 MiB configured ceiling, cap protocol records, and contain independently checksummed frames no larger than 1 MiB. A single logical key that exceeds the request ceiling is still streamed incrementally and is counted in `oversized_logical_key_chunks`; this preserves bounded heap without exposing a partial key. Configure the policy with `cluster.transfer_chunk_target_bytes`, `cluster.transfer_chunk_max_bytes`, and `cluster.transfer_chunk_max_records`.

## Storage and deletion

The StatefulSet uses one `data` PVC per ordinal. Kubernetes normally retains StatefulSet PVCs when pods, the StatefulSet, or the `PirinDBCluster` object are deleted. This protects data but also leaves billable storage. Back up and inspect retained PVCs before deleting them manually. Never reuse a removed ordinal's PVC in another logical cluster without wiping or explicitly importing it.

## Release and diagnostics

Run:

```bash
go test ./...
go vet ./...
go test -race ./cmd/pirindb ./operator/controllers
go test ./... -run Cluster
make fuzz-cluster
bash hack/kind-e2e.sh
```

The kind gate writes and verifies string, list, hash, zset, Bloom, TopK, and TTL data; rejects an anonymous mutation; restarts the operator during both directions; drains 4→2 one ordinal at a time; and scales 2→3 again.

Run the multi-hour add/drain/write/restart leak gate manually or from the `cluster-soak` workflow:

```bash
SOAK_DURATION_SECONDS=14400 make soak-cluster
```

Any positive duration runs at least one complete cycle; use `SOAK_DURATION_SECONDS=1` for a functional smoke test. A release tag still requires the documented multi-hour duration.

Cluster status exposes runtime/configured topology hashes, epoch, active move and orchestration jobs, transfer bytes/records/rate/estimated remaining work, copy and cleanup cursors, dirty-key lag, and `import_staging_keys`. A move only cuts over after the destination receipt is complete and source/destination logical key counts match.

## Health and startup failures

`/health` returns HTTP 503 when storage is closing, closed, or has entered a fatal durability state, or when an enabled Redis listener is not accepting connections. The check does not scan disk contents or wait for transaction locks. It is not an integrity check or a guarantee of shard availability across the cluster.

The operator uses this endpoint for readiness and liveness, with a startup probe allowing up to ten minutes for recovery. Startup errors propagate to a nonzero process exit after cleanup.

The kind E2E and soak scripts use a separate `python:3.13-alpine` client pod to exercise the production images over the network. They require an actual HTTP 401 response for the unauthenticated mutation check; connection and tooling errors fail the check.
