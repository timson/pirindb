# PirinDB cluster guide

This guide covers the cluster implementation in the checked-out source: local
setup, Kubernetes deployment, routing, scaling, recovery, and validation.

## What the cluster provides

PirinDB distributes Redis hash slots across independent storage nodes. Each slot
has one durable owner. A cluster-aware client routes requests to the owner or
follows `MOVED` and `ASK` responses. Multi-key commands must satisfy the server's
same-slot requirements; cross-slot requests return `CROSSSLOT`.

The operator automates scale-up and drains nodes before scale-down. Slot moves
use persisted jobs, bounded copy requests, checksummed frames, dirty-key catch-up,
and destination verification before changing ownership. Interrupted jobs can
resume after a restart. Only one move executes at a time;
`max_parallel_moves > 1` is rejected.

There is no shard replication, data-node leader election, or automatic failover.
If an owner is unavailable, its slots are unavailable. If its volume is lost,
recover its data from a backup. Adding nodes distributes data and capacity; it
does not create redundant copies. Deploy on a trusted, low-latency network.

## Versions and compatibility

There is no separate “cluster v0.1” release contract. Use the same application
release for all server nodes and the matching operator built from that source.
Mixed-version online upgrades are unsupported.

| Identifier | Meaning | What to do |
| --- | --- | --- |
| Application version or image tag | Identifies the server/operator build. The server's default comes from `cmd/pirindb/const.go`; container builds can override it with `VERSION`. | Select images built from the same reviewed release or commit. |
| `v0.1.0` in sample manifests | An example image tag, not proof that the image matches your checkout or is qualified for deployment. | Replace the server and operator image references before installation. |
| `pirindb.timson.dev/v1alpha1` | Kubernetes resource API used by `PirinDBCluster`. | Keep it aligned with the installed CRD/operator; it is not a server release number. |
| Transfer protocol version | Internal framing for moves between nodes. The current implementation uses version 2. | Let the server manage it; do not mix incompatible binaries. |

Database metadata, WAL, and snapshot formats have their own compatibility rules.
Read [storage migration and restore](storage-safety-operations.md) before opening
existing files with a new binary. Keep release notes and qualification results
in the [release process](releasing.md), rather than creating another versioned
cluster guide.

## Run three nodes locally

The [example configurations](../examples/cluster/) use loopback addresses:

| Node | HTTP | Redis | Initial slots |
| --- | --- | --- | --- |
| node-a | 127.0.0.1:4321 | 127.0.0.1:6379 | 0–8191 |
| node-b | 127.0.0.1:4322 | 127.0.0.1:6380 | 8192–16383 |
| node-c | 127.0.0.1:4323 | 127.0.0.1:6381 | None; available as a rebalance destination |

Open three terminals. In each, change to `examples/cluster` from the repository
root, set the same development-only token, and run the corresponding node:

```sh
cd examples/cluster
export PIRINDB_CLUSTER_ADMIN_TOKEN=local-development-only-token
go run ../../cmd/pirindb --config node-a.toml
```

Use `node-b.toml` and `node-c.toml` in the other terminals. Each process creates
its own DB/WAL pair in this directory. The topology file lists all nodes and
supplies the initial slot assignment for a fresh cluster; persisted runtime
ownership changes as slots move.

Check a node's view of the cluster:

```sh
curl --fail \
  -H "Authorization: Bearer $PIRINDB_CLUSTER_ADMIN_TOKEN" \
  http://127.0.0.1:4321/api/v1/cluster/status
```

Test routing with a cluster-aware Redis client. For example, if `redis-cli` is
installed:

```sh
redis-cli -c -h 127.0.0.1 -p 6379 SET example hello
redis-cli -c -h 127.0.0.1 -p 6379 GET example
```

PirinDB implements a Redis subset, not full Redis compatibility. See the
[command semantics and limits](redis-compatibility.md). It has no Redis cluster
bus or replication links; advertised cluster-bus ports are zero.

## Deploy with the Kubernetes operator

Use a disposable cluster for the first deployment. Before applying the manifests:

1. Build/publish a server image and operator image from the same chosen source.
2. Set the Deployment image in
   [operator.yaml](../operator/manifests/operator.yaml) and `spec.image` in
   [sample-cluster.yaml](../operator/manifests/sample-cluster.yaml) to those images.
3. Confirm the sample namespace, storage class, requested volume size, ports,
   and desired number of storage nodes. `spec.replicas` is the node count, not
   the number of redundant data copies.

From the repository root:

```sh
make generate-crd
kubectl apply -k operator/manifests

export PIRINDB_CLUSTER_ADMIN_TOKEN=replace-with-a-private-cluster-token
kubectl -n default create secret generic demo-cluster-admin \
  --from-literal=token="$PIRINDB_CLUSTER_ADMIN_TOKEN"

kubectl apply -f operator/manifests/sample-cluster.yaml
kubectl -n default get pirindbcluster demo
kubectl -n default get pods,pvc
```

The base installs the CRD and operator in `pirindb-system`. The sample creates
`demo` in `default`. Its required `spec.adminSecretRef` selects the token Secret
in the cluster's namespace. The operator reads that Secret through an uncached,
get-only request and injects the token into server pods. It does not copy the
token into the topology ConfigMap or custom-resource status.

The [NetworkPolicy](../operator/manifests/network-policy.yaml) and
[PodDisruptionBudget](../operator/manifests/pod-disruption-budget.yaml) are optional
examples. Adapt them to your namespaces, labels, client access, CNI, and
API-server egress requirements. A disruption budget does not add replication.

## Authentication and network access

Cluster control-plane mutations and `/api/v1/cluster/internal/*` require the
shared bearer token when `cluster.require_auth=true`, the default configuration.
Read-only status endpoints can remain public with `cluster.public_status=true`.
Restrict administrative and internal ports to trusted clients and nodes.

For manually managed nodes, configure `PIRINDB_CLUSTER_ADMIN_TOKEN` or a readable,
protected token file:

```toml
[cluster]
enabled = true
node_id = "node-a"
topology_file = "cluster-topology.toml"
require_auth = true
public_status = false
admin_token_file = "/run/secrets/pirindb/cluster-token"
```

Administrative requests use `Authorization: Bearer <token>`. Cluster HTTP
addresses must use `http://`; the server does not expose TLS certificate/key
configuration. Token authentication does not encrypt traffic.

Cluster credentials are separate from `db.admin_token`, which protects logical
DB transfer administration. The ordinary KV/Redis interfaces still need
appropriate network/application access controls.

The token is loaded at process start and only one is accepted. Rotate it in a
maintenance window: finish active moves, block administrative traffic, update the
Secret or file, restart the operator and every node, verify status with the new
token, then restore access. A rolling old/new-token mixture makes nodes reject
one another.

## Scale and move data

For operator-managed deployments, change `spec.replicas` on the existing
`PirinDBCluster`. Do not directly reduce the StatefulSet or delete a node's volume.

- **Scale up:** publish the expanded topology, wait for nodes to observe it,
  gather cluster-wide metrics, and move ranges onto the added nodes.
- **Scale down:** drain the highest ordinal, wait for it to own zero slots,
  publish and converge on the reduced topology, remove membership, then reduce
  the StatefulSet.

Operations retain idempotency keys and progress in persistent state. An
interrupted job resumes; an unsuccessful transfer does not complete an ownership
cutover. Inspect job status before retrying or editing topology manually.

For manually managed clusters, the administrative API provides these entrypoints
under `/api/v1/cluster`:

| Endpoint | Purpose |
| --- | --- |
| GET `/status`, `/slots`, `/topology` | Inspect membership, ownership, and convergence |
| POST `/rebalance/plan-for-node` | Plan moves to a destination node |
| POST `/rebalance/auto` | Start planned scale-out moves |
| GET `/rebalance/auto/{jobID}` | Poll scale-out progress |
| POST `/rebalance/plan-drain-node` | Plan evacuation of a node |
| POST `/rebalance/drain` | Start a drain |
| GET `/rebalance/drain/{jobID}` | Poll drain progress |

For the local example, preview moves onto node C, then start the operation on
the same coordinator:

```sh
curl --fail \
  -H "Authorization: Bearer $PIRINDB_CLUSTER_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"destination_node_id":"node-c"}' \
  http://127.0.0.1:4321/api/v1/cluster/rebalance/plan-for-node

curl --fail \
  -H "Authorization: Bearer $PIRINDB_CLUSTER_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: local-scale-out-node-c-1' \
  -d '{"destination_node_id":"node-c"}' \
  http://127.0.0.1:4321/api/v1/cluster/rebalance/auto
```

The second response contains `job_id`. Poll
`http://127.0.0.1:4321/api/v1/cluster/rebalance/auto/{jobID}` with the same
authorization header until completion, and inspect any reported error. Reuse the
idempotency key when retrying that operation; choose a new key for a new operation.

Planning uses cluster-wide key/byte metrics. Slot-count fallback is an explicit
fallback when requesting plans, not a substitute for successful metric collection
in normal operation. Do not race manual operations with operator reconciliation.

During handoff, `ASK` requires the client to send `ASKING` to the destination
before retrying that command. After cutover, `MOVED` directs the client to the
new owner. Destination receipts, completed copy manifests, logical key counts,
and dirty-key catch-up protect the transition from publishing a partial move.

Transfer requests target 32 MiB by default with a configured 64 MiB ceiling and
a record limit; individual checksummed frames are bounded to 1 MiB. One oversized
logical key can exceed the request target/ceiling while streaming in bounded
frames; it is reported through `oversized_logical_key_chunks`. The relevant
settings are `cluster.transfer_chunk_target_bytes`,
`cluster.transfer_chunk_max_bytes`, and `cluster.transfer_chunk_max_records`.

## Monitor, restart, and recover

Cluster status exposes topology hashes/epoch, active operations, copy and cleanup
cursors, transferred bytes/records, dirty-key lag, and `import_staging_keys`.
Wait for topology convergence and completed jobs before maintenance.

`/health` returns 503 for closing/closed/fatal storage or an enabled Redis listener
that is not accepting connections. It does not scan disk or establish cluster-wide
availability. The operator uses readiness/liveness probes and allows up to ten
minutes for startup recovery. Startup failures produce a nonzero process exit.

Each StatefulSet ordinal has its own data PVC. Retained volumes help preserve
files across restarts, deletion, and scale-down, but are not backups. Inspect and
back up a retained PVC before deleting it. Never attach a removed ordinal's data
to a different logical cluster without an explicit migration or reinitialization.

For upgrades, stop moves and use a maintenance window with matching binaries on
all nodes. Save matching DB/WAL backups before format migration. For a failed
node, preserve its files and restore its matching volume where possible. A lost
volume requires a backup; there is no automatic reconstruction from peers.

Coordinated cluster-wide point-in-time backup/restore is not implemented. A
snapshot of one node is only that node's data. Live full-DB HTTP import is disabled
in cluster mode; follow the [storage recovery guide](storage-safety-operations.md)
and preserve the cluster's topology/ownership context when planning recovery.

## Validate a deployment locally

From the repository root:

```sh
go test ./...
go vet ./...
go test -race ./storage ./cmd/pirindb ./operator/controllers
make fuzz-cluster
python3 hack/test-kind-client.py
bash -n hack/kind-client.sh hack/kind-e2e.sh hack/kind-soak.sh
```

For container/Kubernetes checks, install Docker, kind, and kubectl, then run:

```sh
bash hack/kind-e2e.sh
SOAK_DURATION_SECONDS=14400 make soak-cluster
```

The scripts create disposable kind environments and use client pods to test the
production images over the network. They exercise supported data types and TTLs,
authentication, restarts, scale-up, and ordered drains. The soak runs complete
cycles, so its elapsed time can exceed the requested duration. A short smoke
run does not replace a multi-hour deployment qualification.

The [local database suite](local-database-tests.md) checks engine and standalone
server behavior. It does not qualify distributed ownership changes or replace
these cluster-specific checks. Record actual test results for the selected
revision and images before release; this guide is not a certification claim.
