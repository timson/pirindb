# PirinDB Sharded Cluster v0.1 Release Candidate

Status: release qualification pending.

This release candidate targets operator-managed sharding in a controlled, trusted, low-latency environment. It is not a highly available database.

## Supported operating envelope

- Deterministic Redis hash-slot routing with `MOVED`, `ASK`, and `CROSSSLOT` behavior.
- Unattended, restart-safe scale-up and drain-before-scale-down through the Kubernetes operator.
- Protocol-v2, checksummed, bounded-frame transfer for strings, lists, hashes, sorted sets, Bloom filters, TopK structures, and absolute TTLs.
- Durable child and parent jobs, destination receipts, idempotent retries, dirty-key catch-up, and pre-cutover verification.
- Strict cluster-wide byte/key metrics for automatic planning, with slot counts retained as a labelled guardrail when sparse data makes exact byte and slot balance mutually exclusive.
- Bearer-authenticated control-plane mutation, Kubernetes Secret injection, request limits, least-privilege RBAC, generated CRD, Kustomize manifests, NetworkPolicy example, and PodDisruptionBudget guidance.

All nodes must run the same PirinDB release. Only one move executes at a time; requests for `max_parallel_moves > 1` are rejected. A single indivisible logical key may exceed the request-level chunk target, but its encoded records remain bounded by the protocol frame limit and the event is exposed through `oversized_logical_key_chunks`.

## Release qualification

Before creating a release tag, run the repository's validation gates:

- full Go tests, vet, and race tests for the server, storage, and operator;
- transfer-decoder fuzzing;
- CRD regeneration, Kustomize rendering, and shell/client-helper checks;
- disposable kind E2E tests using the production images, including authentication, data preservation, scale-up, ordered drain, and operator restarts;
- the manual four-hour `cluster-soak` workflow.

Local unit tests and helper tests do not qualify the container deployment or replace the multi-hour soak. The current container E2E and soak require a fresh successful run before release.

## Known limitations

- A shard has one storage replica. There is no replication, leader election, automatic failover, or zero-data-loss recovery after losing its volume.
- Mixed-version online upgrades are unsupported. Upgrade during a maintenance window with no active transfer.
- Cluster-wide point-in-time backup orchestration and cross-region operation are out of scope.
- The cluster API uses plain HTTP only. Cluster topology addresses using any other URL scheme are rejected.
- Credential rotation is a maintenance operation because the first release accepts one token at process start.
- Retained PVCs protect data during deletion and scale-down but must be backed up, audited, and explicitly removed to avoid stale-data reuse or storage cost.

Installation and operating procedures are in [Cluster Operations](cluster-operations.md).
