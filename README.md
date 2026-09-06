# PirinDB

<img src="images/pirindb_logo.png" alt="PirinDB Logo" width="200">

**PirinDB** is a lightweight key-value database developed as a personal project to deepen my 
understanding of database internals.  
Storage engine is partial based on the implementation of [**LibraDB**](https://github.com/amit-davidson/LibraDB), 
following the excellent article, and draws further inspiration from Alex Petrov’s [book](https://www.amazon.com/Database-Internals-Deep-Distributed-Systems/dp/1492040347) 
and [**BoltDB**](https://github.com/boltdb/bolt) internals. **PirinDB** provides an approachable way to explore how B+Tree-based storage engines
work under the hood.

Another goal of the project is to develop database server with simple HTTP API, with sharding and 
replication support, to play with concepts of distributed systems.

Project contains:
 - Storage engine with BoltDB-like API
 - Database server
 - CLI client
 - Kubernetes operator

## Features

**Storage Engine Features:**

- **Small Codebase:** Designed to be easily readable.
- **BoltDB like API:** Easy to integrate and experiment with.
- **Persistence:** Data is stored on disk.
- **Simple Transactional Support:** Ensures data consistency.
- **Dynamic Freelist Management:** Automatic allocation and reuse of storage pages.
- **Bucket Management:** Tools for organizing and managing buckets.
- **Basic Cursor and Range Scanning:** Provides mechanisms to iterate over data ranges efficiently.
- **View/Update Syntax Sugar:** Convenient API methods to simplify database interactions.
- [x] **Blob Storage:** Supports for BLOBs.
- [x] ~~**Copy on-write:** Support for copy-on-write~~ **Double write** (last transaction log) for data consistency and recovery.
- [ ] **Simple ORM:** Basic object-relational mapping layer for data structures.
- [x] **Statistics:** Built-in statistics.

**Database server:**

- [x] Simple HTTP server
- [x] Redis-compatible TCP interface
- [x] Basic key-value operations
- [ ] Multi-key operations
- [ ] Range scanning operations
- [x] Internal sharding with fixed hash slots
- [ ] Replication support

## Project Status

**PirinDB** is intended purely as a hobby and educational project. It is not designed to be used  
as a production-grade database. Instead, it's a playground for those interested in learning and  
exploring the inner workings of database management systems.

## Quick Start
You need Go 1.26 or later to build PirinDB.

To build PirinDB server and client, run:

```bash
$ make build
```
It creates three binaries in the `bin` directory: `pirindb`, `pirin-cli`, and `pirindb-operator`.

To start the server, run:

```bash
$ ./bin/pirindb
```
It will run the HTTP server on port 4321, and you can access the server at `http://localhost:4321`.
It also starts a Redis-compatible TCP listener on `127.0.0.1:6379` by default.
Database file (by default **pirin.db**) is stored in the current directory.

You can override the Redis listener with flags or environment variables:

```bash
$ ./bin/pirindb --redis-enabled=true --redis-host=127.0.0.1 --redis-port=6379
```

Storage durability mode is also configurable:

```bash
$ ./bin/pirindb --sync-policy=strict
$ ./bin/pirindb --sync-policy=journal --checkpoint-tx-threshold=64
$ ./bin/pirindb --sync-policy=group --group-commit-tx-threshold=16 --group-commit-window-ms=1
$ ./bin/pirindb --node-cache-mb=64
```

- `strict` is the default double-write mode: one durable journal sync and one durable main DB sync per write transaction.
- `journal` keeps full crash recovery semantics, but only fsyncs the journal on each transaction and checkpoints the main DB file every `N` committed write transactions.
- `group` batches concurrent write transactions into one durable flush group. Readers are held back until the batch becomes durable, so committed writers share one `txlog + db` sync cycle.
- `node-cache-mb` bounds the shared immutable B-tree cache (32 MiB by default); set it to `0` for memory-constrained deployments.

The same settings can be supplied via environment variables:

```bash
$ PIRINDB_DB_SYNC_POLICY=journal PIRINDB_DB_CHECKPOINT_TX_THRESHOLD=64 ./bin/pirindb
$ PIRINDB_DB_SYNC_POLICY=group PIRINDB_DB_GROUP_COMMIT_TX_THRESHOLD=16 PIRINDB_DB_GROUP_COMMIT_WINDOW_MS=1 ./bin/pirindb
```

Internal cluster mode is also available for Redis traffic. It uses fixed hash slots and direct-to-shard clients:

```toml
[cluster]
enabled = true
node_id = "node-a"
topology_file = "/etc/pirindb/topology.toml"
require_auth = true
admin_token_file = "/run/secrets/pirindb/cluster-token"
transfer_chunk_target_bytes = 33554432
transfer_chunk_max_bytes = 67108864
transfer_chunk_max_records = 65536
```

Shared topology file:

```toml
[cluster]
name = "prod-eu-1"
slot_count = 16384

[[cluster.nodes]]
id = "node-a"
redis_address = "10.0.0.1:6379"
http_address = "http://10.0.0.1:4321"

[[cluster.nodes]]
id = "node-b"
redis_address = "10.0.0.2:6379"
http_address = "http://10.0.0.2:4321"

[[cluster.bootstrap_slots]]
node_id = "node-a"
slots = ["0-8191"]

[[cluster.bootstrap_slots]]
node_id = "node-b"
slots = ["8192-16383"]
```

In cluster mode:

- Redis keys are routed by Redis-style hash slots, including `{tag}` hash tags.
- Single-key requests are accepted only on the owning shard; other shards return `MOVED slot host:port`.
- Multi-key Redis requests are accepted only when every key hashes to the same slot; otherwise PirinDB returns `CROSSSLOT`.
- `KEYS`, `SCAN`, `DBSIZE`, `FLUSHDB`, and `FLUSHALL` operate on the local shard only.
- Rebalancing streams bounded protocol-v2 frames while normal writes continue. Writes for the moving range are fenced only during final dirty-key catch-up and cutover.
- Transfer requests target 32 MiB and stop at logical-key boundaries; every value frame is capped independently. If one indivisible logical key exceeds the 64 MiB request ceiling, it is streamed with bounded memory and reported through `oversized_logical_key_chunks` rather than materialized in RAM.
- Nodes may exist in cluster metadata with zero owned slots. This is the expected starting state for a newly added shard before any slot ranges are moved onto it.
- Cluster mode is sharding only: there is no shard replication or automatic failover. Losing a shard volume loses the slots stored on it.

A runnable loopback three-node fixture is under [`examples/cluster`](examples/cluster/README.md). It includes a zero-slot third node for local rebalance testing and keeps generated databases out of version control.

All mutating cluster endpoints and every internal endpoint require `Authorization: Bearer <token>` when authentication is enabled. Inter-node requests use the same credential automatically. Cluster HTTP communication is plain `http://` only and has no certificate configuration. See [Cluster Operations](docs/cluster-operations.md) for Kubernetes installation, credential rotation, storage retention, scaling, and the exact failure model. The verified v0.1 operating envelope and known limitations are recorded in the [cluster release notes](docs/cluster-release-v0.1.md).

To start the CLI client, run:

```bash
$ ./bin/pirin-cli
```

CLI client provides a simple interface to interact with the server. It supports the following commands:
- `get <key>`: Retrieves the value for the provided key.
- `set <key> <value>`: Sets the value for the provided key.
- `delete <key>`: Deletes the key-value pair.
- `status`: Retrieves the server status.
- `help`: Displays the help message.

## HTTP API

Basic key-value endpoints:

- `GET /api/v1/kv/{key}`
- `POST /api/v1/kv/{key}`
- `DELETE /api/v1/kv/{key}`
- `GET /api/v1/db/status`

PirinDB also exposes async export/import endpoints for server-local snapshot files:

- `POST /api/v1/db/export`
- `POST /api/v1/db/import`
- `GET /api/v1/db/jobs/{jobID}`

Export/import requests use JSON bodies:

```json
{
  "path": "/tmp/pirindb.snapshot",
  "scope": "db"
}
```

Bucket-scoped export/import:

```json
{
  "path": "/tmp/users.snapshot",
  "scope": "bucket",
  "bucket": "users",
  "overwrite": true
}
```

`POST /api/v1/db/export` and `POST /api/v1/db/import` return `202 Accepted` with a `job_id`. Poll `GET /api/v1/db/jobs/{jobID}` until `status` becomes `done` or `failed`.

Notes:

- paths are resolved on the PirinDB server machine, not uploaded from the client
- export/import uses a versioned binary snapshot format
- bucket import replaces only the target bucket
- db import replaces the whole database bucket set
- only one import job can run at a time; export requests are rejected while an import is running

Cluster admin endpoints:

- `GET /api/v1/cluster/status`
- `GET /api/v1/cluster/slots`
- `GET /api/v1/cluster/topology`
- `POST /api/v1/cluster/reconcile-topology`
- `POST /api/v1/cluster/nodes/join`
- `POST /api/v1/cluster/nodes/remove`
- `POST /api/v1/cluster/rebalance/plan`
- `POST /api/v1/cluster/rebalance/plan-for-node`
- `POST /api/v1/cluster/rebalance/execute`
- `POST /api/v1/cluster/rebalance/auto`
- `POST /api/v1/cluster/rebalance/plan-drain-node`
- `POST /api/v1/cluster/rebalance/drain`
- `GET /api/v1/cluster/rebalance/auto/{jobID}`
- `GET /api/v1/cluster/rebalance/drain/{jobID}`
- `GET /api/v1/cluster/rebalance/{jobID}`

`GET /api/v1/cluster/topology` returns the configured shared topology plus the current runtime topology hash/version stored in the DB.

`POST /api/v1/cluster/reconcile-topology` applies the configured topology membership to the live runtime state without resetting slot ownership. This is the preferred way to add a new node after updating the shared topology file.
When a topology file path is configured, PirinDB reloads that file from disk during `reconcile-topology`, so mounted ConfigMap updates become visible without restarting every shard first.

`GET /api/v1/cluster/status` now includes machine-friendly `conditions` and per-node `node_statuses` summaries. The current condition set is:

- `Available`
- `TopologyAligned`
- `Rebalancing`
- `SlotBalanced`

Join a new node before rebalancing any slots onto it:

```json
{
  "id": "node-c",
  "redis_address": "10.0.0.3:6379",
  "http_address": "http://10.0.0.3:4321"
}
```

The rebalance endpoints move one slot range from one shard to another. Submit `POST /api/v1/cluster/rebalance/execute` to the source shard with:

```json
{
  "source_node_id": "node-a",
  "destination_node_id": "node-b",
  "start_slot": 4096,
  "end_slot": 8191
}
```

PirinDB stages and streams the Redis keys in that slot range, catches up compact dirty-key markers, verifies the destination, updates ownership on all nodes, and only then removes the old source copy.

`POST /api/v1/cluster/rebalance/plan-for-node` computes a deterministic rebalance plan for a destination node. It gathers per-slot key and byte estimates concurrently from every current owner, validates a common epoch, balances estimated bytes first, and retains key and slot counts as guardrails. Missing metrics fail automatic planning; a manual request may explicitly set `allow_slot_count_fallback`. Example:

```json
{
  "destination_node_id": "node-c",
  "max_bytes_per_move": 67108864
}
```

The response includes metric freshness, current/target slot, key, and byte counts, the weight metric used, and the exact contiguous slot ranges to move from each donor shard. Automatic scale and drain jobs require complete cluster-wide metrics, preserve the original move limits across restarts, and recalculate after every completed move.

## Kubernetes operator

Build versioned images, create the cluster credential, and install the generated CRD and operator:

```bash
$ docker build --target pirindb -t ghcr.io/timson/pirindb:v0.1.0 .
$ docker build --target operator -t ghcr.io/timson/pirindb-operator:v0.1.0 .
$ make generate-crd
$ kubectl apply -k operator/manifests
$ export PIRINDB_CLUSTER_ADMIN_TOKEN=local-cluster-admin-token
$ kubectl -n default create secret generic demo-cluster-admin --from-literal=token="$PIRINDB_CLUSTER_ADMIN_TOKEN"
$ kubectl apply -f operator/manifests/sample-cluster.yaml
```

`spec.adminSecretRef` is required. The operator performs unattended, restart-safe scale-up and drain-before-scale-down. Optional NetworkPolicy and PodDisruptionBudget examples are under `operator/manifests/`.

`POST /api/v1/cluster/rebalance/auto` uses that same planner and then executes the planned moves sequentially by submitting rebalance jobs to the relevant source shards. Poll `GET /api/v1/cluster/rebalance/auto/{jobID}` until `status` becomes `done` or `failed`.

Drain a node before removing it from the cluster:

```json
{
  "node_id": "node-a"
}
```

- `POST /api/v1/cluster/rebalance/plan-drain-node` returns the slot ranges required to drain that node.
- `POST /api/v1/cluster/rebalance/drain` executes the drain sequentially and returns an auto-job.
- poll `GET /api/v1/cluster/rebalance/drain/{jobID}` until it becomes `done`
- after the node owns zero slots, `POST /api/v1/cluster/nodes/remove` can remove it from runtime membership

Example remove request:

```json
{
  "id": "node-a"
}
```

For topology-managed clusters, remove the node from the shared topology file first and then call `POST /api/v1/cluster/reconcile-topology` or `POST /api/v1/cluster/nodes/remove`. Runtime removal is rejected while the node is still present in the configured topology.

## Redis Interface

PirinDB exposes a durable Redis-compatible RESP2 subset. Exact compatibility, durability, size limits, and unsupported command families are documented in [docs/redis-compatibility.md](docs/redis-compatibility.md).
Currently supported commands are:

- `ASKING`
- `CLIENT GETNAME|ID|SETINFO|SETNAME`
- `COMMAND`
- `ECHO message`
- `HELLO [2]`
- `BF.ADD key item`
- `BF.EXISTS key item`
- `BF.MADD key item [item ...]`
- `BF.MEXISTS key item [item ...]`
- `BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]`
- `BLPOP key [key ...] timeout`
- `BRPOPLPUSH source destination timeout`
- `BRPOP key [key ...] timeout`
- `CLUSTER INFO`
- `CLUSTER KEYSLOT key`
- `CLUSTER NODES`
- `CLUSTER SHARDS`
- `CLUSTER SLOTS`
- `CONFIG GET pattern`
- `CONFIG HELP`
- `CONFIG RESETSTAT`
- `INFO`
- `DECR key`
- `DECRBY key decrement`
- `DBSIZE`
- `GET key`
- `GETSET key value`
- `EXISTS key [key ...]`
- `HDEL key field [field ...]`
- `HEXISTS key field`
- `HGET key field`
- `HGETALL key`
- `HKEYS key`
- `HLEN key`
- `HSET key field value [field value ...]`
- `HVALS key`
- `INCR key`
- `INCRBY key increment`
- `MGET key [key ...]`
- `LINDEX key index`
- `LLEN key`
- `LPOP key`
- `LPUSH key value [value ...]`
- `LRANGE key start stop`
- `LREM key count element`
- `LSET key index element`
- `LTRIM key start stop`
- `SET key value [NX|XX] [GET] [EX seconds]`
- `TYPE key`
- `MSET key value [key value ...]`
- `RPOP key`
- `RPOPLPUSH source destination`
- `RPUSH key value [value ...]`
- `DEL key [key ...]`
- `EXPIRE key seconds`
- `KEYS pattern`
- `PERSIST key`
- `PEXPIRE key milliseconds`
- `PTTL key`
- `RENAME key newkey`
- `RENAMENX key newkey`
- `SCAN cursor [MATCH pattern] [COUNT n]`
- `SELECT db`
- `TTL key`
- `TOPK.ADD key item [item ...]`
- `TOPK.COUNT key item [item ...]`
- `TOPK.INFO key`
- `TOPK.INCRBY key item increment [item increment ...]`
- `TOPK.LIST key [WITHCOUNT]`
- `TOPK.QUERY key item [item ...]`
- `TOPK.RESERVE key topk [width depth decay]`
- `UNLINK key [key ...]`
- `ZADD key score member [score member ...]`
- `ZCARD key`
- `ZREM key member [member ...]`
- `ZREMRANGEBYLEX key min max`
- `ZREMRANGEBYSCORE key min max`
- `ZREVRANGEBYLEX key max min [LIMIT offset count]`
- `ZREVRANGEBYSCORE key max min [LIMIT offset count]`
- `ZRANGEBYLEX key min max [LIMIT offset count]`
- `ZRANGEBYSCORE key min max [LIMIT offset count]`
- `ZSCORE key member`
- `MULTI` / `EXEC` / `DISCARD`
- `PIRIN.BATCH` / `PIRIN.EXEC` / `PIRIN.DISCARD`
- `PIRIN.CHECK`
- `FLUSHALL`
- `FLUSHDB`

Example with `redis-cli`:

```bash
$ redis-cli -p 6379 SET hello world
OK
$ redis-cli -p 6379 GET hello
"world"
$ redis-cli -p 6379 KEYS 'he*'
1) "hello"
$ redis-cli -p 6379 SELECT 1
OK
$ redis-cli -p 6379 GET hello
(nil)
```

PirinDB rollback-on-any-error batch extension:

```bash
$ redis-cli -p 6379
127.0.0.1:6379> PIRIN.BATCH
OK
127.0.0.1:6379> SET a 1
QUEUED
127.0.0.1:6379> GET a
QUEUED
127.0.0.1:6379> PIRIN.EXEC
1) "OK"
2) "1"
```

> [!NOTE]
> `SSCAN` is not advertised because PirinDB does not currently implement the Redis set type. It returns an unsupported-command error rather than scanning unrelated database keys.

> [!NOTE]
> Standard client pipelining requires no server command. `MULTI`/`EXEC` follows Redis queue-time and runtime-error behavior. PirinDB's rollback-on-any-error semantics are available only under the `PIRIN.BATCH` extension names shown above.

> [!NOTE]
> For write-heavy Redis workloads, `--sync-policy=journal` can significantly reduce per-command latency by checkpointing the main database file less often while keeping recovery through the durable journal.

> [!NOTE]
> `--sync-policy=group` allows independently durable writes already buffered on one connection to share a flush. Replies remain ordered and are emitted only after their transactions are durable.

> [!NOTE]
> `BLPOP` and `BRPOP` are supported for queue-style consumers. They block the client connection until one of the requested lists receives a value or the timeout expires.

> [!NOTE]
> `RPOPLPUSH` and `BRPOPLPUSH` are supported for simple reliable-worker patterns: one connection can atomically move a job from `queue` into `processing`, so the job is no longer lost between pop and re-enqueue.

> [!NOTE]
> `KEYS` and `SCAN` traverse the selected Redis DB keyspace and include string, list, hash, Bloom, TopK, and sorted-set keys.

> [!NOTE]
> `CONFIG` is currently read-only in PirinDB. `CONFIG GET`, `CONFIG HELP`, and `CONFIG RESETSTAT` are supported; `CONFIG SET` and `CONFIG REWRITE` return an error.

> [!NOTE]
> `BF.ADD` and `BF.MADD` auto-create missing Bloom filters with defaults similar to RedisBloom: `error_rate=0.01`, `capacity=100`, `expansion=2`, scaling enabled.

> [!NOTE]
> `TOPK.RESERVE` follows RedisBloom defaults for omitted optional arguments: `width=8`, `depth=7`, and `decay=0.9`.

> [!NOTE]
> PirinDB supports Redis logical DBs `0..9` via `SELECT`. Redis data is stored in reserved Redis-only buckets and is isolated from the HTTP API keyspace, which continues to use the `main` bucket.

> [!NOTE]
> In cluster mode, wrong-shard requests return `MOVED slot host:port`. During the rebalance handoff window, the source shard may return `ASK slot host:port`; retry that command on the destination shard after sending `ASKING`.



## Storage API Quick Start

To start using pirindb storage engine, you need install the package:

```bash
$ go get github.com/timson/pirindb/storage/...
```

```Go
package main

import (
	"log"

	pirindb "github.com/timson/pirindb/storage"
)

func main() {
	db, err := pirindb.Open("test.db", pirindb.DefaultOptions())
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()
}

```

### Modify and Read Data

```Go
db.Update(func(tx *pirindb.Tx) error {
	bucket := tx.CreateBucket([]byte("foo"))
    if err := bucket.Put([]byte("foo"), []byte("bar")); err != nil {
        return err
    }
    return nil
})

db.View(func(tx *pirindb.Tx) error {)
    bucket := tx.GetBucket([]byte("foo"))
    value := bucket.Get([]byte("foo"))
    log.Println(string(value))
    return nil
})
```
> [!NOTE]
> If key value exceed the 1024 bytes, if automatically stored as a blob.


### Manual transaction management

```Go
tx := db.Begin(true)
defer tx.Rollback()
bucket := tx.CreateBucket([]byte("foo"))
bucket.Put([]byte("foo"), []byte("bar"))
tx.Commit()
```

### Cursors

Support for iterating over key-value pairs using cursors:
- `First()`: Move to the first key-value pair.
- `Next()`: Move to the next key-value pair.
- `Seek()`: Move to the first key-value pair that is greater than or equal to the provided key.
- `Last()`: Move to the last key-value pair.
- `Prev()`: Move to the previous key-value pair.

```Go
db.View(func(tx *pirindb.Tx) error {
    bucket := tx.GetBucket([]byte("foo"))
    cursor := bucket.Cursor()
    for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
        log.Printf("key: %s, value: %s", k, v)
    }
    return nil
})
```
Range scanning is also supported:

```Go
db.View(func(tx *pirindb.Tx) error {
    bucket := tx.GetBucket([]byte("foo"))
    cursor := bucket.Cursor()
    prefix := []byte("test")
    for k, v := cursor.Seek([]byte("bar")); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
        log.Printf("key: %s, value: %s", k, v)
    }
    return nil
})
```

> [!NOTE]
> Next() and Prev() methods works correctly only if the cursor is positioned on a valid key-value pair using 
> First(), Last() or Seek().

### Bucket Management

PirinDB provides a simple mechanism for managing bucket of data.

- `CreateBucket()`: Creates a new bucket with the provided name.
- `GetBucket()`: Retrieves an existing bucket by name.
- `DeleteBucket()`: Deletes a bucket by name.
- `Buckets()`: Returns a list of all buckets in the database.



## Inspiration and Credits

- [BoltDB](https://github.com/boltdb/bolt)
- [LibraDB](https://github.com/amit-davidson/LibraDB)
- [Database Internals](https://www.amazon.com/Database-Internals-Deep-Distributed-Systems/dp/1492040347) by Alex Petrov

PirinDB was named after the Pirin Mountains. 
The [Pirin](https://en.wikipedia.org/wiki/Pirin) Mountains are a mountain 
range in southwestern Bulgaria.
