# PirinDB Redis Compatibility and Operating Envelope

PirinDB provides a persistent RESP2 Redis-compatible subset. `COMMAND`, `COMMAND LIST`, `COMMAND COUNT`, and `COMMAND INFO` are generated from the same registry used by the server. That registry is the authoritative command advertisement.

## Cluster discovery

Cluster mode supports `CLUSTER INFO`, `CLUSTER KEYSLOT`, `CLUSTER NODES`, `CLUSTER SHARDS`, and `CLUSTER SLOTS`. `CLUSTER INFO` reports the persisted slot map, membership, and epoch. `CLUSTER NODES` reports every PirinDB storage node as a master, marks the queried node with `myself`, and includes importing or migrating slot markers during the externally visible `ASK` phase. The regular `INFO` reply advertises `redis_mode:cluster` and `cluster_enabled:1` so Redis cluster tooling can recognize a PirinDB cluster node.

PirinDB does not use Redis's separate node-to-node cluster bus or Redis replication links. Consequently, `CLUSTER NODES` advertises cluster-bus port `0`, cluster message counters are `0`, and no replica entries are emitted. Configured PirinDB node IDs are retained in the Redis topology replies so they match operator and HTTP control-plane status.

## Data types

Supported logical types are strings, lists, hashes, sorted sets, Bloom filters, and TopK sketches. Redis sets, streams, scripting, pub/sub, and modules outside the documented Bloom and TopK commands are not implemented. In particular, `SSCAN` is unsupported because returning database keys would not be valid set-member behavior.

## Transactions and pipelines

Normal client pipelining requires no command: send multiple RESP commands without waiting for each reply. Replies remain in command order.

`MULTI`, `EXEC`, and `DISCARD` use Redis transaction behavior:

- valid commands return `QUEUED`;
- a queue-time syntax/arity error marks the transaction dirty and makes `EXEC` return `EXECABORT`;
- runtime command errors occupy their matching `EXEC` array element;
- later queued commands continue and commit;
- `WATCH` is not advertised.

PirinDB also provides an explicit rollback batch extension:

- `PIRIN.BATCH`
- `PIRIN.EXEC`
- `PIRIN.DISCARD`

This extension executes one storage transaction and rolls the entire batch back on the first command error. It is not Redis `MULTI` semantics.

## Persistence policies

- `strict`: each ordinary write completes its own durable database synchronization.
- `journal`: each acknowledged write is durable in the recovery journal; the main file is checkpointed according to the configured threshold.
- `group`: independent transactions may share one durable flush. Buffered writes on one client connection use asynchronous commit futures, preserve same-connection order and visibility, and receive replies only after durability.

PirinDB atomic batches use one storage transaction under every policy.

## Expiry and deletion

TTL deadlines use checked integer arithmetic. A positive duration whose absolute deadline cannot be represented returns an error before mutation. Reads treat expired keys as absent.

The active expiry worker processes bounded batches round-robin across logical databases. `INFO`, `KEYS`, `SCAN`, and `DBSIZE` do not initiate an unbounded cleanup transaction.

`UNLINK` removes directory, TTL, and slot visibility atomically, then leaves physical reclamation to the durable garbage-collection queue. Large `DEL` replacements and expired composite values use the same path. Garbage tasks have persisted cursors and bounded record/byte budgets.

`FLUSHDB ASYNC` and `FLUSHALL ASYNC` atomically move fixed bucket roots into an obsolete generation and return. `SYNC` (and the command without a mode) waits for the relevant durable cleanup queue to drain. Object-ID counters are retained across flushes and IDs are not reused.

## Large collections

List indexing, short ranges, element replacement, and trimming navigate only relevant segments. Detached trim chains are reclaimed by resumable garbage tasks.

Large standalone `LREM`, `ZREMRANGEBYSCORE`, and `ZREMRANGEBYLEX` operations construct a hidden shared object in bounded transactions, atomically switch the key directory, and queue the old generation. Reads continue seeing the old value until activation. A crash before activation leaves the original authoritative; startup converts abandoned build records into cleanup tasks.

Large copy-on-write transformations are deliberately rejected inside `MULTI` and `PIRIN.BATCH`, where they cannot span storage transactions without changing the transaction contract.

## Persistent format and migration

Every logical database has a versioned authoritative key directory containing type, storage format, object ID, expiry deadline, flags, and object generation.

New composite objects use fixed shared buckets with big-endian `objectID` prefixes. Legacy private-bucket objects remain readable. The background migrator:

1. scans the key directory in bounded batches;
2. copies one object into a hidden shared object prefix;
3. verifies expected record counts;
4. atomically changes type metadata and the key directory;
5. queues legacy buckets for durable reclamation.

A key-scoped mutation latch prevents source changes during copying. After restart, a partial hidden destination is abandoned safely and copied again under a new monotonic object ID.

## Limits

The RESP parser accepts at most 65,536 arguments and 64 MiB of encoded request data per array command, with a 64 KiB limit on protocol lines (including inline commands). Oversized lengths are rejected before payload allocation; malformed requests receive a protocol error and the connection closes.

The storage transaction limit defaults to 64 MiB and is configurable through storage options. One-transaction writes receive a stable error before mutation when request payload and estimated dirty state exceed this limit. Large collection transformations listed above use bounded copy-on-write instead.

Connection pipeline collection defaults to 256 commands or 8 MiB of request data per buffered batch. Expiry, migration, and garbage-collection limits are visible through `CONFIG GET pirindb-*` and `INFO`.

## Integrity

`PIRIN.CHECK` runs the read-only Redis logical checker and returns an empty array on success. The checker validates key ownership (including orphaned string records), type/object identity, reachable list segments and length, hash cardinality, both sorted-set indexes, Bloom/TopK metadata, both directions of TTL and slot indexes, hidden-build/migration state, and garbage-task encoding. It does not repair data. Physical page/B-tree/freelist checking remains available through `DB.Check()`.
