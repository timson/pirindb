# PirinDB

<img src="images/pirindb_logo.png" alt="PirinDB logo" width="200">

PirinDB is a Go key-value database with an embedded B-tree storage engine, an
HTTP server, a Redis-compatible RESP2 interface, a CLI client, and a Kubernetes
operator for sharding.

This is an experimental database. Local correctness tests are available, but
production qualification requires sustained testing on the intended storage and
verified restore drills. Cluster mode provides sharding, without replication or
automatic failover.

## Build and run

Use Go 1.26 or later; the module requirement is in [go.mod](go.mod).

```sh
make build
./bin/pirindb
```

The build produces `pirindb`, `pirin-cli`, and `pirindb-operator` in `bin/`.
By default the server listens on `127.0.0.1:4321` for HTTP and
`127.0.0.1:6379` for RESP2. A new database uses `pirin.db` and
`pirin.db.tlog` in the working directory. Existing legacy journal paths are
detected; read the [migration guide](docs/storage-safety-operations.md) before
opening an existing database with this version.

```sh
./bin/pirindb --help
./bin/pirindb --db /data/pirin.db --redis-enabled=false
./bin/pirindb --sync-policy=group --group-commit-tx-threshold=16 --group-commit-window-ms=1
./bin/pirin-cli --help
```

Configuration can be supplied through a TOML file (`--config`) and environment
variables prefixed with `PIRINDB_`. For example:

```sh
PIRINDB_DB_SYNC_POLICY=journal PIRINDB_DB_CHECKPOINT_TX_THRESHOLD=64 ./bin/pirindb
```

Storage sync policies:

- `strict` syncs the WAL body, publishes and syncs its commit header, then writes
  and syncs the main database before acknowledging the transaction.
- `journal` preserves acknowledged writes in the synced WAL and checkpoints the
  main database according to its threshold and journal-size limits.
- `group` batches independent transactions into a shared durable flush. Readers
  wait for publication, and callers must wait for commit futures before treating
  asynchronous writes as acknowledged.

The shared node cache defaults to 32 MiB and can be configured with
`--node-cache-mb`. Writes are serialized; long read transactions, including
snapshot exports, block writers.

## Server interfaces

The HTTP API provides:

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/health` | Lifecycle and fatal-storage health |
| GET | `/api/v1/kv/{key}` | Read a value |
| POST | `/api/v1/kv/{key}` | Store a plain-text value |
| DELETE | `/api/v1/kv/{key}` | Delete a value |
| GET | `/api/v1/db/status` | Database statistics |
| POST | `/api/v1/db/export` | Start a logical snapshot export |
| POST | `/api/v1/db/import` | Start a maintenance restore |
| GET | `/api/v1/db/jobs/{jobID}` | Read transfer job status |

```sh
curl -X POST --data-binary 'hello' http://127.0.0.1:4321/api/v1/kv/example
curl http://127.0.0.1:4321/api/v1/kv/example
```

Transfer administration requires a configured `db.admin_token` and
`Authorization: Bearer <token>`. Paths are server-local and must remain inside
`db.backup_directory`. Live imports additionally require `db.restore_enabled`,
standalone mode, and the Redis listener disabled. A `202 Accepted` response means
the job was queued; poll the authenticated job endpoint for completion.

For exports, the JSON request is:

```json
{"path":"/backups/pirin.snapshot","scope":"db","overwrite":false}
```

Bucket exports use `"scope":"bucket"` and `"bucket":"users"`. Imports replace
all database buckets or the selected target bucket. Follow the
[backup and restore guide](docs/storage-safety-operations.md) for configuration,
sequence-preserving snapshots, and recovery procedures.

The RESP2 server implements strings, lists, hashes, sorted sets, Bloom filters,
TopK sketches, expiry, transactions, and pipelines. It implements a Redis subset;
full Redis compatibility is not a project requirement. `COMMAND` is the
server's command advertisement. See the
[compatibility and operating envelope](docs/redis-compatibility.md) for semantics
and limits, and [performance notes](docs/redis-performance.md) for measurements.

## Embedded storage

```sh
go get github.com/timson/pirindb/storage
```

```go
package main

import (
    "log"

    "github.com/timson/pirindb/storage"
)

func main() {
    db, err := storage.Open("example.db", storage.DefaultOptions())
    if err != nil {
        log.Fatal(err)
    }
    defer func() {
        if err := db.Close(); err != nil {
            log.Print(err)
        }
    }()

    err = db.Update(func(tx *storage.Tx) error {
        bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
        if err != nil {
            return err
        }
        return bucket.Put([]byte("alice"), []byte("hello"))
    })
    if err != nil {
        log.Fatal(err)
    }

    err = db.View(func(tx *storage.Tx) error {
        bucket, err := tx.GetBucket([]byte("users"))
        if err != nil {
            return err
        }
        value, found, err := bucket.GetE([]byte("alice"))
        if err != nil {
            return err
        }
        log.Printf("found=%t value=%s", found, value)
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

`Update` commits on success and rolls back on callback errors. `View` opens a
read transaction. Manual transactions use `BeginE`, `Commit`, and `Rollback`.
Buckets provide `GetE`, `Put`, `Remove`, `ForEach`, cursors, and sequences. Large
values use blob pages; streaming APIs avoid materializing entire values.
`DB.Check()` checks physical structure and freelist ownership.

Commit errors marked `ErrCommitUncertain` require recovery and outcome resolution
before an application retries. See the [durability contract](docs/storage-safety-operations.md#errors-and-acknowledgement).

## Backup and offline restore

Snapshot v2 preserves bucket sequences and includes a SHA-256 content digest.
Old snapshot v1 files must be re-exported or converted through a reviewed recovery
procedure. New metadata v0.4 and WAL v4 are not readable by older binaries.

```sh
./bin/pirindb restore --snapshot /backups/pirin.snapshot --directory /data/restored
```

The destination must not exist. Restore uses bounded transactions and publishes
a `READY` manifest only after validation and clean shutdown. It never replaces
the running database. Keep the DB and WAL together when activating the restored
directory; consult the [operations guide](docs/storage-safety-operations.md).

## Development and validation

```sh
make test
make vet
make test-db
make test-db-extended SEED=42
make test-db-soak DURATION=24h POLICY=group
make test-db-replay CASE=tmp/dbtest/run-.../case-...
```

The local suite runs without Redis, Docker, or a CI service. It covers transaction
invariants, concurrent histories, modeled persistence faults, process crashes,
freelist regressions, and backup/restore consistency. See the
[local test guide](docs/local-database-tests.md) for reports, replay, and model
limitations. A passing run is evidence for those scenarios, not universal ACID
proof or hardware power-loss certification.

The `playground/` directory contains maintained dataset loaders and performance
experiments. `examples/cluster/` contains local cluster configurations.

## Cluster and releases

- [Cluster guide: local setup, Kubernetes, scaling, compatibility, and recovery](docs/cluster.md)
- [Release preparation and qualification](docs/releasing.md)
- [Changelog](CHANGELOG.md)

## Credits

The storage engine draws on [LibraDB](https://github.com/amit-davidson/LibraDB),
[BoltDB](https://github.com/boltdb/bolt), and Alex Petrov's *Database Internals*.

## License

See [LICENSE](LICENSE).
