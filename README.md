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
- [ ] Sharding support (naive or consistent hashing)
- [ ] Replication support

## Project Status

**PirinDB** is intended purely as a hobby and educational project. It is not designed to be used  
as a production-grade database. Instead, it's a playground for those interested in learning and  
exploring the inner workings of database management systems.

## Quick Start
You need Go 1.22 or later to build PirinDB.

To build PirinDB server and client, run:

```bash
$ make build
```
It will create two binaries in the `bin` directory: `pirindb` and `pirindb-cli`.

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
```

- `strict` is the default double-write mode: one durable journal sync and one durable main DB sync per write transaction.
- `journal` keeps full crash recovery semantics, but only fsyncs the journal on each transaction and checkpoints the main DB file every `N` committed write transactions.
- `group` batches concurrent write transactions into one durable flush group. Readers are held back until the batch becomes durable, so committed writers share one `txlog + db` sync cycle.

The same settings can be supplied via environment variables:

```bash
$ PIRINDB_DB_SYNC_POLICY=journal PIRINDB_DB_CHECKPOINT_TX_THRESHOLD=64 ./bin/pirindb
$ PIRINDB_DB_SYNC_POLICY=group PIRINDB_DB_GROUP_COMMIT_TX_THRESHOLD=16 PIRINDB_DB_GROUP_COMMIT_WINDOW_MS=1 ./bin/pirindb
```

To start the CLI client, run:

```bash
$ ./bin/pirindb-cli
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

## Redis Interface

PirinDB now exposes a Redis-compatible RESP endpoint intended for simple cache and key-value integrations.
Currently supported commands are:

- `COMMAND`
- `BF.ADD key item`
- `BF.EXISTS key item`
- `BF.MADD key item [item ...]`
- `BF.MEXISTS key item [item ...]`
- `BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]`
- `BLPOP key [key ...] timeout`
- `BRPOPLPUSH source destination timeout`
- `BRPOP key [key ...] timeout`
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
- `SSCAN key cursor [MATCH pattern] [COUNT n]`
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
- `PIPELINE` / `MULTI`
- `EXEC`
- `FLUSHALL`
- `FLUSHDB`
- `DISCARD`

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

Atomic batched execution:

```bash
$ redis-cli -p 6379
127.0.0.1:6379> PIPELINE
OK
127.0.0.1:6379> SET a 1
QUEUED
127.0.0.1:6379> GET a
QUEUED
127.0.0.1:6379> EXEC
1) "OK"
2) "1"
```

> [!NOTE]
> `SSCAN` is implemented as a compatibility alias over PirinDB's single keyspace scan. PirinDB does not currently implement Redis set data types.

> [!NOTE]
> `PIPELINE` and `MULTI` are aliases. Queued commands are executed atomically on `EXEC` inside one PirinDB transaction, and the whole batch is rolled back if any command fails.

> [!NOTE]
> For write-heavy Redis workloads, `--sync-policy=journal` can significantly reduce per-command latency by checkpointing the main database file less often while keeping recovery through the durable journal.

> [!NOTE]
> `--sync-policy=group` is intended for concurrent write-heavy workloads. A single client issuing one write at a time may not benefit, because group commit needs overlapping writers to amortize the flush cost.

> [!NOTE]
> `BLPOP` and `BRPOP` are supported for queue-style consumers. They block the client connection until one of the requested lists receives a value or the timeout expires.

> [!NOTE]
> `RPOPLPUSH` and `BRPOPLPUSH` are supported for simple reliable-worker patterns: one connection can atomically move a job from `queue` into `processing`, so the job is no longer lost between pop and re-enqueue.

> [!NOTE]
> `KEYS`, `SCAN`, and `SSCAN` traverse the selected Redis DB keyspace and include string, list, hash, bloom, topk, and sorted-set keys. `SSCAN` remains a compatibility alias because PirinDB does not implement Redis set data types yet.

> [!NOTE]
> `CONFIG` is currently read-only in PirinDB. `CONFIG GET`, `CONFIG HELP`, and `CONFIG RESETSTAT` are supported; `CONFIG SET` and `CONFIG REWRITE` return an error.

> [!NOTE]
> `BF.ADD` and `BF.MADD` auto-create missing Bloom filters with defaults similar to RedisBloom: `error_rate=0.01`, `capacity=100`, `expansion=2`, scaling enabled.

> [!NOTE]
> `TOPK.RESERVE` follows RedisBloom defaults for omitted optional arguments: `width=8`, `depth=7`, and `decay=0.9`.

> [!NOTE]
> PirinDB supports Redis logical DBs `0..9` via `SELECT`. Redis data is stored in reserved Redis-only buckets and is isolated from the HTTP API keyspace, which continues to use the `main` bucket.



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
