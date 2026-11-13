# Redis Disk Perf Playground

Small manual playground for testing PirinDB as a disk-backed Redis-style KV store when the dataset is larger than RAM.

This tool does not go through the TCP RESP server. It writes and reads directly through the storage engine using the reserved Redis string bucket layout for DB `0` by default, which is the closest way to isolate disk/B-tree behavior from network overhead.

## What It Does

- fills the Redis string keyspace to an approximate target size
- closes and reopens the DB
- performs random key reads
- prints fill throughput, random-read throughput, logical bytes, used DB size, and file size

## Quick Start

Full fill + reopen + random reads:

```bash
go run ./playground/redis_disk_perf \
  -total-bytes 8GiB \
  -value-bytes 1024 \
  -batch-size 5000 \
  -random-reads 100000 \
  -progress-interval 2s
```

Prepare only:

```bash
go run ./playground/redis_disk_perf \
  -prepare-only \
  -total-bytes 8GiB \
  -value-bytes 1024
```

Read only against an existing DB:

```bash
go run ./playground/redis_disk_perf \
  -read-only \
  -db playground/redis_disk_perf/redis-disk-perf.db \
  -total-bytes 8GiB \
  -value-bytes 1024 \
  -random-reads 100000
```

## Useful Flags

- `-db` path to DB file
- `-total-bytes` target dataset size like `4GiB`, `8GiB`, `500MB`
- `-value-bytes` value size for each key
- `-batch-size` keys per write transaction
- `-random-reads` number of random reads to measure
- `-progress-interval` how often to print progress updates; `0` disables periodic progress
- `-key-prefix` generated key prefix
- `-redis-db` logical Redis DB index for bucket naming
- `-reset-db` remove existing DB/tlog before prepare phase
- `-prepare-only`
- `-read-only`
- `-sync-policy` one of `strict`, `journal`, `group`
- `-checkpoint-tx-threshold`
- `-read-seed`

## Notes

- Default sync policy is `journal`, because it is a more realistic fast-write setting for this kind of bulk load.
- Random reads currently run one read transaction per key, which is closer to the cost shape of individual Redis `GET` commands than a single shared read transaction.
- Progress lines print percent complete, current throughput, elapsed time, and ETA for both prepare and read phases.
- If you want to compare pure storage read speed without per-request transaction overhead, that should be a separate playground mode.
