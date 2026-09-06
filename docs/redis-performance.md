# Redis Performance Verification

Run correctness and focused benchmarks from the repository root:

```bash
go test ./cmd/pirindb -count=1
go test ./cmd/pirindb -run '^$' \
  -bench 'BenchmarkRedis(String|Raw|List|StandardPipeline)' \
  -benchmem -count=5
go test ./storage -run '^$' -bench . -benchmem -count=5
```

Run the optional differential suite against a local reference Redis endpoint:

```bash
PIRINDB_REDIS_REFERENCE_ADDR=127.0.0.1:6380 \
  go test ./cmd/pirindb -run TestRedisSupportedSubsetAgainstReference -count=1
```

The checked-in gates cover:

- key-directory lookup and string operations;
- segment-local list index/range/set paths on a 50,000-item fixture;
- bounded expiry batches;
- fixed shared-bucket catalog growth;
- online migration and restart cleanup;
- one-connection standard pipeline participation in one group-commit batch;
- large list and sorted-set hidden rebuilds;
- physical and Redis logical integrity.

Benchmark comparisons should record machine, Go version, durability policy, transaction limit, value size, client count, and pipeline depth. A release candidate should not accept an unexplained regression above 10 percent in the same environment.

## 2026-07-12 qualification sample

Environment: Apple M1 arm64, macOS 26.5.1, Go 1.26.0. These are local regression measurements, not universal capacity claims.

Direct storage paths used journal durability and the default 64 MiB transaction limit:

| Path | Result |
| --- | ---: |
| Existing string `GET` | 873 ns/op |
| Authoritative string type lookup | 514 ns/op |
| `LINDEX 0`, 50,000 values | 6.75 us/op |
| `LINDEX -1`, 50,000 values | 3.74 us/op |
| `LRANGE 0 9`, 50,000 values | 6.91 us/op |
| Durable `LSET 0`, 50,000 values | 8.60 ms/op |

The standard RESP pipeline benchmark used group durability, a 16-transaction flush threshold, a 2 ms group window, and one connection:

| Pipeline depth | Commands/s | Time/command |
| ---: | ---: | ---: |
| 1 | 92 | 10.82 ms |
| 16 | 1,619 | 0.618 ms |
| 64 | 1,627 | 0.615 ms |
| 256 | 1,471 | 0.680 ms |

Depth 16 delivered about 17.5 times the depth-1 throughput in this setup, exceeding the 4-times release target. Throughput plateaus beyond depth 16 because the configured group threshold deliberately starts another durable batch.

The opt-in release-scale gate is available as `make scale-redis`. On the same machine it created one million one-field hashes in 49.43 seconds (20,231 keys/s), kept the physical bucket count unchanged after the first 1,000 keys, measured `INFO` p95 at 57.8 us, and passed `DB.Check()`.

The expanded five-minute local soak completed 40 consecutive iterations covering expiry, migration/restart, group-policy restart, large COW operations, generation/exact-key GC, logical integrity, Bloom, TopK, randomized B-tree operations, and freelist checks. The manually dispatched CI workflow defaults to a four-hour run.
