# Local database validation

Run from the repository root with the Go version required by go.mod. No Redis installation, Docker service, external database, or CI account is needed. All database files and server processes belong to the suite. Never point these tests at a production database.

```sh
make test-db
make test-db-extended SEED=42
make test-db-soak DURATION=24h POLICY=group
make test-db-replay CASE=tmp/dbtest/run-.../case-...
```

`test-db` runs bounded regression/fault scenarios and short workloads; its timeout budget is ten minutes, but typical execution is much shorter. Extended mode adds a 10-second Go decoder fuzz campaign and persistence seeds and runs engine churn and server crash/restart loops for 55 minutes within a one-hour timeout. Soak defaults to 24 hours, with one persistent engine database and one persistent server database across restart cycles. `POLICY` selects the engine churn policy; server SIGKILL tests use strict mode. Strict, journal, and group modes are all covered by the short persistence/isolation/fault campaigns.

`SEED`, `WORKERS`, and `ARTIFACT_DIR` customize the local targets. `WORKERS` selects concurrent churn clients and Go test parallelism; bounded isolation histories deliberately remain four clients/two operations each. Test packages can execute concurrently. Use `go run ./cmd/dbtest --help` for runner flags. A shortened extended smoke check is:

```sh
go run ./cmd/dbtest --profile extended --duration 10s --seed 2
```

Results appear under `tmp/dbtest/run-*`: `events.jsonl` contains Go test events, and `report.json` records revision, dirty working tree, platform, profile, configuration, pass/fail/subcase counts, and required-scenario completion. Missing required scenarios or evidence write failures result in failure. Successful temporary databases are removed; failed cases are retained. Server logs rotate to eight recent child logs. Workload snapshots/restores are replaced rather than accumulated.

Failed storage cases retain `scenario.json`; persistence-cut cases also retain `case.json`, filesystem `trace.json`, and their files. Replay reruns the recorded scenario and seed. For persistence cuts it verifies the I/O operation, filename, offset, and size sequence through the requested cut. UUIDs are regenerated, so it is semantic schedule replay rather than byte-identical file replay. Concurrent real-server histories remain subject to OS scheduling; rerunning their seed does not reproduce a CPU schedule exactly. Keep the event log alongside any failure report.

## What is checked

- Existing atomicity/durability, freelist, corruption, migration, snapshot, ORM identity, and administration regressions.
- Multi-key bank invariants and acknowledged transaction bounds after every modeled crash cut, across all three sync policies.
- Separate drop-only and sampled partial/reordered persistence, with 512-byte and 4096-byte units, regardless of seed parity.
- Explicit ENOSPC, short-write, and sync-error injection, followed by recovery and validation of earlier acknowledged values.
- An independently checked serial-history search for bounded concurrent increment/read transactions; a known invalid history verifies that the checker rejects anomalies.
- A Go map oracle for randomized insert/update/delete workloads, mandatory final comparison, mandatory logical restore comparison, and physical integrity checks.
- Repeated interruptions during recovery before a final successful restart, and logical backup consistency while a concurrent writer is active.
- Backup replacement and offline READY publication interrupted at filesystem operation boundaries, including directory reachability after loss of uncommitted parent entries.
- Real locally launched server processes killed with SIGKILL after acknowledged HTTP writes, then restarted and checked through HTTP. Extended/soak cycles use different values each time, so earlier-cycle data cannot satisfy later acknowledgements.

## Persistence model and limits

Production storage uses the OS adapter behind an internal filesystem interface. Test-only model files retain separate volatile bytes, durable inode images, and synced directory entries. A successful file sync persists preceding file writes/size changes. Directory sync commits namespace entries separately. Crash reconstruction preserves only names reachable through durable parents.

For partial persistence, unsynced write fragments and truncations are sampled and reordered. Directory publication samples volatile versus synced directory images. This is bounded exploration of operation cuts and sampled persistence outcomes, not exhaustive exploration of every possible device state. The model does not emulate a specific filesystem, controller, or drive. Its self-tests distinguish file durability from directory durability, including an unpublished parent directory.

Real process kills leave the host page cache intact. They test process recovery, not physical power removal. Corruption rejection and missing-acknowledgement outcomes are interpreted according to the specific scenario; a readable but incorrect recovered database is never accepted as success.

The engine model currently uses small bank transactions; existing randomized freelist/B-tree tests supplement them. It does not model distributed partitions or establish cluster replication/failover guarantees. The suite does not require Redis-equivalent command behavior. Runtime summaries and file sizes support investigation; there are no machine-independent throughput or memory-leak certification thresholds.

A passing run is evidence for the scenarios and model exercised. It is not universal ACID proof or hardware power-loss certification. Complete longer runs on the intended production storage before using their results for release qualification.
