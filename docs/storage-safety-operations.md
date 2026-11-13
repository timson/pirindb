# Storage safety, migration, backup, and restore

PirinDB uses a write-ahead recovery journal, checksummed pages, and verified logical snapshots. This guide describes file-format migration, backups, restore, and administrative access. See [local database validation](local-database-tests.md) for test coverage and [release qualification](releasing.md) before tagging a release.

## On-disk compatibility

New databases use metadata v0.4 and identity-bound WAL v4. Metadata carries a random database ID and a monotonically increasing commit sequence. The journal must match that ID, and an active journal must not predate any surviving metadata identity record. WAL header copies occupy separate 4096-byte blocks. Journal body sync precedes publication of the commit header; durable inactive headers precede compaction.

Opening a clean checksummed v0.3 database automatically upgrades its metadata, mirror first, with a durability barrier between copies. **Save a matching offline backup before opening it with this version. Older binaries cannot open the upgraded format.** Mixed v0.3/v0.4 metadata is accepted only for the controlled upgrade case where existing fields agree.

New default journal paths append `.tlog` to the complete database filename: `pirin.db.tlog`. Existing legacy `pirin.tlog` paths are detected and retained. If both possible log files exist, startup refuses to guess; use `Options.WithTxLogPath` with the verified matching path. Never delete a journal merely to bypass a startup error.

Active unbound journals are rejected by default. For a verified matching v0.3 DB/WAL pair, an offline migration program may use:

```go
opts := storage.DefaultOptions().
    WithTxLogPath(verifiedJournalPath).
    WithLegacyRecovery(true)
db, err := storage.Open(databasePath, opts)
// Stop on any error. After success, cleanly close before normal startup.
```

This explicitly trusts that pair; it cannot retrospectively prove ownership. Legacy v1/v2 journals must be recovered and cleanly checkpointed with the original compatible binary before upgrading. Legacy v0.2 databases remain readable/exportable; writes require the explicit legacy migration option. Move their data into a fresh database using snapshot export and offline restore. Do not use legacy write compatibility as a production deployment mode.

If both complete metadata checksums are damaged, startup can use a surviving independently checksummed target identity record and a matching journal. It never adopts an arbitrary journal's identity as proof of ownership. Conflicting identities or missing identity anchors fail closed. Preserve all files for investigation.

## Verified snapshots

Exports now write snapshot v2. Its SHA-256 trailer covers framing, source database identity, creation time, bucket names, item counts, sequences, keys, and values. Imports reject wrong digests, incomplete streams, trailing data, duplicate buckets, and unordered/duplicate keys. V1 snapshots are rejected because they lack both integrity verification and sequence state; re-export from the original database or use a separately reviewed recovery conversion with explicit sequence repair.

Exports use a temporary file on the destination filesystem, file sync, atomic publication, and parent-directory sync. `overwrite=false` atomically refuses an existing destination. `overwrite=true` replaces the destination without first deleting it. Storage rejects active database/journal destinations and their aliases. Use immutable backup names and retain previous verified generations; the API does not implement a retention scheduler or an off-host backup service.

An export holds a read transaction throughout the scan and file writes. Readers can proceed, but writers wait. Schedule backups accordingly. Copy a completed snapshot off-host and compare its digest before recording it as usable. A digest protects against accidental damage; trusted delivery or signatures are needed if deliberate tampering is a concern.

## Offline restore into a new directory

For large databases, prefer the new offline command:

```sh
pirindb restore --snapshot /backups/pirin-2026-11-12.snapshot \
    --directory /data/restored-generation-001
```

The destination directory must not exist. Restore batches writes so total snapshot size can exceed the configured transaction limit. Each individual value and its B-tree/bookkeeping work must still fit that limit. The Go API `storage.RestoreDBToDirectory` accepts `Options` for that budget and sync policy.

Restore verifies the complete snapshot digest and database integrity, closes/checkpoints the new database, then durably publishes a `READY` manifest. It contains the database and journal filenames, new identity, snapshot digest, and restored counts. Ordinary errors remove the new directory. A process interruption may leave an incomplete directory **without READY**; do not open or serve it. Remove it after investigation and restore into another new directory.

This command never replaces the running database. After it succeeds:

1. Validate READY, expected counts, bucket sequences, application records/indexes, and Redis TTL semantics where applicable.
2. Stop the old server and wait for successful shutdown.
3. Configure `db.filename` to the new directory's `database.db`; keep its `database.db.tlog` with it.
4. Start and verify the new server. Retain the previous complete directory for rollback.

Avoid replacing DB and journal with separate uncoordinated renames. The command is a standalone restore, not coordinated cluster recovery. Cluster topology, shard owners, and epochs need their own recovery procedure.

## HTTP administration

DB transfer endpoints and job status require `Authorization: Bearer <db.admin_token>`. With no token configured they return 503; missing/incorrect credentials return 401. Configure a private, existing backup directory. Example TOML:

```toml
[db]
filename = "/data/pirin.db"
admin_token = "replace-with-a-secret-from-your-deployment"
backup_directory = "/backups/pirindb"
restore_enabled = false
```

Use the existing configuration environment overrides for secrets, e.g. `PIRINDB_DB_ADMIN_TOKEN`, rather than committing real tokens. This token protects transfer administration; the ordinary KV API still requires network/application access controls appropriate to the deployment.

Export/import paths must remain inside the configured directory after resolving aliases. Only one transfer runs at a time; recent job status is bounded to 128 entries. A 202 response is only admission: poll the authenticated status endpoint until `done` or `failed`.

Live import is disabled unless `db.restore_enabled=true`, Redis is disabled, and cluster mode is disabled. During import, HTTP KV requests return 503. It remains one atomic transaction and therefore has a size limit. Prefer offline restore for production datasets. Turn restore admission back off after maintenance.

## Errors and acknowledgement

`ErrCommitUncertain` means a commit failed after its outcome could have become durable, or rollback of the journal itself could not be established. Recover the database and resolve the application operation before retrying. A missing response can correspond to a complete committed operation. Use idempotent operation IDs for application retries.

`CommitAsync()` publishes work to the commit mechanism; successful `future.Wait()` is the acknowledgement. A health check reports lifecycle/fatal durability status, not a complete integrity scan. `DB.Check()` verifies logical/physical structure and allocation ownership; it does not prove values match an external source or protect against loss of a shard volume.

Cluster mode remains sharding without replication. Backups define a recovery point after volume loss; they cannot provide zero acknowledged-write loss after that loss.
