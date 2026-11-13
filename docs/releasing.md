# Release preparation

Keep release notes in CHANGELOG.md under Unreleased until choosing a version.
The database format version, application version, and cluster protocol version
are separate compatibility contracts; do not infer one from another.

## Source checks

Run from a clean checkout with the Go toolchain required by go.mod:

```sh
go test ./...
go vet ./...
go test -race ./storage ./orm/... ./cmd/pirindb ./operator/controllers
make test-db
make build
git diff --check
```

Before merging, include intended new source/test/documentation files, inspect the
complete diff, and confirm no local databases, snapshots, binaries, test reports,
credentials, editor state, or internal implementation plans are staged. Generated
local outputs belong in ignored `bin/` and `tmp/` directories.

Build/test results from a developer machine are useful review evidence. Record
the tested revision, toolchain, platform, and any omitted qualification steps in
the release notes. Database test reports are local artifacts, not release assets.

## Compatibility and qualification

- Back up existing DB/WAL pairs before opening them with the new binary. Clean
  v0.3 files upgrade to v0.4; older binaries cannot read the upgraded metadata.
- Explain WAL filename detection, active legacy-journal handling, snapshot v1
  rejection, and the new authenticated transfer endpoints in release notes.
- Run the extended database suite and a sustained soak on the intended storage.
  Use multiple recorded seeds and each sync policy intended for deployment.
- Verify a snapshot copied off-host by restoring it into a fresh directory and
  comparing application data, sequences, indexes, counts, and integrity results.
- Distinguish simulated persistence faults from real process kills and physical
  power-loss testing. Do not describe unit-test passes as hardware certification.

For cluster releases, also validate generated CRDs/Kustomize manifests, transfer
fuzzing, disposable kind E2E tests, scaling/drain, and restart preservation with
the production images. See [cluster operations](cluster-operations.md) and the
[cluster release envelope](cluster-release-v0.1.md). Cluster mode has no shard
replication or automatic failover; losing a shard volume requires recovery from
backup. Coordinated cluster-wide point-in-time restore remains unsupported.

The one-hour extended and full 24-hour local soak profiles must be recorded as
pending until actually run. Shortened smoke runs validate the test machinery,
not the duration-dependent qualification.

## Tagging and publishing

After review and qualification, select the application version, update
`cmd/pirindb/const.go` and CHANGELOG.md, and review the migration notes. Merge the
reviewed branch into master, then tag that exact commit. Use the selected version
for the container builder's VERSION argument and retain reproducible build
metadata. Avoid publishing a tag or container image from a dirty working tree.

Install and upgrade procedures are in the
[storage operations guide](storage-safety-operations.md). Keep a matching offline
backup for rollback; switching back to an older binary is not an on-disk downgrade
procedure.
