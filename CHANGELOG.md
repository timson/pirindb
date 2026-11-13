# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Storage safety

- Fix freelist serialization when chain expansion consumes released pages, and restore allocator bookkeeping after transaction rollback.
- Add identity-bound database metadata v0.4 and WAL v4, commit sequence checks, separately aligned headers, ordered durability barriers, and prevalidated recovery.
- Upgrade clean v0.3 metadata safely; reject unverified active legacy journals by default. Legacy v0.2 writes require explicit migration compatibility.
- Add checksummed snapshot v2 with sequence preservation and active-file protection. Snapshot v1 is rejected; re-export or perform a reviewed recovery conversion.
- Add bounded offline `pirindb restore` into a new directory with a verified READY manifest.
- Publish generated ORM IDs only after successful commit; expose uncertain commit errors.
- Require transfer administrator credentials and backup-directory confinement; live restore requires explicit standalone maintenance configuration.
- New databases use `<database filename>.tlog`; existing legacy log paths are detected and ambiguous paths rejected.

See [migration and operations](docs/storage-safety-operations.md) before upgrading and [release qualification](docs/releasing.md) before tagging.

### Development

- Add local database validation, fault simulation, crash/recovery checks, replay artifacts, and extended/soak profiles.
- Move storage fixture helpers (`CreateTestDB`, `OpenTestDB`, `CloseTestDB`, `TempFileName`) to test-only code; downstream test callers should use `testing.T.TempDir` and `storage.Open`. Remove unused internal wrappers.

## [0.0.2] - 2025-04-16
### Added
- Transactional log (double write) mechanism to improve durability and ACID compliance
- `Freelist` dirty flag to avoid unnecessary writes when no changes are made
- Configurable database options:
    - File open mode
    - Transaction log filename
    - Recovery toggle to reapply the transaction log on restart

### Changed
- Switched from `mmap` to manual file read/write for better control over disk persistence

### Fixed
- Deadlock issue in write transactions

## [0.0.1] - 2025-03-30
- Initial release
