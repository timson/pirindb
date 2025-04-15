# Changelog

All notable changes to this project will be documented in this file.

## [0.0.2] - 2025-04-16
### Added
- Transactional log (double write) mechanism to improve durability and ACID compliance
- `Freelist` dirty flag to avoid unnecessary writes when no changes are made  
  *(TODO: Optimize further by writing only affected pages)*
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
