# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.3.3] - 2026-XX-XX

### Changed
- Changed metrics collection to track bytes progressively during I/O operations (sync, compression, extraction) for more accurate progress reporting.
- Renamed compression metrics from `OriginalBytes`/`CompressedBytes` to `BytesRead`/`BytesWritten` to better reflect I/O direction and support extraction scenarios.
- Updated dependencies.

## [v1.3.2] - 2026-01-28

### Added
- Added a unique identifier (UUID) to the backup metadata (`.pgl-backup.meta.json`) to provide an immutable identity for backups, independent of their file path.
- Added a pre-flight check to verify that the source directory is readable.
- Added a pre-flight check to detect if source and target paths resolve to the same physical directory (e.g., via symlinks or bind mounts).
- Added `list` command to enumerate all backups in the repository, simplifying the restore process.
- Added `-sort` flag to the `list` command to control the display order (`desc` or `asc`).
- Added interactive backup selection to the `restore` command. If `-uuid` is omitted, a list of available backups is displayed for selection.

### Changed
- The `-mode` flag is now optional for the `restore` command. The system now attempts to locate the backup in incremental storage first, falling back to snapshot storage if not found.
- Refactored core engine functions (`Sync`, `Archive`) to return results explicitly instead of modifying input structs via side effects. This improves data flow clarity and moves metafile writing responsibility to the main runner.
- **BREAKING CHANGE**: The `restore` command now uses `-uuid` instead of `-backup-name` to identify backups. This ensures unambiguous selection of backups regardless of directory naming or location.
- **BREAKING CHANGE**: Renamed configuration field `paths.*.backupNamePrefix` to `paths.*.archiveEntryPrefix` to better reflect that it controls the naming of directories within the archive.
- Introduced a new internal `hints` package to decouple error handling. The engine now checks for ignorable error *behaviors* (e.g., "task disabled") rather than specific error types from sub-packages, making the system more modular.

### Fixed
- Added safety checks to ensure backup metadata is valid before attempting to write it or perform archiving/retention.

## [v1.3.1] - 2026-01-24

### Changed
- **BREAKING CHANGE**: Renamed configuration field `paths.*.backupDirPrefix` to `paths.*.backupNamePrefix` for clarity.
- **BREAKING CHANGE**: Removed `source` path from the persisted configuration file and metadata to improve privacy and portability. The `-source` flag is now mandatory for the `backup` command.
- Refactored internal configuration handling to strictly separate runtime parameters (`base`, `target`, `backup-name`) from persisted settings.

### Fixed
- Fixed configuration validation to use case-insensitive comparisons for path names, preventing potential collisions on Windows and macOS.
- Fixed configuration parsing to automatically normalize enum values (e.g., `Mode`, `CompressionFormat`) to lowercase, ensuring consistent behavior regardless of input case.

## [v1.3.0] - 2026-01-24

### Added
- Added `restore` command to recover files from backups.
  - Usage: `pgl-backup restore -base="/path/to/repo" -target="/restore/path" -backup-name="current" -mode="incremental"`
- Added `OverwriteBehavior` to extraction logic, allowing control over how existing files are handled (`always`, `never`, `if-newer`).
- Added `CompressionLevel` configuration (`default`, `fastest`, `better`, `best`) for `zip`, `tar.gz`, and `tar.zst` formats.

### Changed
- Refactored `pathcompression.Plan` into separate `CompressPlan` and `ExtractPlan` structs for better type safety and clarity.
- **BREAKING CHANGE**: Simplified configuration structure. Merged `Sync` and `Compression` settings into single top-level sections (removing `Incremental`/`Snapshot` split). Simplified `Archive` settings to focus on incremental rollover. Updated CLI flags to match (e.g., `-sync-incremental-engine` is now `-sync-engine`).
- **BREAKING CHANGE**: Renamed CLI flags for clarity. `-target` is now `-base` (the repository location). `-source` remains `-source` for backups. For restores, `-target` specifies the restore destination.

#### Migration Guide
To upgrade your existing `pgl-backup.config.json`, you must flatten the `sync`, `archive`, and `compression` sections. The `incremental` and `snapshot` subsections have been removed in favor of shared settings.

**Before:**
```json
"sync": { "incremental": { "engine": "native" }, "snapshot": { "engine": "native" } },
"archive": { "incremental": { "enabled": true, "intervalMode": "auto" }, "snapshot": { "enabled": true, "intervalMode": "manual" } },
"compression": { "incremental": { "enabled": true }, "snapshot": { "enabled": true } }
```

**After:**
```json
"sync": { "engine": "native" },
"archive": { "enabled": true, "intervalMode": "auto" },
"compression": { "enabled": true }
```

### Fixed

## [v1.2.0] - 2026-01-20

### Changed
- **BREAKING CHANGE**: Completely rewrote the core engine architecture to use a modular "Leaf Package" orchestration pattern. The `Runner` now coordinates distinct `Validator`, `Syncer`, `Archiver`, `Retainer`, and `Compressor` components.
- **BREAKING CHANGE**: Redesigned the configuration structure (`pgl-backup.config.json`) to be more hierarchical and logical. Settings are now grouped by domain (e.g., `sync`, `archive`, `retention`, `compression`).
- Introduced a new `planner` package to generate immutable execution plans from configuration before runtime.
- Refactored `flagparse` to map CLI flags to the new configuration structure.

## [v1.1.0] - 2026-01-10

### Added
- Added `prune` command to manually apply retention policies without running a full backup.

### Changed
- Refactored CLI architecture to use subcommands (`init`, `backup`, `version`) instead of top-level action flags.
- The `backup` command is now mandatory; implicit backup behavior has been removed.
- Replaced `-init` and `-init-default` flags with `init` and `init -default` commands.
- Replaced `-version` flag with `version` command.
- Exit with code 0 if user cancels `init -default` confirmation.

## [v1.0.0] - 2026-01-07

### Added
- Added `-init-default` flag to explicitly overwrite configuration with defaults.
- Added `-force` flag to bypass confirmation prompts.

### Changed
- Promoted v1.0.0-rc.1 to stable release.
- Fixed typo in usage output
- Refactored internal worker engines to use explicit Producer-Consumer patterns and fixed some edge cases.
- Added pre-flight check to prevent source/target path nesting.
- Compress osx release as tar.gz instead of zip
- Optimized directory synchronization to avoid redundant syscalls for existing directories.
- Updated `-init` flag behavior to preserve existing configuration settings, merging them with provided CLI flags.
- Changed default retention policy to keep 0 daily, 4 weekly, 0 monthly, and 0 yearly backups. This provides a simple 4-week history by default.

### Fixed
- Daylight Saving Time (DST) handling for archive intervals to correctly handle 23-hour and 25-hour days.
- Critical safety fix: Inaccessible source paths (e.g., permission denied) are now preserved in the destination instead of being deleted during the mirror phase.
- Fixed an issue where a destination file blocking a source directory creation would cause sync failures; the file is now correctly replaced by the directory.
- Fixed an issue where the backup process would fail if the current backup directory was locked (e.g., open in Windows Explorer) during archiving. It now logs a warning and proceeds with the sync.

## [v1.0.0-rc.1] - 2025-12-31

### Added
- Initial public release candidate.
- **Core Engine**: High-performance, concurrent sync engine (`native`) and `robocopy` support for Windows.
- **Backup Modes**: Support for efficient Incremental backups and point-in-time Snapshots.
- **Retention Policy**: Flexible GFS (Grandfather-Father-Son) rotation for hourly, daily, weekly, monthly, and yearly archives.
- **Compression**: Automatic archiving to `.tar.zst`, `.tar.gz`, or `.zip`.
- **Safety**:
    - Pre-flight checks for paths and permissions.
    - "Ghost Directory" protection for unmounted drives.
    - Cross-platform case-sensitivity detection.
    - Permission lockout protection.
- **Configuration**: JSON-based configuration (`pgl-backup.config.json`) with command-line overrides.
- **Observability**: Structured logging (JSON/Console) and detailed metrics.
- **Hooks**: Pre- and post-backup shell command execution.