# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v1.0.0] - 2026-xx-xx

### Changed
- Promoted v1.0.0-rc.1 to stable release.
- Fixed typo in usage output
- Refactored internal worker engines to use explicit Producer-Consumer patterns and fixed some edge cases.
- Added pre-flight check to prevent source/target path nesting.
- Compress osx release as tar.gz instead of zip
- Optimized directory synchronization to avoid redundant syscalls for existing directories.

### Fixed
- Daylight Saving Time (DST) handling for archive intervals to correctly handle 23-hour and 25-hour days.
- Critical safety fix: Inaccessible source paths (e.g., permission denied) are now preserved in the destination instead of being deleted during the mirror phase.
- Fixed an issue where a destination file blocking a source directory creation would cause sync failures; the file is now correctly replaced by the directory.

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