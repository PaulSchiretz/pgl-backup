# pgl-backup

[![Go Reference](https://pkg.go.dev/badge/github.com/paulschiretz/pgl-backup.svg)](https://pkg.go.dev/github.com/paulschiretz/pgl-backup) [![License](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE) [![Go Report Card](https://goreportcard.com/badge/github.com/paulschiretz/pgl-backup)](https://goreportcard.com/report/github.com/paulschiretz/pgl-backup) [![Latest Release](https://img.shields.io/github/v/release/paulschiretz/pgl-backup)](https://github.com/paulschiretz/pgl-backup/releases)

> This project was born from the desire to create a hassle-free backup tool that is fast, reliable, and as simple as possible. It is built in a robust, best-practice way, avoiding fancy features that could compromise solidity. The goal is to provide everything you need for solid, performant, cross-platform backups—no more, no less.

`pgl-backup` is a simple, powerful, and robust file backup utility written in Go. It is designed for creating versioned backups of local directories to another local or network-attached drive. It supports periodic snapshots, an efficient incremental mode with a flexible retention policy, and automatic compression.

## Table of Contents

*   [Key Features](#key-features)
*   [Installation](#installation)
    *   [From Source](#from-source)
    *   [From Pre-compiled Binaries](#from-pre-compiled-binaries)
*   [Security and Binary Verification](#security-and-binary-verification)
    *   [Running on macOS (Gatekeeper)](#running-on-macos-gatekeeper)
    *   [Running on Windows (SmartScreen)](#running-on-windows-smartscreen)
    *   [Verifying Checksums](#verifying-checksums)
*   [Getting Started: A 3-Step Guide](#getting-started-a-3-step-guide)
    *   [Step 1: Initialize Configuration](#step-1-initialize-configuration)
    *   [Step 2: Review Configuration (Optional)](#step-2-review-configuration-optional)
    *   [Step 3: Run Your First Backup](#step-3-run-your-first-backup)
*   [Usage and Examples](#usage-and-examples)
    *   [Basic Backup](#basic-backup)
    *   [Dry Run](#dry-run)
    *   [One-Off Snapshot Backup](#one-off-snapshot-backup)
    *   [Excluding Files and Directories](#excluding-files-and-directories)
    *   [Using Pre- and Post-Backup Hooks](#using-pre--and-post-backup-hooks)
*   [Cross-Platform Backups and Case-Sensitivity](#cross-platform-backups-and-case-sensitivity)
*   [Exclusion Rules](#exclusion-rules)
*   [Log Levels](#log-levels)
*   [Retention Policy](#retention-policy)
    *   [Retention Policy Examples](#retention-policy-examples)
*   [Configuration Details](#configuration-details)
*   [Troubleshooting](#troubleshooting)
*   [Contributing](#contributing)
*   [Acknowledgments](#acknowledgments)
*   [License](#license)

## Key Features

*   **Backup Modes**:
    *   **Incremental (default)**: Maintains a single "current" backup directory that is efficiently updated. At a configurable interval (e.g., daily), the "current" backup is rolled over into a timestamped archive. This is ideal for frequent, low-overhead backups.
    *   **Snapshot**: Each backup run creates a new, unique, timestamped directory. This is useful for creating distinct, point-in-time copies.
*   **Flexible Retention Policy**: Automatically cleans up outdated backups by keeping a configurable number of hourly, daily, weekly, monthly, and yearly archives. This gives you a fine-grained history without filling up your disk.
*   **Intelligent Archiving (Incremental Mode)**: The archive interval can be set to `auto` (the default) to automatically align with your retention policy. If you keep hourly backups, it will archive hourly. This prevents configuration mismatches and ensures your retention slots are filled efficiently.
*   **Automatic and Resilient Compression**: To save disk space, you can enable automatic compression of backups into `.tar.zst`, `.zip` or `.tar.gz` archives. If compression fails (e.g., due to a corrupt file), the original backup is left untouched.
*   **Concurrency Safe**: A robust file-locking mechanism prevents multiple backup instances from running against the same target directory simultaneously, protecting data integrity.
*   **Symbolic Link Support**: Preserves symbolic links in the destination. On Windows, creating symlinks requires the user to have the appropriate privileges (Run as Administrator) or Developer Mode enabled.
*   **Pre- and Post-Backup Hooks**: Execute custom shell commands before the sync begins or after it completes, perfect for tasks like dumping a database or sending a notification.
*   **Adjustable Configuration**: Configure backups using a simple `pgl-backup.config.json` JSON file, and override any setting with command-line flags for one-off tasks.
*   **Multiple Sync Engines**:
    *   **`native`**: The default engine. It is a high-performance, concurrent, cross-platform engine written in pure Go that generally offers the best performance with no external dependencies.
    *   **`robocopy`** (Windows only): An alternative engine that uses the battle-tested `robocopy.exe` utility. While reliable, the `native` engine is often faster due to its modern design.
*   **Safety First**:
    *   **Pre-flight Checks**: Validates source and target paths, permissions, and configuration *before* starting any file operations to fail fast and provide clear errors.
    *   **Dry Run Mode**: The `-dry-run` flag lets you see exactly what files would be copied, updated, or deleted without making any actual changes.
    *   **Permission Lockout Protection**: When copying files and directories, `pgl-backup` automatically ensures the backup user has write permissions on the destination, even if the source is read-only. This prevents the backup process from locking itself out on subsequent runs.
    *   **Consistent User Execution**: While `pgl-backup` includes features to prevent permission lockouts, it is still a best practice to run all backups for a specific target as the same user. This ensures consistent file ownership and avoids potential permission issues on Unix-like systems.
    *   **"Ghost Directory" Protection**: On Unix-like systems, it helps prevent accidentally backing up to a mount point when the external drive is not actually mounted.
    *   **Cross-Platform Safety**: Detects and halts on risky backup scenarios, such as backing up a case-sensitive Linux source from a case-insensitive Windows host, which can lead to silent data loss.

## Installation

### From Source

Ensure you have [Go](https://go.dev/dl/) installed (version 1.25+ recommended).
If you have a Go environment set up, you can install `pgl-backup` directly:

```sh
go install github.com/paulschiretz/pgl-backup/cmd/pgl-backup@latest
```

### From Pre-compiled Binaries

You can download the latest pre-compiled binaries for your operating system from the Releases page.

## Security and Binary Verification

As an open-source project, the pre-compiled binaries provided in the Releases section are currently **unsigned**. This means that operating systems like macOS and Windows may flag them as "unrecognized" or "untrusted" when you try to run them for the first time.

This is expected behavior for unsigned software. You can verify the integrity of the downloaded files using the provided `checksums.txt` file, or build the project from source if you prefer.

**Note:** At the current stage, binaries are unsigned to avoid the high costs of code signing certificates for a free open-source tool. If this is a significant issue for you, please open an issue on GitHub. I am willing to sign binaries in the future if there is sufficient demand to justify the cost.

### Running on macOS (Gatekeeper)

When you first run `pgl-backup` on macOS, you may see a popup saying it "cannot be opened because the developer cannot be verified."

1.  Locate the `pgl-backup` binary in Finder.
2.  **Right-click** (or Control-click) the file and select **Open**.
3.  Click **Open** in the dialog box that appears.
4.  You only need to do this once.

Alternatively, you can remove the quarantine attribute via the terminal:
```sh
xattr -d com.apple.quarantine pgl-backup
```

### Running on Windows (SmartScreen)

Windows Defender SmartScreen may prevent the application from starting.

1.  Click **More info** in the popup window.
2.  Click the **Run anyway** button.

### Verifying Checksums

Every release includes a `checksums.txt` file containing the SHA256 hashes of the artifacts. You can verify that your download has not been tampered with or corrupted.

**Linux / macOS:**
```sh
# Download the checksums.txt and the archive to the same directory
shasum -a 256 -c checksums.txt

# Or manually check a specific file:
shasum -a 256 pgl-backup_v1.0.0_darwin_arm64.tar.gz
# Compare output with the content of checksums.txt
```

**Windows (PowerShell):**
```powershell
Get-FileHash .\pgl-backup_v1.0.0_windows_amd64.zip -Algorithm SHA256
# Compare the hash with the content of checksums.txt
```

## Getting Started: A 3-Step Guide

Let's set up a daily incremental backup for your `~/Documents` folder to an external drive.

### Step 1: Initialize Configuration

The easiest way to get started is to use the `-init` flag. This will generate a `pgl-backup.config.json` file in your target directory. The `-source` and `-target` flags are required for this step.

```sh
# Example for Linux/macOS
pgl-backup -source="$HOME/Documents" -target="/media/backup-drive/MyDocumentsBackup" -init

# Example for Windows
pgl-backup -source="C:\Users\YourUser\Documents" -target="E:\Backups\MyDocumentsBackup" -init
```

This command will:
1.  Perform pre-flight checks to ensure the paths are valid.
2.  Create the target directory `/media/backup-drive/MyDocumentsBackup`.
3.  Generate a `pgl-backup.config.json` file inside it.

### Step 2: Review Configuration (Optional)

Open the newly created `pgl-backup.config.json` file. It will look something like this, filled with sensible defaults. You can adjust the retention policy, archive policy, or add exclusions.

```json
{
  "mode": "incremental",
  "engine": {
    "type": "native",
    "retryCount": 3,
    "retryWaitSeconds": 5,
    "modTimeWindowSeconds": 1,
    "performance": {
      "mirrorWorkers": 8,
      "bufferSizeKB": 256,
      "syncWorkers": 8,
      "compressWorkers": 4,
      "deleteWorkers": 4
    }
  },
  "logLevel": "info",
  "dryRun": false,
  "metrics": true,
  "compression": {
    "enabled": true,
    "format": "tar.zst"
  },
  "naming": {
    "prefix": "PGL_Backup_"
  },
  "paths": {
    "source": "/home/user/Documents",
    "targetBase": "/media/backup-drive/MyDocumentsBackup",
    "snapshotsSubDir": "PGL_Backup_Snapshots",
    "archivesSubDir": "PGL_Backup_Archives",
    "incrementalSubDir": "PGL_Backup_Current",
    "preserveSourceDirectoryName": true,
    "defaultExcludeFiles": [
      "*.tmp",
      "*.temp",
      "*.swp",
      "*.lnk",
      "~*",
      "desktop.ini",
      ".DS_Store",
      "Thumbs.db",
      "Icon\r"
    ],
    "defaultExcludeDirs": [
      "@tmp",
      "@eadir",
      ".SynologyWorkingDirectory",
      "#recycle",
      "$Recycle.Bin"
    ],
    "userExcludeFiles": [],
    "userExcludeDirs": []
  },
  "retention": {
    "incremental": {
      "enabled": true,
      "hours": 0,
      "days": 7,
      "weeks": 4,
      "months": 3,
      "years": 1
    },
    "snapshot": {
      "enabled": false,
      "hours": 0,
      "days": 0,
      "weeks": 0,
      "months": 0,
      "years": 0
    }
  },
  "archive": {
    "incremental": {
      "mode": "auto",
      "interval": "24h0m0s"
    }
  },
  "hooks": {}
}
```

### Step 3: Run Your First Backup

Now, simply point `pgl-backup` at the target directory. It will automatically load the configuration file and run the backup. 

```sh
pgl-backup -target="/media/backup-drive/MyDocumentsBackup"
```

The first run will copy all files into a `PGL_Backup_Content` subdirectory inside the main `PGL_Backup_Current` directory.
Subsequent runs will efficiently update the contents of `PGL_Backup_Current/PGL_Backup_Content`. After 24 hours (or your configured interval), the next run will first rename the entire PGL_Backup_Current directory to a timestamped archive (e.g., `PGL_Backup_2023-10-27-...`) inside the `PGL_Backup_Archives` sub-directory, and then create a new, clean `PGL_Backup_Current` for the next sync.
When compression is enabled, the `PGL_Backup_Content` subdirectory within an archive is compressed, and the original subdirectory is removed, leaving the compressed file alongside the backup's metadata.

Your backup target will be organized like this:
```
/path/to/your/backup-target/
├── pgl-backup.config.json (The configuration for this backup set)
├── PGL_Backup_Current/ (The active incremental backup)
├── PGL_Backup_Archives/ (Timestamped historical incremental archives)
└── PGL_Backup_Snapshots/ (Timestamped snapshots from snapshot mode)
```

## Usage and Examples

### Basic Backup

Once configured, this is the only command you need. It's perfect for a cron job or scheduled task.

```sh
pgl-backup -target="/path/to/your/backup-target"
```

### Dry Run

See what changes would be made without touching any files.

```sh
pgl-backup -target="/path/to/your/backup-target" -dry-run
```

### One-Off Snapshot Backup

You can override any configuration setting with a command-line flag. Here's how to perform a single snapshot backup, ignoring the `incremental` mode set in the config file.

```sh
pgl-backup -target="/path/to/your/backup-target" -mode=snapshot
```

### Excluding Files and Directories

Exclude temporary files and `node_modules` directories. Patterns support standard file globbing.

```sh
pgl-backup -target="..." -user-exclude-files="*.tmp,*.log" -user-exclude-dirs="node_modules,.cache"
```
> **Note on Matching**: All exclusion patterns are case-insensitive on all operating systems. A pattern like *.jpeg will match photo.jpeg, photo.JPEG, and photo.JpEg. This ensures your configuration is portable and behaves predictably across Windows, macOS, and Linux.

### Using Pre- and Post-Backup Hooks

Run a script before the backup starts. Commands with spaces must be wrapped in single or double quotes.

```sh
pgl-backup -target="..." -pre-backup-hooks="'/usr/local/bin/dump_database.sh', 'echo Backup starting...'"
```
>**Security Note:** Hooks execute arbitrary shell commands. Ensure that any commands in your configuration are from a trusted source and have the correct permissions to prevent unintended side effects.

## Cross-Platform Backups and Case-Sensitivity

`pgl-backup` includes a critical safety check to prevent data loss from filesystem case-sensitivity mismatches.

*   **The Problem**: Backing up a case-sensitive source (like a Linux filesystem with both `File.txt` and `file.txt`) from a case-insensitive host (like Windows) is dangerous. The host OS may only "see" one of the files, leading to silent data loss as the other is never backed up.
*   **The Solution**: `pgl-backup` automatically tests the case-sensitivity of both the source and the host environment. If it detects this risky combination, it will stop with a clear error message before the backup begins.
*   **Recommendation**: For maximum data integrity when backing up a case-sensitive source (like a Linux server or a WSL environment), you should always **run `pgl-backup` on a case-sensitive operating system** (like Linux, or from within WSL on Windows).

## Exclusion Rules

`pgl-backup` provides a flexible exclusion system to keep your backups clean and efficient.

### Case-Insensitive Matching

All exclusion patterns are case-insensitive on all operating systems. A pattern like *.jpeg will match photo.jpeg, photo.JPEG, and photo.JpEg. This ensures your configuration is portable and behaves predictably across Windows, macOS, and Linux.

### Default and System Exclusions

To provide a better out-of-the-box experience and protect its own metadata, `pgl-backup` uses two layers of exclusions.

#### 1. System Exclusions (Always Active)
A small, non-configurable list of files are always excluded to prevent the backup tool from backing up its own operational files.
These are:
*   `pgl-backup.config.json`
*   `.pgl-backup.meta.json`
*   `.~pgl-backup.lock`

#### 2. Predefined Exclusions (Sensible Defaults)
A list of common temporary and system files are excluded by default. When you generate a new configuration file with `-init`, these defaults are included, and you can customize them in your `pgl-backup.config.json` file.
The JSON keys for these are `defaultExcludeFiles` and `defaultExcludeDirs`.

*   **Default Excluded Files:** `*.tmp`, `*.temp`, `*.swp`, `*.lnk`, `~*`, `desktop.ini`, `.DS_Store`, `Thumbs.db`, `Icon\r`.
*   **Default Excluded Directories:** `@tmp`, `@eadir`, `.SynologyWorkingDirectory`, `#recycle`, `$Recycle.Bin`.

### User defined Exclusions (Customizable)
You can define your own list of files and directories to exclude using the `userExcludeFiles` and `userExcludeDirs` keys in the configuration file, or via the command-line flags `-user-exclude-files` and `-user-exclude-dirs`. These are combined with the system and default exclusions.

## Log Levels

`pgl-backup` uses a structured logging system with several levels of verbosity, allowing you to control how much detail you see. You can set the level using the `-log-level` flag or the `logLevel` key in your configuration file.

*   **`error`**: Only shows critical errors that cause the backup process to halt. Use this if you only want to be alerted to complete failures.
*   **`warn`**: Shows errors and warnings. Warnings are non-critical issues that the application has recovered from but that you should be aware of (e.g., a single file failed to copy, a configuration mismatch).
*   **`info` (Default)**: Provides a high-level summary of the backup process. It shows the start and end of major phases like synchronization, compression, and retention cleanup. This is the recommended level for daily use and cron jobs, as it provides a clean, readable overview.
*   **`notice`**: Shows everything from `info` plus detailed, per-item operational messages. Use this level to see a log of every file being copied, deleted, archived, or compressed. It's useful for verifying that specific files are being handled correctly without the full verbosity of `debug`.
*   **`debug`**: The most verbose level. Includes everything from `notice` plus detailed developer-oriented information for tracing execution flow and diagnosing complex issues.

## Retention Policy

The retention policy is designed to give you a detailed short-term history and a space-efficient long-term history. It works using a "promotion" system. When cleaning up old backups, `pgl-backup` scans all your archives from newest to oldest and decides which ones to keep.

Here's how it works for each backup, in order of priority:
1.  **Hourly**: Is there an open "hourly" slot? If yes, keep this backup and move to the next one.
2.  **Daily**: If not kept as hourly, is there an open "daily" slot for this backup's calendar day? If yes, keep it and move on.
3.  **Weekly**: If not kept as daily, is there an open "weekly" slot for this backup's calendar week? If yes, keep it and move on.
4.  **Monthly**: If not kept as weekly, is there an open "monthly" slot? If yes, keep it and move on.
5.  **Yearly**: If not kept as monthly, is there an open "yearly" slot? If yes, keep it.

If a backup doesn't fill any available slot, it is deleted. This ensures that a backup is only "promoted" to a longer-term slot (like weekly) if it's no longer needed to fill a shorter-term slot (like daily).

### Retention Policy Examples

Here are a few example policies you can use in your `pgl-backup.config.json` file.
The best policy depends on how much data you are backing up and how much disk space you have available. For many use cases, a simple policy of keeping 2 daily, 1 weekly, and perhaps 1 monthly backup is more than enough.


#### The "Simple" (Minimal)
**Goal**: A minimal policy for users who need a basic safety net without using much disk space. It provides a few recent recovery points.
**Total Backups Stored**: ~4 (2 daily + 1 weekly + 1 monthly)
**Auto Archive Interval Sets To**: 24 Hours

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 0,
    "days": 2,
    "weeks": 1,
    "months": 1,
    "years": 0
  }
}
```


#### The "Default" (Balanced)
**Goal**: A balanced, "set-it-and-forget-it" policy that provides a good mix of recent and long-term history without using excessive disk space. This is the default policy when you first initialize `pgl-backup`.
**Total Backups Stored**: ~15 (7 daily + 4 weekly + 3 monthly + 1 yearly)
**Auto Archive Interval Sets To**: 24 Hours

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 0,
    "days": 7,
    "weeks": 4,
    "months": 3,
    "years": 1
  }
}
```

#### The "Safety Net" (Developer Machine)
**Goal**: Protect active work where recent recovery is critical. The priority is being able to undo a mistake made an hour ago. Long-term history is less important.
**Total Backups Stored**: ~31 (24 hourly + 7 daily)
**Auto Archive Interval Sets To**: 1 Hour

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 24,
    "days": 7,
    "weeks": 0,
    "months": 0,
    "years": 0
  }
}
```
#### The "Standard GFS" (Balanced)
**Goal**: The industry standard for production servers. It balances a reasonable safety net (1 week of daily backups) with a solid historical archive (1 year) without using excessive storage.
**Total Backups Stored**: ~23 (7 daily + 4 weekly + 12 monthly)
**Auto Archive Interval Sets To**: 24 Hours

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 0,
    "days": 7,
    "weeks": 4,
    "months": 12,
    "years": 0
  }
}
```

#### The "Legal Compliance" (Long-Term Archive)
**Goal**: Audit trails and regulatory compliance (e.g., tax or financial data). Recent recovery is less important than proving what data existed 5 years ago.
**Total Backups Stored**: ~49 (30 daily + 12 monthly + 7 yearly)
**Auto Archive Interval Sets To**: 24 Hours

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 0,
    "days": 30,
    "weeks": 0,
    "months": 12,
    "years": 7
  }
}
```

#### The "Home Media" (Minimalist)
**Goal**: Backing up terabytes of movies/photos where data rarely changes and storage costs are high. You just want a few recent versions in case of accidental deletion.
**Total Backups Stored**: ~7 (2 daily + 2 weekly + 2 monthly + 1 yearly)
**Auto Archive Interval Sets To**: 24 Hours

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 0,
    "days": 2,
    "weeks": 2,
    "months": 2,
    "years": 1
  }
}
```

## Configuration Details

All command-line flags can be set in the `pgl-backup.config.json` file.

| Flag / JSON Key                 | Type          | Default                               | Description                                                                                             |
| ------------------------------- | ------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `source` / `paths.source`       | `string`      | `""`                                  | The directory to back up. **Required**. |
| `target` / `paths.targetBase`   | `string`      | `""`                                  | The base directory where backups are stored. **Required**. |
| `mode` / `mode`                 | `string`      | `"incremental"`                       | Backup mode: `"incremental"` or `"snapshot"`. |
| `paths.snapshotsSubDir`          | `string`      | `"PGL_Backup_Snapshots"`               | The name of the sub-directory where snapshots are stored (snapshot mode only). |
| `paths.archivesSubDir`          | `string`      | `"PGL_Backup_Archives"`               | The name of the sub-directory within the target where historical archives are stored (incremental mode only). |
| `paths.incrementalSubDir`       | `string`      | `"PGL_Backup_Current"`                | The name of the directory for the active incremental backup. |
| `paths.contentSubDir`           | `string`      | `"PGL_Backup_Content"`                | The name of the sub-directory within a backup that holds the actual synced content. |
| `init`                          | `bool`        | `false`                               | If true, generates a config file and exits. |
| `dry-run` / `dryRun`            | `bool`        | `false`                               | If true, simulates the backup without making changes. |
| `log-level` / `logLevel`        | `string`      | `"info"`                              | Set the logging level: `"debug"`, `"notice"`, `"info"`, `"warn"`, or `"error"`. |
| `metrics` / `metrics`           | `bool`        | `true`                                | If true, enables detailed performance and file-counting metrics. |
| `sync-engine` / `engine.type`   | `string`      | `"native"`                            | The sync engine to use: `"native"` or `"robocopy"` (Windows only). |
| `archive.incremental.mode` | `string` | `"auto"` | Archive interval mode: `"auto"` (derives interval from retention policy) or `"manual"`. In `auto` mode, if the retention policy is disabled, archiving is also disabled. |
| `archive.incremental.interval` | `duration` | `"24h"` | In `manual` mode, the interval after which a new archive is created (e.g., "24h", "168h"). Use "0" to disable archiving. |
| `retention.incremental.enabled`         | `bool`         | `true`                             | Enables the retention policy for incremental mode archives. |
| `retention.incremental.hours`         | `int`         | `0`                                   | Number of recent hourly incremental archives to keep. |
| `retention.incremental.days`          | `int`         | `7`                                   | Number of recent daily incremental archives to keep. |
| `retention.incremental.weeks`         | `int`         | `4`                                   | Number of recent weekly incremental archives to keep. |
| `retention.incremental.months`        | `int`         | `3`                                   | Number of recent monthly incremental archives to keep. |
| `retention.incremental.years`         | `int`         | `1`                                   | Number of recent yearly incremental archives to keep. |
| `retention.snapshot.enabled`         | `bool`         | `false`                               | Set to true to enable the retention policy for snapshot mode backups. Disabled by default. |
| `retention.snapshot.hours`         | `int`         | `0`                                      | Number of recent hourly snapshots to keep. |
| `retention.snapshot.days`          | `int`         | `0`                                      | Number of recent daily snapshots to keep. |
| `retention.snapshot.weeks`         | `int`         | `0`                                      | Number of recent weekly snapshots to keep. |
| `retention.snapshot.months`        | `int`         | `0`                                      | Number of recent monthly snapshots to keep. |
| `retention.snapshot.years`         | `int`         | `0`                                      | Number of recent yearly snapshots to keep. |
| `compression` / `compression.enabled` | `bool` | `false` | If true, enables compression for any backups (incremental archives or snapshots) that are not already compressed. |
| `compression-format` / `compression.format` | `string` | `"tar.zst"` | The archive format to use: `"zip"`, `"tar.gz"` or `"tar.zst"`. |
| `defaultExcludeFiles`           | `[]string`    | `[*.tmp, *.temp, *.swp, *.lnk, ~*, desktop.ini, .DS_Store, Thumbs.db, Icon\r]`                     | The list of default file patterns to exclude. Can be customized. |
| `defaultExcludeDirs`            | `[]string`    | `[@tmp, @eadir, .SynologyWorkingDirectory, #recycle, $Recycle.Bin]`                     | The list of default directory patterns to exclude. Can be customized. |
| `user-exclude-files` / `userExcludeFiles`| `[]string`    | `[]`                                  | List of file patterns to exclude. |
| `user-exclude-dirs` / `userExcludeDirs`  | `[]string`    | `[]`                                  | List of directory patterns to exclude. |
| `pre-backup-hooks` / `preBackup`| `[]string`    | `[]`                                  | List of shell commands to run before the backup. |
| `post-backup-hooks` / `postBackup`| `[]string`    | `[]`                                  | List of shell commands to run after the backup. |
| `preserve-source-name` / `paths.preserveSourceDirectoryName` | `bool` | `true` | If true, appends the source directory's name to the destination path. |
| **Performance Tuning** | | | | 
| `sync-workers` / `engine.performance.syncWorkers` | `int` | `runtime.NumCPU()` | Number of concurrent workers for file synchronization. |
| `mirror-workers` / `engine.performance.mirrorWorkers` | `int` | `runtime.NumCPU()` | Number of concurrent workers for file deletions in mirror mode. |
| `delete-workers` / `engine.performance.deleteWorkers` | `int` | `4` | Number of concurrent workers for deleting outdated backups. |
| `compress-workers` / `engine.performance.compressWorkers` | `int` | `4` | Number of concurrent workers for compressing backups. |
| `retry-count` / `engine.retryCount` | `int` | `3` | Number of retries for failed file copies. |
| `retry-wait` / `engine.retryWaitSeconds` | `int` | `5` | Seconds to wait between retries. |
| `mod-time-window` / `engine.modTimeWindowSeconds` | `int` | `1` | Time window in seconds to consider file modification times equal (default 1s). |
| `buffer-size-kb` / `engine.performance.bufferSizeKB` | `int` | `256` | Size of the I/O buffer in kilobytes for file copies and compression. |

## Troubleshooting

### Error: "Unrecognized Developer" or "Untrusted" warning on startup

This occurs because the binaries are currently unsigned. Please refer to the Security and Binary Verification section for instructions on how to verify and run the application on macOS and Windows.

### Error: `permission denied` when reading source or writing to target

*   **Cause**: The user running `pgl-backup` does not have the necessary read permissions for the source directory or write permissions for the target directory.
*   **Solution**:
    *   Ensure you are running the command as a user with appropriate permissions.
    *   On Unix-like systems, use `ls -l` to check ownership and permissions. You may need to use `sudo` or adjust file permissions with `chmod` and `chown`.
    *   For network shares (SMB/NFS), verify that the share is mounted correctly and that the user has write access.

### Error: `lock is active, held by PID ...`

*   **Cause**: Another `pgl-backup` process is currently running against the same target directory, or a previous run crashed without cleanly releasing the lock. The application has stale-lock detection, but it may not cover all edge cases (e.g., system clock changes).
*   **Solution**:
    1.  First, verify that no other `pgl-backup` process is running. Use `ps aux | grep pgl-backup` (Linux/macOS) or check Task Manager (Windows).
    2.  If you are certain no other process is active, the lock file is stale. You can safely delete the `.~pgl-backup.lock` file from the root of your target backup directory and re-run the command.

### Pre/Post-Backup Hooks are failing

*   **Cause**: The command or script specified in the hook may not be executable, not in the system's `PATH`, or has incorrect quoting.
*   **Solution**:
    *   Ensure that any script you are calling is marked as executable (e.g., `chmod +x /path/to/your/script.sh`).
    *   Use absolute paths for your scripts (e.g., `/usr/local/bin/my_script.sh`) to avoid `PATH` issues, especially in automated environments like `cron`.
    *   If your command contains spaces or special characters, ensure it is correctly quoted within the JSON configuration or on the command line (e.g., `"'echo Backup starting...'"`, `"/path/to/script with spaces.sh"`).

### Backup performance is slow

*   **Cause**: The default number of concurrent workers may not be optimal for your specific hardware (e.g., slow spinning disks vs. fast SSDs, network latency).
*   **Solution**:
    *   Adjust the `sync-workers` and `buffer-size-kb` settings in your `pgl-backup.config.json` file.
    *   For systems with slow I/O (like a single spinning disk or a high-latency network share), *decreasing* the number of `sync-workers` (e.g., to `2` or `4`) can sometimes improve performance by reducing disk head thrashing.
    *   For systems with very fast I/O (like a local NVMe SSD), increasing `sync-workers` might yield better results.

### Error: `failed to create symlink (requires Admin or Developer Mode)`

*   **Cause**: The backup source contains symbolic links, but the user running `pgl-backup` on Windows does not have permission to create them in the destination.
*   **Solution**:
    *   Run the command prompt or PowerShell as **Administrator**.
    *   Enable **Developer Mode** in Windows settings, which allows non-admins to create symlinks.

### Error: `invalid character` or `cannot unmarshal` when loading configuration

*   **Cause**: The `pgl-backup.config.json` file contains invalid JSON syntax (e.g., a missing comma, a trailing comma after the last item in a list, or unescaped backslashes in paths).
*   **Solution**:
    *   Validate your JSON content using an online JSON validator.
    *   Ensure backslashes in Windows paths are escaped (e.g., `"C:\\Users\\Name"` instead of `"C:\Users\Name"`) or use forward slashes (`"C:/Users/Name"`), which work on all platforms.

### Error: `The process cannot access the file...` (Windows)

*   **Cause**: The file is currently open and locked by another application (e.g., an open Word document, Outlook .pst file, or running database). `pgl-backup` does not currently support Volume Shadow Copy Service (VSS) to back up locked files.
*   **Solution**:
    *   Close the application using the file before running the backup.
    *   Use the `pre-backup-hooks` feature to stop the relevant service/application before the backup and `post-backup-hooks` to restart it.

### Issue: Files are being re-copied every time, even if unchanged

*   **Cause**: This often happens when backing up to a file system with lower timestamp precision (like FAT32/exFAT) or a network share (SMB/CIFS) where the server time drifts from the client time.
*   **Solution**:
    *   Increase the `modTimeWindowSeconds` setting in your configuration (e.g., to `2` or `5` seconds). This tells `pgl-backup` to treat timestamps as identical if they are within that window.

## Contributing

Please see our [Contributing Guidelines](./CONTRIBUTING.md) for details on how to contribute to the project.

## Acknowledgments

*   Special thanks to [Klaus Post](https://github.com/klauspost) for his excellent compression libraries (`github.com/klauspost/compress`), which enable the high-performance compression features in `pgl-backup`.
*   The Go Authors for the Go programming language and standard library.

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for full license text.

This software includes components from third-party projects and the Go standard library. See the [NOTICE](./NOTICE) file for third-party attributions and full license texts.
