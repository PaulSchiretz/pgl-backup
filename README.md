# pgl-backup

[![Go Report Card](https://goreportcard.com/badge/pixelgardenlabs.io/pgl-backup)](https://goreportcard.com/report/pixelgardenlabs.io/pgl-backup) [![Latest Release](https://img.shields.io/github/v/release/pgl-backup/pgl-backup)](https://github.com/pgl-backup/pgl-backup/releases) [![License](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

> This project was born from the desire to create a hassle-free backup tool that is fast, reliable, and as simple as possible. It is built in a robust, best-practice way, avoiding fancy features that could compromise solidity. The goal is to provide everything you need for solid, performant, cross-platform backups—no more, no less.

`pgl-backup` is a simple, powerful, and robust file backup utility written in Go. It is designed for creating versioned backups of local directories to another local or network-attached drive. It supports both periodic snapshots and an efficient incremental mode with a flexible retention policy.

## Key Features

*   **Backup Modes**:
    *   **Incremental (default)**: Maintains a single "current" backup directory that is efficiently updated. At a configurable interval (e.g., daily), the "current" backup is rolled over into a timestamped archive. This is ideal for frequent, low-overhead backups.
    *   **Snapshot**: Each backup run creates a new, unique, timestamped directory. This is useful for creating distinct, point-in-time copies.
*   **Flexible Retention Policy**: Automatically cleans up outdated backups by keeping a configurable number of hourly, daily, weekly, monthly, and yearly archives. This gives you a fine-grained history without filling up your disk.
*   **Intelligent Archiving (Incremental Mode)**: The archive interval can be set to `auto` (the default) to automatically align with your retention policy. If you keep hourly backups, it will archive hourly. This prevents configuration mismatches and ensures your retention slots are filled efficiently.
*   **Automatic and Resilient Compression**: To save disk space, you can enable automatic compression of backups into `.zip` or `.tar.gz` archives. If compression fails (e.g., due to a corrupt file), the original backup is left untouched, and the system will retry up to a configurable `maxRetries` limit on subsequent runs before giving up.
*   **Concurrency Safe**: A robust file-locking mechanism prevents multiple backup instances from running against the same target directory simultaneously, protecting data integrity.
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

If you have a Go environment set up, you can install `pgl-backup` directly:

```sh
go install pixelgardenlabs.io/pgl-backup/cmd/pgl-backup@latest
```

### From Pre-compiled Binaries

You can download the latest pre-compiled binaries for your operating system from the Releases page.

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
      "copyBufferSizeKB": 256,
      "syncWorkers": 8,
      "compressWorkers": 4,
      "deleteWorkers": 4
    }
  },
  "logLevel": "info",
  "dryRun": false,
  "metrics": true,
  "compression": {
    "incremental": {
      "enabled": false,
      "format": "tar.gz",
      "maxRetries": 3
    },
    "snapshot": {
      "enabled": false,
      "format": "tar.gz",
      "maxRetries": 3
    }
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

### Using Pre- and Post-Backup Hooks

Run a script before the backup starts. Commands with spaces must be wrapped in single or double quotes.

```sh
pgl-backup -target="..." -pre-backup-hooks="'/usr/local/bin/dump_database.sh', 'echo Backup starting...'"
```
>**Security Note:** Hooks execute arbitrary shell commands. Ensure that any commands in your configuration are from a trusted source and have the correct permissions to prevent unintended side effects.

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
| `log-level` / `logLevel`        | `string`      | `"info"`                              | Set the logging level: `"debug"`, `"info"`, `"warn"`, or `"error"`. |
| `metrics` / `metrics`           | `bool`        | `true`                                | If true, enables detailed performance and file-counting metrics. |
| `sync-engine` / `engine.type`   | `string`      | `"native"`                            | The sync engine to use: `"native"` or `"robocopy"` (Windows only). |
| `archive.incremental.mode` | `string` | `"auto"` | Archive interval mode: `"auto"` (derives interval from retention policy) or `"manual"`. In `auto` mode, if the retention policy is disabled, archiving is also disabled. |
| `archive.incremental.interval` | `duration` | `"24h"` | In `manual` mode, the interval after which a new archive is created (e.g., "24h", "168h"). Use "0" to disable archiving. |
| `retention.incremental.enabled`         | `bool`         | `true`                             | Enables the retention policy for incremental mode archives. |
| `retention.incremental.hours`         | `int`         | `0`                                   | Number of recent hourly incremental archives to keep. |
| `retention.incremental.days`          | `int`         | `7`                                   | Number of recent daily incremental archives to keep. |
| `retention.incremental.weeks`         | `int`         | `4`                                   | Number of recent weekly incremental archives to keep. |
| `retention.incremental.months`        | `int`         | `3`                                   | Number of recent monthly incremental archives to keep. |
| `retention.incremental.years`         | `int`         | `1`                                   | Number of recent yearly incremental archives to keep.        
| `retention.snapshot.enabled`         | `bool`         | `false`                               | Set to true to enable the retention policy for snapshot mode backups. Disabled by default. |
| `retention.snapshot.hours`         | `int`         | `0`                                      | Number of recent hourly snapshots to keep. |
| `retention.snapshot.days`          | `int`         | `0`                                      | Number of recent daily snapshots to keep. |
| `retention.snapshot.weeks`         | `int`         | `0`                                      | Number of recent weekly snapshots to keep. |
| `retention.snapshot.months`        | `int`         | `0`                                      | Number of recent monthly snapshots to keep. |
| `retention.snapshot.years`         | `int`         | `0`                                      | Number of recent yearly snapshots to keep. |
| `incremental-compression` / `compression.incremental.enabled` | `bool` | `false` | If true, enables compression for any incremental backups that are not already compressed. |
| `incremental-compression-format` / `compression.incremental.format` | `string` | `"tar.gz"` | The archive format to use for incremental backups: `"zip"` or `"tar.gz"`. |
| `incremental-compression-max-retries` / `compression.incremental.maxRetries` | `int` | `3` | Maximum number of times to retry compressing a backup before giving up. |
| `snapshot-compression` / `compression.snapshot.enabled` | `bool` | `false` | If true, enables compression for any snapshots that are not already compressed. |
| `snapshot-compression-format` / `compression.snapshot.format` | `string` | `"tar.gz"` | The archive format to use for snapshots: `"zip"` or `"tar.gz"`. |
| `snapshot-compression-max-retries` / `compression.snapshot.maxRetries` | `int` | `3` | Maximum number of times to retry compressing a backup before giving up. |
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
| `copy-buffer-kb` / `engine.performance.copyBufferSizeKB` | `int` | `256` | Size of the I/O buffer in kilobytes for file copies. |

## Understanding the Retention Policy

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

## Troubleshooting

### A Note on Cross-Platform Backups and Case-Sensitivity

`pgl-backup` includes a critical safety check to prevent data loss from filesystem case-sensitivity mismatches.

*   **The Problem**: Backing up a case-sensitive source (like a Linux filesystem with both `File.txt` and `file.txt`) from a case-insensitive host (like Windows) is dangerous. The host OS may only "see" one of the files, leading to silent data loss as the other is never backed up.
*   **The Solution**: `pgl-backup` automatically tests the case-sensitivity of both the source and the host environment. If it detects this risky combination, it will stop with a clear error message before the backup begins.
*   **Recommendation**: For maximum data integrity when backing up a case-sensitive source (like a Linux server or a WSL environment), you should always **run `pgl-backup` on a case-sensitive operating system** (like Linux, or from within WSL on Windows).

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
    *   Adjust the `sync-workers` and `copy-buffer-kb` settings in your `pgl-backup.config.json` file.
    *   For systems with slow I/O (like a single spinning disk or a high-latency network share), *decreasing* the number of `sync-workers` (e.g., to `2` or `4`) can sometimes improve performance by reducing disk head thrashing.
    *   For systems with very fast I/O (like a local NVMe SSD), increasing `sync-workers` might yield better results.

## Contributing

Please see our [Contributing Guidelines](./CONTRIBUTING.md) for details on how to contribute to the project.

## License

This project is licensed under the MIT License - see the [LICENSE file](./LICENSE) for details.
