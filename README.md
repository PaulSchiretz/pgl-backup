# pgl-backup

`pgl-backup` is a simple, powerful, and robust file backup utility written in Go. It is designed for creating versioned backups of local directories to another local or network-attached drive. It supports both periodic snapshots and an efficient incremental mode with a sophisticated retention policy.

## Key Features

*   **Backup Modes**:
    *   **Incremental (default)**: Maintains a single "current" backup directory that is efficiently updated. At a configurable interval (e.g., daily), the "current" backup is rolled over into a timestamped archive. This is ideal for frequent, low-overhead backups.
    *   **Snapshot**: Each backup run creates a new, unique, timestamped directory. This is useful for creating distinct, point-in-time copies.
*   **Sophisticated Retention Policy**: Automatically cleans up old backups by keeping a configurable number of hourly, daily, weekly, and monthly archives. This gives you a fine-grained history without filling up your disk.
*   **Concurrency Safe**: A robust file-locking mechanism prevents multiple backup instances from running against the same target directory simultaneously, protecting data integrity.
*   **Pre- and Post-Backup Hooks**: Execute custom shell commands before the sync begins or after it completes, perfect for tasks like dumping a database or sending a notification.
*   **Flexible Configuration**: Configure backups using a simple `pgl-backup.conf` JSON file, and override any setting with command-line flags for one-off tasks.
*   **Multiple Sync Engines**:
    *   **`native`**: A high-performance, concurrent, cross-platform engine written in pure Go.
    *   **`robocopy`** (Windows only): Uses the battle-tested `robocopy.exe` utility for maximum performance on Windows.
*   **Safety First**:
    *   **Pre-flight Checks**: Validates source and target paths, permissions, and configuration *before* starting any file operations to fail fast and provide clear errors.
    *   **Dry Run Mode**: The `-dry-run` flag lets you see exactly what files would be copied, updated, or deleted without making any actual changes.
    *   **"Ghost Directory" Protection**: On Unix-like systems, it helps prevent accidentally backing up to a mount point when the external drive is not actually mounted.
    *   **Consistent User Execution**: For maximum reliability, especially when replicating permissions on Unix-like systems, it is recommended to always run `pgl-backup` as the same user for a given target directory. This prevents potential permission conflicts if different users write to the same backup set.

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

The easiest way to get started is to use the `-init` flag. This will generate a `pgl-backup.conf` file in your target directory. The `-source` and `-target` flags are required for this step.

```sh
# Example for Linux/macOS
pgl-backup -source="$HOME/Documents" -target="/media/backup-drive/MyDocumentsBackup" -init

# Example for Windows
pgl-backup -source="C:\Users\YourUser\Documents" -target="E:\Backups\MyDocumentsBackup" -init
```

This command will:
1.  Perform pre-flight checks to ensure the paths are valid.
2.  Create the target directory `/media/backup-drive/MyDocumentsBackup`.
3.  Generate a `pgl-backup.conf` file inside it.

### Step 2: Review Configuration (Optional)

Open the newly created `pgl-backup.conf` file. It will look something like this, filled with sensible defaults. You can adjust the retention policy, rollover interval, or add exclusions.

```json
{
  "mode": "incremental",
  "rolloverInterval": "24h0m0s",
  "engine": {
    "type": "native",
    "nativeEngineWorkers": 8,
    "nativeEngineRetryCount": 3,
    "nativeEngineRetryWaitSeconds": 5
  },
  "quiet": false,
  "dryRun": false,
  "naming": {
    "prefix": "PGL_Backup_",
    "incrementalModeSuffix": "Current"
  },
  "paths": {
    "source": "/home/user/Documents",
    "targetBase": "/media/backup-drive/MyDocumentsBackup",
    "preserveSourceDirectoryName": true,
    "excludeFiles": [],
    "excludeDirs": []
  },
  "retentionPolicy": {
    "hours": 24,
    "days": 7,
    "weeks": 4,
    "months": 12
  },
  "hooks": {}
}
```

### Step 3: Run Your First Backup

Now, simply point `pgl-backup` at the target directory. It will automatically load the configuration file and run the backup.

```sh
pgl-backup -target="/media/backup-drive/MyDocumentsBackup"
```

The first run will copy all files into a `.../PGL_Backup_current` directory. Subsequent runs will update this directory. After 24 hours, the next run will first rename `PGL_Backup_current` to a timestamped archive (e.g., `PGL_Backup_2023-10-27-...`) and then create a new `current` backup.

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
pgl-backup -target="..." -exclude-files="*.tmp,*.log" -exclude-dirs="node_modules,.cache"
```

### Using Hooks

Run a script before the backup starts. Commands with spaces must be wrapped in single or double quotes.

```sh
pgl-backup -target="..." -pre-backup-hooks="'/usr/local/bin/dump_database.sh', 'echo Backup starting...'"
```
>**Security Note:** Hooks execute arbitrary shell commands. Ensure that any commands in your configuration are from a trusted source and have the correct permissions to prevent unintended side effects.

## Configuration Details

All command-line flags can be set in the `pgl-backup.conf` file.

| Flag / JSON Key                 | Type          | Default                               | Description                                                                                             |
| ------------------------------- | ------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `source` / `paths.source`       | `string`      | `""`                                  | The directory to back up. **Required**.                                                                 |
| `target` / `paths.targetBase`   | `string`      | `""`                                  | The base directory where backups are stored. **Required**.                                              |
| `mode` / `mode`                 | `string`      | `"incremental"`                       | Backup mode: `"incremental"` or `"snapshot"`.                                                           |
| `init`                          | `bool`        | `false`                               | If true, generates a config file and exits.                                                             |
| `dry-run` / `dryRun`            | `bool`        | `false`                               | If true, simulates the backup without making changes.                                                   |
| `quiet` / `quiet`               | `bool`        | `false`                               | If true, suppresses logs for individual file operations.                                                |
| `sync-engine` / `engine.type`   | `string`      | `"native"`                            | The sync engine to use: `"native"` or `"robocopy"` (Windows only).                                      |
| `rolloverInterval`              | `duration`    | `"24h0m0s"`                           | In incremental mode, the interval after which a new archive is created (e.g., "24h", "168h").          |
| `retentionPolicy.hours`         | `int`         | `24`                                  | Number of recent hourly backups to keep.                                                                |
| `retentionPolicy.days`          | `int`         | `7`                                   | Number of recent daily backups to keep.                                                                 |
| `retentionPolicy.weeks`         | `int`         | `4`                                   | Number of recent weekly backups to keep.                                                                |
| `retentionPolicy.months`        | `int`         | `12`                                  | Number of recent monthly backups to keep.                                                               |
| `exclude-files` / `excludeFiles`| `[]string`    | `[]`                                  | List of file patterns to exclude.                                                                       |
| `exclude-dirs` / `excludeDirs`  | `[]string`    | `[]`                                  | List of directory patterns to exclude.                                                                  |
| `pre-backup-hooks` / `preBackup`| `[]string`    | `[]`                                  | List of shell commands to run before the backup.                                                        |
| `post-backup-hooks` / `postBackup`| `[]string`    | `[]`                                  | List of shell commands to run after the backup.                                                         |
| `preserve-source-name` / `preserveSourceDirectoryName` | `bool` | `true` | If true, appends the source directory's name to the destination path. |
| `native-engine-workers` / `nativeEngineWorkers` | `int` | `runtime.NumCPU()` | Number of concurrent workers for the native sync engine. |
| `native-retry-count` / `nativeEngineRetryCount` | `int` | `3` | Number of retries for failed file copies in the native engine. |
| `native-retry-wait` / `nativeEngineRetryWaitSeconds` | `int` | `5` | Seconds to wait between retries in the native engine. |
