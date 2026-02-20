# pgl-backup

[![Go Reference](https://pkg.go.dev/badge/github.com/paulschiretz/pgl-backup.svg)](https://pkg.go.dev/github.com/paulschiretz/pgl-backup)
[![Latest Release](https://img.shields.io/github/v/release/paulschiretz/pgl-backup)](https://github.com/paulschiretz/pgl-backup/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/paulschiretz/pgl-backup)](https://goreportcard.com/report/github.com/paulschiretz/pgl-backup)
[![Changelog](https://img.shields.io/badge/changelog-md-blue)](./CHANGELOG.md)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

> This project was born from a desire for a backup tool that is fast, reliable, and simple. It prioritizes robust, best-practice engineering over fancy features that could compromise solidity. The goal is to provide everything you need for solid, performant, cross-platform backups—no more, no less.

`pgl-backup` is a simple, powerful, and robust file backup utility written in Go. It is designed for creating versioned backups of local directories to another local or network-attached drive. It supports periodic snapshots, an efficient incremental mode with a flexible retention policy, and automatic compression. A core design goal is ensuring backups can be restored no matter what; by relying on standard file structures and open archive formats, your data remains fully accessible via native OS tools without needing `pgl-backup` installed.

## Table of Contents

*   [Core Concepts](#core-concepts)
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
    *   [Portable Usage (Plug-and-Play)](#portable-usage-plug-and-play)
    *   [Dry Run](#dry-run)
    *   [One-Off Snapshot Backup](#one-off-snapshot-backup)
    *   [Excluding Files and Directories](#excluding-files-and-directories)
    *   [Using Pre- and Post-Backup Hooks](#using-pre--and-post-backup-hooks)
*   [Automation](#automation)
*   [How to Restore a Backup](#how-to-restore-a-backup)
    *   [1. Locate Your Backup](#1-locate-your-backup)
    *   [2. Extract Files from Archives](#2-extract-files-from-archives)
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

## Core Concepts

*   **Reliability**: Pre-flight checks validate paths, permissions, and configuration *before* a single byte is moved.
*   **Transparency**: No proprietary containers. Backups are stored exactly as they appear in the source or within standard archives.
*   **Performance**: A high-performance, concurrent sync engine designed for modern hardware and high-latency network shares.
*   **Safety**: Built-in protection against "ghost" mount points, permission lockouts, and case-sensitivity mismatches.

## Key Features

*   **Backup Modes**:
    *   **Incremental (default)**: Maintains a single "current" backup directory that is efficiently updated. At a configurable interval (e.g., daily), the "current" backup is rolled over into a timestamped archive. This is ideal for frequent, low-overhead backups.
    *   **Snapshot**: Each backup run creates a new, unique, timestamped directory. This is useful for creating distinct, point-in-time copies.
*   **Zero Vendor Lock-in**: Your data is stored in standard directories and open archive formats (`.zip`, `.tar.gz`, `.tar.zst`). You can always access and restore your files using native operating system tools, even without `pgl-backup` installed.
*   **Flexible Retention Policy**: Automatically cleans up outdated backups by keeping a configurable number of hourly, daily, weekly, monthly, and yearly archives. This gives you a fine-grained history without filling up your disk.
*   **Intelligent Archiving (Incremental Mode)**: The archive interval can be set to `auto` (the default) to automatically align with your retention policy. If you keep hourly backups, it will archive hourly. This prevents configuration mismatches and ensures your retention slots are filled efficiently.
*   **Automatic and Resilient Compression**: To save disk space, you can enable automatic compression of backups into `.tar.zst`, `.zip` or `.tar.gz` archives. If compression fails (e.g., due to a corrupt file), the original backup is left untouched.
*   **Concurrency Safe**: A robust file-locking mechanism prevents multiple backup instances from running against the same target directory simultaneously, protecting data integrity.
*   **Symbolic Link Support**: Preserves symbolic links in the destination. On Windows, creating symlinks requires the user to have the appropriate privileges (Run as Administrator) or Developer Mode enabled.
*   **Plug and Play**: The binary is static and self-contained. You can run it directly from an external drive without installation, making it perfect for portable backup workflows.
*   **Pre- and Post-Backup Hooks**: Execute custom shell commands before the sync begins or after it completes, perfect for tasks like dumping a database or sending a notification.
*   **Adjustable Configuration**: Configure backups using a simple `pgl-backup.config.json` JSON file, and override any setting with command-line flags for one-off tasks.
*   **High performance Sync Engine**:
    *   **`native`**: A high-performance, concurrent, cross-platform engine written in pure Go that offers the best performance with no external dependencies.
*   **Safety First**:
    *   **Pre-flight Checks**: Validates source and target paths, permissions, and configuration *before* starting any file operations to fail fast and provide clear errors.
    *   **Dry Run Mode**: The `-dry-run` flag lets you see exactly what files would be copied, updated, or deleted without making any actual changes.
    *   **Permission Lockout Protection**: When copying files and directories, `pgl-backup` automatically ensures the backup user has write permissions on the destination, even if the source is read-only. This prevents the backup process from locking itself out on subsequent runs.
    *   **Consistent User Execution**: While `pgl-backup` includes features to prevent permission lockouts, it is still a best practice to run all backups for a specific target as the same user. This ensures consistent file ownership and avoids potential permission issues on Unix-like systems.
    *   **"Ghost Directory" Protection**: On Unix-like systems, it helps prevent accidentally backing up to a mount point when the external drive is not actually mounted.
    *   **Cross-Platform Safety**: Detects and halts on risky backup scenarios, such as backing up a case-sensitive Linux source from a case-insensitive Windows host, which can lead to silent data loss.
    *   **Path Nesting Protection**: Prevents infinite recursion loops by ensuring source and target directories are not nested within each other.

## Installation

### From Source

Ensure you have [Go](https://go.dev/dl/) installed (version 1.26+ recommended).
If you have a Go environment set up, you can install `pgl-backup` directly:

```sh
go install github.com/paulschiretz/pgl-backup@latest
```

### From Pre-compiled Binaries

You can download the latest pre-compiled binaries for your operating system from the Releases page.

## Security and Binary Verification

As an open-source project, the pre-compiled binaries provided in the Releases section are currently **unsigned**. This means that operating systems like macOS and Windows may flag them as "unrecognized" or "untrusted" when you try to run them for the first time.

This is expected behavior for unsigned software. You can verify the integrity of the downloaded files using the provided `checksums.txt` file, or build the project from source if you prefer.

> **Note:** At the current stage, binaries are unsigned to avoid the high costs of code signing certificates for a free open-source tool. If this is a significant issue for you, please open an issue on GitHub. I am willing to sign binaries in the future if there is sufficient demand to justify the cost.

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
shasum -a 256 pgl-backup_v1.4.1_darwin_arm64.tar.gz
# Compare output with the content of checksums.txt
```

**Windows (PowerShell):**
```powershell
Get-FileHash .\pgl-backup_v1.4.1_windows_amd64.zip -Algorithm SHA256
# Compare the hash with the content of checksums.txt
```

## Getting Started: A 3-Step Guide

Let's set up a daily incremental backup for your `~/Documents` folder to an external drive.

### Step 1: Initialize Configuration

The easiest way to get started is to use the `init` command. This will generate a `pgl-backup.config.json` file in your target directory.

*   **New Backups**: It creates the directory and a default configuration file. The `-base` and `-source` flags are required.
*   **Existing Backups**: It reads your existing configuration, applies any new flags you provide (like changing the log level), and saves the updated config. It preserves your retention policies and other settings. Note that `-source` is required to perform pre-flight checks.
*   **Fresh Start**: If you want to completely overwrite an existing configuration with defaults, use `init -default` instead.

```sh
# Example for Linux/macOS
pgl-backup init -base="/media/backup-drive/MyDocumentsBackup" -source="$HOME/Documents"

# Example for Windows
pgl-backup init -base="E:\Backups\MyDocumentsBackup" -source="C:\Users\YourUser\Documents"
```

You can also combine the init command with other flags to customize the configuration immediately:

```sh
# Example for Linux/macOS
pgl-backup init -base="/media/backup-drive/MyDocumentsBackup" -source="$HOME/Documents" -log-level=debug -user-exclude-files="*.mp4"

# Example for Windows
pgl-backup init -base="E:\Backups\MyDocumentsBackup" -source="C:\Users\YourUser\Documents" -log-level=debug -user-exclude-files="*.mp4"
```

This command will:
1.  Perform pre-flight checks to ensure the paths are valid.
2.  Create the target directory `/media/backup-drive/MyDocumentsBackup`.
3.  Generate a `pgl-backup.config.json` file inside it.

### Step 2: Review Configuration (Optional)

Open the newly created `pgl-backup.config.json` file. It will look something like this, filled with sensible defaults. You can adjust the retention policy, archive policy, or add exclusions.

```json
{
  "version": "1.4.1",
  "logLevel": "info",
  "paths": {
    "incremental": {
      "current": "PGL_Backup_Incremental_Current",
      "archive": "PGL_Backup_Incremental_Archive",
      "content": "PGL_Backup_Content",
      "archiveEntryPrefix": "PGL_Backup_"
    },
    "snapshot": {
      "current": "PGL_Backup_Snapshot_Current",
      "archive": "PGL_Backup_Snapshot_Archive",
      "content": "PGL_Backup_Content",
      "archiveEntryPrefix": "PGL_Backup_"
    }
  },
  "engine": {
    "metrics": true,
    "failFast": false,
    "performance": {
      "syncWorkers": 4,
      "mirrorWorkers": 4,
      "deleteWorkers": 4,
      "compressWorkers": 4,
      "bufferSizeKB": 1024,
      "readAheadLimitKB": 262144
    }
  },
  "sync": {
    "enabled": true,
    "preserveSourceDirName": true,
    "engine": "native",
    "safeCopy": true,
    "retryCount": 3,
    "retryWaitSeconds": 5,
    "modTimeWindowSeconds": 1,
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
  "archive": {
    "enabled": true,
    "intervalMode": "auto",
    "intervalSeconds": 86400
  },
  "retention": {
    "incremental": {
      "enabled": true,
      "hours": 0,
      "days": 0,
      "weeks": 4,
      "months": 0,
      "years": 0
    },
    "snapshot": {
      "enabled": false,
      "hours": -1,
      "days": -1,
      "weeks": -1,
      "months": -1,
      "years": -1
    }
  },
  "compression": {
    "enabled": true,
    "format": "tar.zst",
    "level": "default"
  },
  "hooks": {
    "preBackup": [],
    "postBackup": [],
    "preRestore": [],
    "postRestore": []
  }
}
```

### Step 3: Run Your First Backup

Now, simply point `pgl-backup` at the target directory and provide the source. It will automatically load the configuration file from the base directory and run the backup. The `-source` flag is mandatory.

```sh
# Run the backup
pgl-backup backup -base="/media/backup-drive/MyDocumentsBackup" -source="$HOME/Documents"
```

**Command Structure:**
*   `-base`**(Required)**: The root directory of your backup repository (where `pgl-backup.config.json` is located).
*   `-source`**(Required)**: The source directory where the files you want to backup are stored.

The first run will copy all files into a `PGL_Backup_Content` subdirectory inside the main `PGL_Backup_Incremental_Current` directory.
Subsequent runs will efficiently update the contents of `PGL_Backup_Incremental_Current/PGL_Backup_Content`. After 1 week (by default) or your configured interval, the next run will first rename the entire `PGL_Backup_Incremental_Current` directory to a timestamped archive (e.g., `PGL_Backup_2023-10-27-...`) inside the `PGL_Backup_Incremental_Archive` sub-directory, and then create a new, clean `PGL_Backup_Incremental_Current` for the next sync.
When compression is enabled, the `PGL_Backup_Content` subdirectory within an archive is compressed, and the original subdirectory is removed, leaving the compressed file alongside the backup's metadata.

Your backup target will be organized like this:
```
/path/to/your/backup-target/
├── pgl-backup.config.json (The configuration for this backup set)
├── PGL_Backup_Incremental_Current/ (The current incremental backup)
├── PGL_Backup_Incremental_Archive/ (Timestamped historical incremental backups)
└── PGL_Backup_Snapshot_Archive/ (Timestamped backups from snapshot mode)
```

## Usage and Examples

`pgl-backup` uses a subcommand structure: `pgl-backup <command> [flags]`. Running `pgl-backup` without any arguments will display the help message.

### Basic Backup

Once configured, this is the only command you need for your backup script, cron job or scheduled task.

```sh
pgl-backup backup -base="/path/to/your/backup-target" -source="/path/to/your/source-data"
```
### Portable Usage (Plug-and-Play)

You can store the `pgl-backup` binary directly on your external backup drive alongside your data. This allows you to plug the drive into any computer and run backups immediately without installing any software.

For example, you can place the binary and a simple script on your external drive:

**Directory Structure:** 

```
X:\ (External Drive)
├── pgl-backup.exe (Windows binary)
├── pgl-backup (Linux/macOS binary)
├── run_backup.bat
├── run_backup.sh
└── MyBackups\
```

**Windows (`run_backup.bat`):**

```bat
:: Set working directory to where the .bat file is
cd /d "%~dp0"
:: Run pgl-backup using a relative path for the base
pgl-backup.exe backup -base=".\MyBackups" -source="C:\Users\YourUser\Documents"
pause
```

**Linux / macOS (`run_backup.sh`):**

```sh
#!/bin/sh
cd "$(dirname "$0")"
./pgl-backup backup -base="./MyBackups" -source="$HOME/Documents"
```


### Dry Run

See what changes would be made without touching any files.

```sh
pgl-backup backup -base="/path/to/your/backup-target" -source="/path/to/your/source-data" -dry-run
```

### One-Off Snapshot Backup

Here's how to perform a single snapshot backup.

```sh
pgl-backup backup -base="/path/to/your/backup-target" -source="/path/to/your/source-data" -mode=snapshot
```

### Excluding Files and Directories

Exclude temporary files and `node_modules` directories. Patterns support standard file globbing.

```sh
pgl-backup backup -base="..." -source="..." -user-exclude-files="*.tmp,*.log" -user-exclude-dirs="node_modules,.cache"
```
> **Note on Matching**: All exclusion patterns are case-insensitive on all operating systems. A pattern like *.jpeg will match photo.jpeg, photo.JPEG, and photo.JpEg. This ensures your configuration is portable and behaves predictably across Windows, macOS, and Linux.

### Using Pre- and Post-Backup Hooks

Run a script before the backup starts. Commands with spaces must be wrapped in single or double quotes.

```sh
pgl-backup backup -base="..." -source="..." -pre-backup-hooks="'/usr/local/bin/dump_database.sh', 'echo Backup starting...'"
```
>**Security Note:** Hooks execute arbitrary shell commands. Ensure that any commands in your configuration are from a trusted source and have the correct permissions to prevent unintended side effects.

### Prune Backups

Manually apply retention policies to clean up outdated backups without running a full backup. This is useful for freeing up disk space immediately. Supports `-dry-run` to preview deletions.

```sh
pgl-backup prune -base="/path/to/your/backup-target"
```

**Command Structure:**
*   `-base`**(Required)**: The root directory of your backup repository (where `pgl-backup.config.json` is located).
*   `-mode`: Specify the backup mode (`incremental` or `snapshot`). If omitted, `pgl-backup` automatically prunes **incremental** and **snapshot** backups.


### List Backups

View a list of all backups stored in the repository, including their timestamps, modes, and UUIDs. This is useful for finding the specific name of a backup you want to restore.

```sh
pgl-backup list -base="/path/to/your/backup-target"
```

**Command Structure:**
*   `-base`**(Required)**: The root directory of your backup repository (where `pgl-backup.config.json` is located).
*   `-mode`: Specify the backup mode (`incremental` or `snapshot`). If omitted, `pgl-backup` automatically lists **incremental** and **snapshot** backups.
*   `-sort`: Sort order for the list (`desc` or `asc`). Defaults to `desc` (newest first).

## Automation

You can easily automate your backups using your operating system's built-in scheduler.

### Windows Task Scheduler

1.  Open **Task Scheduler** and click **Create Basic Task**.
2.  Name it "Daily Backup" and click **Next**.
3.  Choose your trigger (e.g., **Daily**) and set the time.
4.  Select **Start a program** as the action.
5.  **Program/script**: Browse to your `pgl-backup.exe`.
6.  **Add arguments**: Enter your backup command flags.
    ```
    backup -base="D:\Backups" -source="C:\Users\YourName\Documents"
    ```
7.  Finish the wizard.

### Linux / macOS (Cron)

Edit your crontab file (`crontab -e`) and add a line to run the backup daily (e.g., at 2:00 AM):

```cron
0 2 * * * /usr/local/bin/pgl-backup backup -base="/mnt/backup-drive" -source="/home/yourname/Documents" >> /var/log/pgl-backup.log 2>&1
```

### Note on External Drives

If your backup target is an external drive and it is not connected when the scheduled task runs, pgl-backup will detect this during its pre-flight checks and exit safely with an error. No data will be lost, and the backup will simply be skipped until the next scheduled run when the drive is available. 

## How to Restore a Backup

`pgl-backup` provides two ways to restore your data: using the built-in `restore` command for convenience, or by manually accessing your files with standard OS tools. The manual method guarantees you can always recover your data, even without `pgl-backup` installed.

### Using the `restore` Command (Recommended)

The `restore` command is the easiest and safest way to restore a backup. It automatically handles both compressed archives and uncompressed directories.

The `restore` command copies data from a specific backup to a target directory.

```sh
pgl-backup restore -base="/path/to/backup-repo" -target="/path/to/restore-location"
```

**Command Structure:**
*   `-base`**(Required)**: The root directory of your backup repository (where `pgl-backup.config.json` is located).
*   `-target`**(Required)**: The directory where you want to restore the files.
*   `-uuid`: The UUID of the backup you want to restore. If omitted (the default), `pgl-backup` launches an easy-to-use interactive menu where you can browse and select a backup from a list.
*   `-mode`: Filter the interactive list by backup mode (`incremental` or `snapshot`), or restrict the search when `-uuid` is provided. If omitted (`any`), `pgl-backup` lists all backups or searches both locations.
*   `-overwrite`: Control overwrite behavior (`always`, `never`, `if-newer`, `update`). If omitted, defaults to `never`, so existing files are not overwritten.
*   `-fail-fast`: Stop immediately on the first error. If omitted, defaults to `false`.

**Examples:**

**1. Interactive Restore (Default & Easiest):**
Simply omit the `-uuid` flag to see a list of all available backups and select one by number.
```sh
pgl-backup restore -base="/media/backup-drive/MyDocumentsBackup" \
                   -target="$HOME/RestoredDocuments"
```
The list displays the backup type:
* [INC]: Incremental backup.
* [SNP]: Snapshot backup

**2. Restore a specific backup by UUID:**
This finds the backup with the specified UUID, automatically decompresses it if needed, and restores its contents. You can find the UUID using the `list` command.
```sh
pgl-backup restore -base="/media/backup-drive/MyDocumentsBackup" \
                   -target="$HOME/RestoredFromSnapshot" \
                   -uuid="123e4567-e89b-12d3-a456-426614174000" \
```

**3. Restore the latest backup:**
Automatically selects and restores the most recent backup.
```sh
pgl-backup restore -base="/media/backup-drive/MyDocumentsBackup" \
                   -target="$HOME/RestoredFromSnapshot" \
                   -uuid="latest" \
```

**4. Restore a specific archived snapshot:**
This finds the backup with the specified UUID inside the snapshot archive directory, automatically decompresses it if needed, and restores its contents.
```sh
pgl-backup restore -base="/media/backup-drive/MyDocumentsBackup" \
                   -target="$HOME/RestoredFromSnapshot" \
                   -uuid="123e4567-e89b-12d3-a456-426614174000" \
                   -mode="snapshot"
```

**Overwrite Behavior:**
By default, the `restore` command will **never** overwrite existing files in the target directory (`-overwrite=never`). You can change this behavior:
*   `-overwrite=if-newer`: Only overwrite if the file in the backup is newer.
*   `-overwrite=always`: Always overwrite existing files.

### Manual Restoration (Guaranteed Access)

A core design philosophy of `pgl-backup` is zero vendor lock-in. Your data is always accessible.

#### 1. Locate Your Backup

Navigate to your backup target directory. You will see the following structure:

*   **`PGL_Backup_Incremental_Current/`**: Contains the most recent version of your files within the `PGL_Backup_Content` subdirectory. You can browse this directory directly and copy files out using your file explorer.
*   **`PGL_Backup_Incremental_Archive/`**: Contains compressed historical incremental backups (e.g., `PGL_Backup_2023-10-27-14-00-00...tar.zst`), if compression is disabled all your files are within the `PGL_Backup_Content` subdirectory.
*   **`PGL_Backup_Snapshot_Archive/`**: Contains point-in-time snapshots if you use snapshot mode. If compression is enabled (e.g., `PGL_Backup_2023-10-27-14-00-00...tar.zst`), if compression is disabled all your files are within the `PGL_Backup_Content` subdirectory.

#### 2. Extract Files from Archives

If you need to restore files from a compressed archive in `PGL_Backup_Incremental_Archive` or `PGL_Backup_Snapshot_Archive`, use the standard tools for your operating system.

> **Tip:** For a consistent graphical experience across platforms, or if you prefer not to use the command line, tools like 7-Zip (Windows) or PeaZip (Windows, Linux, macOS) can handle `.zip`, `.tar.gz`, and `.tar.zst` archives effortlessly.

##### Windows (`.zip`)
1.  Right-click the `.zip` file.
2.  Select **Extract All...**.
3.  Choose a destination and click **Extract**.

##### macOS / Linux (`.tar.gz`)
You can double-click the file to extract it, or use the terminal:

```sh
# Extract the entire archive
tar -xvf PGL_Backup_2023-10-27.tar.gz

# Extract a specific file
tar -xvf PGL_Backup_2023-10-27.tar.gz "path/to/file.txt"
```

##### macOS / Linux (`.tar.zst`)
The `.tar.zst` format uses Zstandard compression for high speed and ratio. You may need to install `zstd` (e.g., `brew install zstd` or `apt install zstd`).

```sh
# Extract the entire archive
tar --zstd -xvf PGL_Backup_2023-10-27.tar.zst

# Extract a specific file
tar --zstd -xvf PGL_Backup_2023-10-27.tar.zst "path/to/file.txt"
```

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

### User-Defined Exclusions (Customizable)
You can define your own list of files and directories to exclude using the `userExcludeFiles` and `userExcludeDirs` keys in the configuration file, or via the command-line flags `-user-exclude-files` and `-user-exclude-dirs`. These are combined with the system and default exclusions.

## Log Levels

`pgl-backup` uses a structured logging system with several levels of verbosity, allowing you to control how much detail you see. You can set the level using the `-log-level` flag or the `logLevel` key in your configuration file.

*   **`error`**: Only shows critical errors that cause the backup process to halt. Use this if you only want to be alerted to complete failures.
*   **`warn`**: Shows errors and warnings. Warnings are non-critical issues that the application has recovered from but that you should be aware of (e.g., a single file failed to copy, a configuration mismatch).
*   **`info` (Default)**: Provides a high-level summary of the backup process. It shows the start, periodic progress, and end of major phases like synchronization, compression, and retention cleanup. This is the recommended level for daily use and cron jobs, as it provides a clean, readable overview.
*   **`notice`**: Shows everything from `info` plus detailed, per-item operational messages. Use this level to see a log of every file being copied, deleted, archived, or compressed. It's useful for verifying that specific files are being handled correctly without the full verbosity of `debug`.
*   **`debug`**: The most verbose level. Includes everything from `notice` plus detailed developer-oriented information for tracing execution flow and diagnosing complex issues.

## Retention Policy

The retention policy is designed to give you a detailed short-term history and a space-efficient long-term history. `pgl-backup` supports separate retention policies the **Incremental** mode archive and **Snapshot** mode archive.

### Incremental vs. Snapshot Policies

*   **Incremental Retention**: Designed for your *routine* backup history. Since incremental backups run frequently (e.g., daily), this policy thins them out over time to save space. It is **enabled by default**.
*   **Snapshot Retention**: Designed for *manual* milestones (e.g., "Pre-Upgrade"). By default, this policy is **disabled**, meaning snapshots are kept forever. If you enable it, you must explicitly set retention periods to `0` or greater (defaults are `-1` for safety).

### How Retention Works

Both policies work using a "promotion" system. When cleaning up old backups, `pgl-backup` scans all your archived backups from newest to oldest and decides which ones to keep.

Here's how it works for each backup, in order of priority:
1.  **Hourly**: Is there an open "hourly" slot? If yes, keep this backup and move to the next one.
2.  **Daily**: If not kept as hourly, is there an open "daily" slot for this backup's calendar day? If yes, keep it and move on.
3.  **Weekly**: If not kept as daily, is there an open "weekly" slot for this backup's calendar week? If yes, keep it and move on.
4.  **Monthly**: If not kept as weekly, is there an open "monthly" slot? If yes, keep it and move on.
5.  **Yearly**: If not kept as monthly, is there an open "yearly" slot? If yes, keep it.

If a backup doesn't fill any available slot, it is deleted. This ensures that a backup is only "promoted" to a longer-term slot (like weekly) if it's no longer needed to fill a shorter-term slot (like daily).

### Retention Policy Examples

Here are a few example policies you can use in your `pgl-backup.config.json` file.
The best policy depends on how much data you are backing up and how much disk space you have available. For many use cases, a simple policy of keeping 4 weekly backups is more than enough.


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

#### The "Default" (Simple 4-Week History)
**Goal**: A simple, "set-it-and-forget-it" policy that provides a month of history without using excessive disk space. This is the default policy when you first initialize `pgl-backup`.
**Total Backups Stored**: ~4 (4 weekly)
**Auto Archive Interval Sets To**: 1 Week

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 0,
    "days": 0,
    "weeks": 4,
    "months": 0,
    "years": 0
  }
}
```

#### The "Purely Incremental" (Minimal History)
**Goal**: You mostly care about the current state (which is always available in `PGL_Backup_Incremental_Current`) but want one previous snapshot just in case.
**Total Backups Stored**: ~1 (1 monthly)
**Auto Archive Interval Sets To**: ~30 Days

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 0,
    "days": 0,
    "weeks": 0,
    "months": 1,
    "years": 0
  }
}
```

#### The "Monthly Only" (Low Frequency)
**Goal**: For data that changes slowly. You only create an archive once a month.
**Total Backups Stored**: ~12 (12 monthly)
**Auto Archive Interval Sets To**: 30 Days

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 0,
    "days": 0,
    "weeks": 0,
    "months": 12,
    "years": 0
  }
}
```

#### The "Safety Net" (Active Work)
**Goal**: Protect active work where recent recovery is critical. The priority is being able to undo a mistake made recently.
**Total Backups Stored**: ~6 (3 hourly + 3 daily)
**Auto Archive Interval Sets To**: 1 Hour

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 3,
    "days": 3,
    "weeks": 0,
    "months": 0,
    "years": 0
  }
}
```
#### The "Standard GFS" (Balanced)
**Goal**: A robust policy for important data. It balances a reasonable safety net (3 days) with a solid historical archive (3 months).
**Total Backups Stored**: ~10 (3 daily + 4 weekly + 3 monthly)
**Auto Archive Interval Sets To**: 24 Hours

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 0,
    "days": 3,
    "weeks": 4,
    "months": 3,
    "years": 0
  }
}
```

#### The "Long-Term" (Yearly Archive)
**Goal**: For data where you might need to go back many years, but fine-grained daily history isn't needed.
**Total Backups Stored**: ~11 (4 weekly + 7 yearly)
**Auto Archive Interval Sets To**: 1 Week

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 0,
    "days": 0,
    "weeks": 4,
    "months": 0,
    "years": 7
  }
}
```

#### The "Home Media" (Minimalist)
**Goal**: Backing up terabytes of movies/photos where data rarely changes and storage costs are high. You just want a few recent versions in case of accidental deletion.
**Total Backups Stored**: ~3 (1 weekly + 1 monthly + 1 yearly)
**Auto Archive Interval Sets To**: 1 Week

```json
"retention": {
  "incremental": {
    "enabled": true,
    "hours": 0,
    "days": 0,
    "weeks": 1,
    "months": 1,
    "years": 1
  }
}
```

## Configuration Details

### Commands

* `backup`: Run the backup operation. Use `-help` to see all the options.
* `restore`: Run the restore operation. Use `-help` to see all the options.
* `list`: List all backups in the base directory. Use `-help` to see all the options.
* `prune`: Apply retention policies to clean up outdated backups. Use `-help` to see all the options.
* `init`: Initialize or update a configuration. Use `-default` to overwrite an existing configuration with defaults.
* `version`: Print the application version.

### Flags

All command-line flags can also be set in the `pgl-backup.config.json` file. Note that structural options (like directory paths) are only available in the configuration file to ensure consistency.

| Flag / JSON Key | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `version`| `version` | `""` | The `pgl-backup` version. |
| `base` / `base` (internal) | `string` | `""` | The base directory where backups are stored. **Required**. |
| `source` (backup/init) / - | `string` | `""` | The directory to back up. **Required for Backup and Init**. |
| `target` (restore) / - | `string` | `""` | The destination directory for a restore operation. **Required for Restore**.  |
| `uuid` (restore) / - | `string` | `""` | The UUID of the backup you want to restore.  |
| `mode` / `runtime.mode` (internal) | `string` | `"incremental"` | Backup mode: `"incremental"` or `"snapshot"`. Defaults to `"any"` for list/prune/restore. |
| `sort` / `runtime.listSort` (internal) | `string` | `"desc"` | Sort order for list command: `"desc"` or `"asc"`. |
| `overwrite` (backup) / `runtime.backupOverwriteBehavior` (internal) | `string` | `"update"` | Overwrite behavior for backup: `'always'`, `'never'`, `'if-newer'`, `'update'`. |
| `overwrite` (restore) / `runtime.restoreOverwriteBehavior` (internal) | `string` | `"never"` | Overwrite behavior for restore: `'always'`, `'never'`, `'if-newer'`, `'update'`. |
| `fail-fast` / `engine.failFast` | `bool` | `false` | If true, stops the backup immediately on the first file sync error. |
| `ignore-case-mismatch` / `runtime.ignoreCaseMismatch` (internal) | `bool` | `false` | Bypass the case-sensitivity safety check (use with caution). |
| `default` / - | `false` | Used with `init` command. Overwrite existing configuration with defaults. |
| `force` / - | `false` | Bypass confirmation prompts (e.g., for init -default). |
| `dry-run` / `runtime.dryRun` (internal) | `false` | If true, simulates the backup without making changes. |
| `log-level` / `logLevel` | `string` | `"info"` | Set the logging level: `"debug"`, `"notice"`, `"info"`, `"warn"`, or `"error"`. |
| `metrics` / `engine.metrics` | `bool` | `true` | If true, enables detailed performance and file-counting metrics. |
| **Paths** | | | |
| - / `paths.incremental.archive` | `string` | `"PGL_Backup_Incremental_Archive"` | Sub-directory for historical incremental backups. |
| - / `paths.incremental.current` | `string` | `"PGL_Backup_Incremental_Current"` | Sub-directory for the current incremental backup. |
| - / `paths.incremental.content` | `string` | `"PGL_Backup_Content"` | Sub-directory for the content within an incremental backup. |
| - / `paths.incremental.archiveEntryPrefix` | `string` | `"PGL_Backup_"` | Prefix for timestamped archive directories (incremental). |
| - / `paths.snapshot.archive` | `string` | `"PGL_Backup_Snapshot_Archive"` | Sub-directory for snapshot backups. |
| - / `paths.snapshot.current` | `string` | `"PGL_Backup_Snapshot_Current"` | Sub-directory for the current snapshot backup. |
| - / `paths.snapshot.content` | `string` | `"PGL_Backup_Content"` | Sub-directory for the content within a snapshot backup. |
| - / `paths.snapshot.archiveEntryPrefix` | `string` | `"PGL_Backup_"` | Prefix for timestamped archive directories (snapshot). |
| **Sync Settings** | | | |
| - / `sync.enabled` | `bool` | `true` | Enable file synchronization. |
| `sync-engine` / `sync.engine` | `string` | `"native"` | The sync engine to use: `"native"`. |
| `sync-safe-copy` / `sync.safeCopy` | `bool` | `true` | Enable 'copy then rename' for secure file syncing (slower but safer). |
| `sync-retry-count` / `sync.retryCount` | `int` | `3` | Number of retries for failed file copies. |
| `sync-retry-wait` / `sync.retryWaitSeconds` | `int` | `5` | Seconds to wait between retries. |
| `sync-mod-time-window` / `sync.modTimeWindowSeconds` | `int` | `1` | Time window in seconds to consider file modification times equal. |
| `sync-preserve-source-dir-name` / `sync.PreserveSourceDirName` | `bool` | `true` | If true, creates a subdirectory in the destination named after the source directory. |
| **Exclusions & Hooks** | | | |
| `user-exclude-files` / `sync.userExcludeFiles` | `[]string` | `[]` | List of file patterns to exclude. |
| `user-exclude-dirs` / `sync.userExcludeDirs` | `[]string` | `[]` | List of directory patterns to exclude. |
| - / `sync.defaultExcludeFiles` | `[]string` | `[...]` | Default file patterns to exclude. |
| - / `sync.defaultExcludeDirs` | `[]string` | `[...]` | Default directory patterns to exclude. |
| `pre-backup-hooks` / `hooks.preBackup` | `[]string` | `[]` | List of shell commands to run before the backup. |
| `post-backup-hooks` / `hooks.postBackup` | `[]string` | `[]` | List of shell commands to run after the backup. |
| `pre-restore-hooks` / `hooks.preRestore` | `[]string` | `[]` | List of shell commands to run before the restore. |
| `post-restore-hooks` / `hooks.postRestore` | `[]string` | `[]` | List of shell commands to run after the restore. |
| **Archive & Retention** | | | |
| `archive` / `archive.enabled` | `bool` | `true` | Enable archiving (rollover) for incremental backups. |
| `archive-interval-mode` / `archive.intervalMode` | `string` | `"auto"` | `"auto"` or `"manual"`. |
| `archive-interval-seconds` / `archive.intervalSeconds` | `int` | `86400` | Interval in seconds for manual mode. |
| `retention-incremental` / `retention.incremental.enabled` | `bool` | `true` | Enable retention policy for incremental backups. |
| `retention-incremental-hours` / `retention.incremental.hours` | `int` | `0` | Hourly backups to keep. |
| `retention-incremental-days` / `retention.incremental.days` | `int` | `0` | Daily backups to keep. |
| `retention-incremental-weeks` / `retention.incremental.weeks` | `int` | `4` | Weekly backups to keep. |
| `retention-incremental-months` / `retention.incremental.months` | `int` | `0` | Monthly backups to keep. |
| `retention-incremental-years` / `retention.incremental.years` | `int` | `0` | Yearly backups to keep. |
| `retention-snapshot` / `retention.snapshot.enabled` | `bool` | `false` | Enable retention policy for snapshot backups. |
| `retention-snapshot-hours` / `retention.snapshot.hours` | `int` | `-1` | Hourly backups to keep. |
| `retention-snapshot-days` / `retention.snapshot.days` | `int` | `-1` | Daily backups to keep. |
| `retention-snapshot-weeks` / `retention.snapshot.weeks` | `int` | `-1` | Weekly backups to keep. |
| `retention-snapshot-months` / `retention.snapshot.months` | `int` | `-1` | Monthly backups to keep. |
| `retention-snapshot-years` / `retention.snapshot.years` | `int` | `-1` | Yearly backups to keep. |
| **Compression** | | | |
| `compression` / `compression.enabled` | `bool` | `true` | Enable compression for backups. |
| `compression-format` / `compression.format` | `string` | `"tar.zst"` | `"zip"`, `"tar.gz"`, or `"tar.zst"`. |
| `compression-level` / `compression.level` | `string` | `"default"` | Compression level: `"default"`, `"fastest"`, `"better"`, `"best"`. |
| **Performance Tuning** | | | |
| `sync-workers` / `engine.performance.syncWorkers` | `int` | `4` | Number of concurrent workers for file synchronization. |
| `mirror-workers` / `engine.performance.mirrorWorkers` | `int` | `4` | Number of concurrent workers for file deletions in mirror mode. |
| `delete-workers` / `engine.performance.deleteWorkers` | `int` | `4` | Number of concurrent workers for deleting outdated backups. |
| `compress-workers` / `engine.performance.compressWorkers` | `int` | `4` | Number of concurrent workers for compressing backups. |
| `buffer-size-kb` / `engine.performance.bufferSizeKB` | `int64` | `1024` | Size of the I/O buffer in kilobytes for file copies and compression. |
| `readahead-limit-kb` / `engine.performance.readAheadLimitKB` | `int64` | `262144` | Limit of the I/O readahead in kilobytes for file compression. 0 disables the readahead on systems with very low memory |

## Troubleshooting

### Error: "Unrecognized Developer" or "Untrusted" warning on startup

This occurs because the binaries are currently unsigned. Please refer to the [Security and Binary Verification](#security-and-binary-verification) section for instructions on how to verify and run the application on macOS and Windows.

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
    *   For systems with slow I/O (like a single spinning disk or a high-latency network share), *decreasing* the number of `sync-workers` (e.g., to `1` or `2`) is recommended to minimize seek latency, as higher concurrency can cause significant thrashing.
    *   For systems with very fast I/O (like a local NVMe SSD), increasing `sync-workers` might yield better results.
*   **Cause**: The default "Safe Copy" mechanism ensures atomicity but increases metadata operations (create temp file + rename). This can be slow on high-latency network shares or with many small files.
*   **Solution**:
    *   Try disabling Safe Copy by setting `"safeCopy": false` in your config or using `-sync-safe-copy=false`. This switches to direct writes, improving performance at the cost of atomicity (a power loss during copy might leave a partial file, which will be fixed on the next run).

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

*   **Cause**: The file is currently open and locked by another application (e.g., an open Word document, Outlook .pst file, or running database).
*   **Note**: `pgl-backup` does not use the Volume Shadow Copy Service (VSS). VSS is a complex, Windows-only technology that requires Administrator privileges and is typically only needed for specific edge cases. `pgl-backup` prioritizes simplicity, speed, and cross-platform consistency.
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
