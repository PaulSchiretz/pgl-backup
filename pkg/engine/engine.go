package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/lockfile"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/patharchive"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/pathretention"
	"github.com/paulschiretz/pgl-backup/pkg/pathsync"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/preflight"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// --- ARCHITECTURAL OVERVIEW: Core Strategies ---
//
// The engine orchestrates three distinct strategies to handle the lifecycle of backups,
// balancing user predictability, historical consistency, and system robustness.
//
// 1. Archive (Snapshot Creation) - "Predictable Creation"
//    - Goal:  To honor the user's configured `ArchiveInterval` as literally as possible.
//    - Logic: Calculates time-based "bucketing" based on the **local system's midnight**
//             for day-or-longer intervals. This gives the user direct, predictable control
//             over the *frequency* of new archives, anchored to their local day.
//
// 2. Retention (Snapshot Deletion) - "Consistent History"
//    - Goal:  To organize the backup history into intuitive, calendar-based slots for cleanup.
//    - Logic: Applies fixed calendar concepts (like ISO weeks and YYYY-MM-DD dates) to the
//             **UTC timestamp** stored in each backup's metadata. This ensures retention rules
//             always refer to standard calendar periods defined in UTC, providing a clean
//             and portable history.
//
// 3. Compression - "Compress Once / Fail-Forward"
//    - Goal:  To ensure robustness and performance by avoiding "poison pill" scenarios.
//    - Logic: Only attempts to compress the specific backup created during the current run.
//             If compression fails (e.g., due to corrupt data), the backup is left uncompressed,
//             but future runs continue to succeed. This avoids the overhead and risk of
//             scanning/retrying historical failures on every run.
//
// By decoupling these concepts, the system provides a predictable creation schedule,
// a consistent historical view, and a resilient compression pipeline.

// engineRunState holds the mutable state for a single execution of the backup engine.
// This makes the Engine itself stateless and safe for concurrent use if needed.
type engineRunState struct {
	source              string
	mode                config.BackupMode
	currentTimestampUTC time.Time
	target              string
}

// Engine orchestrates the entire backup process.
type Engine struct {
	config             config.Config
	version            string
	syncer             pathsync.Syncer
	archiver           patharchive.Archiver
	retentionManager   pathretention.RetentionManager
	compressionManager pathcompression.CompressionManager
	// hookCommandExecutor allows mocking os/exec for testing hooks.
	hookCommandExecutor func(ctx context.Context, name string, arg ...string) *exec.Cmd
}

// New creates a new backup engine with the given configuration and version.
func New(cfg config.Config, version string) *Engine {
	return &Engine{
		config:              cfg,
		version:             version,
		syncer:              pathsync.NewPathSyncer(cfg),
		archiver:            patharchive.NewPathArchiver(cfg),
		retentionManager:    pathretention.NewPathRetentionManager(cfg),
		compressionManager:  pathcompression.NewPathCompressionManager(cfg),
		hookCommandExecutor: exec.CommandContext, // Default to the real implementation.
	}
}

// acquireTargetLock ensures the target directory exists and acquires a file lock within it.
// It returns a release function that must be called to unlock the directory.
func (e *Engine) acquireTargetLock(ctx context.Context) (func(), error) {
	lockFilePath := filepath.Join(e.config.Paths.TargetBase, config.LockFileName)
	appID := fmt.Sprintf("pgl-backup:%s", e.config.Paths.Source)

	plog.Debug("Attempting to acquire lock", "path", lockFilePath)
	lock, err := lockfile.Acquire(ctx, lockFilePath, appID)
	if err != nil {
		var lockErr *lockfile.ErrLockActive
		if errors.As(err, &lockErr) {
			plog.Warn("Operation is already running for this target, skipping run.", "details", lockErr.Error())
			return nil, nil // Return nil error to indicate a graceful exit.
		}
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	plog.Debug("Lock acquired successfully.")

	return lock.Release, nil
}

// InitializeBackupTarget sets up a new backup target directory by running pre-flight checks
// and generating a default configuration file.
func (e *Engine) InitializeBackupTarget(ctx context.Context) error {
	// Check for cancellation at the very beginning.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	plog.Info("Initializing backup target")

	// Perform preflight checks before attempting to lock or write.
	// The config is passed by pointer because Validate() can modify it (e.g., cleaning paths).
	if err := preflight.RunChecks(&e.config); err != nil {
		return err
	}

	// Now that pre-flight checks (including directory creation) have passed, acquire the lock.
	releaseLock, err := e.acquireTargetLock(ctx)
	if err != nil {
		return err
	}
	if releaseLock == nil {
		return nil // Lock was already held by another process, exit gracefully.
	}
	defer releaseLock()

	plog.Info("Pre-flight checks passed. Generating configuration file.")

	// Generate the pgl-backup.config.json file in the target directory.
	return config.Generate(e.config)
}

// ExecuteBackup runs the entire backup job from start to finish.
func (e *Engine) ExecuteBackup(ctx context.Context) error {
	// Check for cancellation at the very beginning.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Perform preflight checks on the final, merged configuration.
	if err := preflight.RunChecks(&e.config); err != nil {
		return err
	}

	// Acquire Lock on Target Directory using final config.
	releaseLock, err := e.acquireTargetLock(ctx)
	if err != nil {
		return err // A real error occurred during lock acquisition.
	}
	if releaseLock == nil {
		return nil // Lock was already held, exit gracefully.
	}
	defer releaseLock()

	// --- Pre-Backup Hooks ---
	plog.Info("Running pre-backup hooks")
	if err := e.runHooks(ctx, e.config.Hooks.PreBackup, "pre-backup"); err != nil {
		// All pre-backup hook errors are fatal. We wrap the error with a message
		// that distinguishes between a cancellation and a failure.
		errMsg := "pre-backup hook failed"
		if errors.Is(err, context.Canceled) {
			errMsg = "pre-backup hook canceled"
		}
		return fmt.Errorf("%s: %w", errMsg, err)
	}

	// --- Post-Backup Hooks (deferred) ---
	// These will run at the end of the function, even if the backup fails.
	defer func() {
		plog.Info("Running post-backup hooks")
		if err := e.runHooks(ctx, e.config.Hooks.PostBackup, "post-backup"); err != nil {
			if errors.Is(err, context.Canceled) {
				plog.Info("Post-backup hooks skipped due to cancellation.")
			} else {
				plog.Warn("Post-backup hook failed", "error", err)
			}
		}
	}()

	// Capture a consistent UTC timestamp for the entire run to ensure unambiguous folder names
	// and avoid daylight saving time conflicts.
	runState := &engineRunState{
		source:              e.config.Paths.Source,
		mode:                e.config.Mode,
		currentTimestampUTC: time.Now().UTC(),
		target:              "",
	}

	// Prepare for the backup run
	if err := e.prepareRun(runState); err != nil {
		return err
	}

	// Log a single, comprehensive message about the upcoming backup process.
	logMsg := "Starting backup"
	if e.config.DryRun {
		logMsg = "Starting backup (DRY RUN)"
	}
	plog.Info(logMsg,
		"source", runState.source,
		"target", runState.target,
		"mode", runState.mode,
	)

	var backupsToCompress []string

	// Perform incremental Archiving
	if runState.mode == config.IncrementalMode {
		archivePath, err := e.performArchiving(ctx, runState)
		if err != nil {
			return fmt.Errorf("error during backup archiving: %w", err)
		}
		// Make sure an archive was created, the path might be empty if a non critical error occuered.
		if archivePath != "" {
			backupsToCompress = append(backupsToCompress, archivePath)
		}
	}

	// Perform the backup
	if err := e.performSync(ctx, runState); err != nil {
		return fmt.Errorf("fatal backup error during sync: %w", err)
	}

	// Add snapshot to compression list if in snapshot mode
	if runState.mode == config.SnapshotMode {
		backupsToCompress = append(backupsToCompress, runState.target)
	}

	// Clean up outdated backups (archives and snapshots)
	if err := e.performRetention(ctx); err != nil {
		return fmt.Errorf("fatal backup error during retention: %w", err)
	}

	// Compress backups that are eligible
	if err := e.performCompression(ctx, backupsToCompress); err != nil {
		return fmt.Errorf("fatal backup error during compression: %w", err)
	}

	plog.Info("Backup completed")
	return nil
}

// runHooks executes a list of shell commands for a given hook type.
func (e *Engine) runHooks(ctx context.Context, commands []string, hookType string) error {
	if len(commands) == 0 {
		return nil
	}

	for _, command := range commands {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		plog.Info(fmt.Sprintf("Executing %s hook", hookType), "command", command)
		if e.config.DryRun {
			plog.Info("[DRY RUN] Would execute command", "command", command)
			continue
		}

		cmd := e.createHookCommand(ctx, command)

		// Pipe output to our logger for visibility
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			// Check if the context was canceled, which can cause cmd.Wait() to return an error.
			// If so, we should return the context's error to be more specific.
			if ctx.Err() == context.Canceled {
				return context.Canceled
			}
			return fmt.Errorf("command '%s' failed: %w", command, err)
		}
	}
	return nil
}

// performRetention cleans up outdated backups based on the configured retention policies.
// It applies all enabled retention policies regardless of the current backup mode.
// This ensures the backup target is always kept in a consistent state according
// to the user's configuration and prevents "retention debt" if one mode is
// run less frequently than another.
func (e *Engine) performRetention(ctx context.Context) error {
	// Apply retention for incremental archives, if enabled.
	if e.config.Retention.Incremental.Enabled {
		archivesDir := filepath.Join(e.config.Paths.TargetBase, e.config.Paths.ArchivesSubDir)
		incrementalDirName := e.config.Paths.IncrementalSubDir
		if err := e.retentionManager.Apply(ctx, "incremental", archivesDir, e.config.Retention.Incremental, incrementalDirName); err != nil {
			plog.Warn("Error applying incremental retention policy", "error", err)
			// no error is returned as our backup is still good no need to fail here
		}
	}

	// Apply retention for snapshots, if enabled.
	if e.config.Retention.Snapshot.Enabled {
		snapshotsDir := filepath.Join(e.config.Paths.TargetBase, e.config.Paths.SnapshotsSubDir)
		if err := e.retentionManager.Apply(ctx, "snapshot", snapshotsDir, e.config.Retention.Snapshot, ""); err != nil {
			plog.Warn("Error applying snapshot retention policy", "error", err)
			// no error is returned as our backup is still good no need to fail here
		}
	}
	return nil
}

// performCompression compresses outdated backups based on the configured policies.
func (e *Engine) performCompression(ctx context.Context, backupsToCompress []string) error {
	if e.config.Compression.Enabled && len(backupsToCompress) > 0 {
		if err := e.compressionManager.Compress(ctx, backupsToCompress, e.config.Compression); err != nil {
			plog.Warn("Error during backup compression", "error", err)
		}
	}
	return nil
}

// prepareRun calculates the target directory for the backup.
func (e *Engine) prepareRun(runState *engineRunState) error {
	if runState.mode == config.SnapshotMode {
		// SNAPSHOT MODE
		//
		// The directory name must remain uniquely based on UTC time to avoid DST conflicts,
		// but we add the user's local offset to make the timezone clear to the user.
		timestamp := config.FormatTimestampWithOffset(runState.currentTimestampUTC)
		backupDirName := e.config.Naming.Prefix + timestamp
		snapshotsSubDir := filepath.Join(e.config.Paths.TargetBase, e.config.Paths.SnapshotsSubDir)
		if !e.config.DryRun {
			os.MkdirAll(snapshotsSubDir, util.UserWritableDirPerms)
		}
		runState.target = filepath.Join(snapshotsSubDir, backupDirName)
	} else {
		// INCREMENTAL MODE
		incrementalDir := filepath.Join(e.config.Paths.TargetBase, e.config.Paths.IncrementalSubDir)
		runState.target = incrementalDir
	}
	return nil
}

// performSync is the main entry point for synchronization.
func (e *Engine) performSync(ctx context.Context, runState *engineRunState) error {

	mirror := runState.mode == config.IncrementalMode

	// The actual content will be synced into a dedicated subdirectory.
	contentTarget := filepath.Join(runState.target, e.config.Paths.ContentSubDir)
	// If configured, append the source's base directory name to the destination path.
	if e.config.Paths.PreserveSourceDirectoryName {
		var nameToAppend string

		// Check if the path is a root path (e.g., "/" or "C:\")
		if filepath.Dir(runState.source) == runState.source {
			// Handle Windows Drive Roots (e.g., "D:\") -> "D"
			vol := filepath.VolumeName(runState.source)
			// On Windows, for "C:\", VolumeName is "C:", we trim the colon.
			// On Unix, for "/", VolumeName is "", so nameToAppend remains empty.
			if vol != "" && strings.HasSuffix(vol, ":") {
				nameToAppend = strings.TrimSuffix(vol, ":")
			}
		} else {
			// Standard folder
			nameToAppend = filepath.Base(runState.source)
		}

		// Append if valid
		if nameToAppend != "" && nameToAppend != "." && nameToAppend != string(filepath.Separator) {
			contentTarget = filepath.Join(contentTarget, nameToAppend)
		}
	}

	// Sync and check for errors after attempting the sync.
	if syncErr := e.syncer.Sync(ctx, runState.source, contentTarget, mirror, e.config.Paths.ExcludeFiles(), e.config.Paths.ExcludeDirs(), e.config.Metrics); syncErr != nil {
		return fmt.Errorf("sync failed: %w", syncErr)
	}

	if e.config.DryRun {
		plog.Info("[DRY RUN] Would write metafile", "directory", runState.target)
		return nil
	}
	// If the sync was successful, write the metafile to the target for retention purposes.
	metadata := metafile.MetafileContent{
		Version:      e.version,
		TimestampUTC: runState.currentTimestampUTC,
		Mode:         runState.mode.String(),
		Source:       runState.source,
	}
	return metafile.Write(runState.target, metadata)
}

// performArchiving is the main entry point for archive updates.
func (e *Engine) performArchiving(ctx context.Context, runState *engineRunState) (string, error) {
	incrementalDirName := e.config.Paths.IncrementalSubDir
	currentBackupPath := filepath.Join(e.config.Paths.TargetBase, incrementalDirName)
	archivesDir := filepath.Join(e.config.Paths.TargetBase, e.config.Paths.ArchivesSubDir)

	// The archiver responsible for reading the metadata file and determining
	// if an archive is necessary.
	return e.archiver.Archive(ctx, archivesDir, currentBackupPath, runState.currentTimestampUTC)
}
