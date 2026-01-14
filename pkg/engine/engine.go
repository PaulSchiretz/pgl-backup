package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	absSource     string
	absTargetBase string

	mode         config.BackupMode
	timestampUTC time.Time

	relCurrentPath string
	relArchivePath string

	doSync                   bool
	relSyncTargetPath        string
	relSyncTargetContentPath string
	compressSyncResult       bool

	doArchiving             bool
	archivingPolicy         config.ArchivePolicyConfig
	compressArchivingResult bool

	doRetention     bool
	retentionPolicy config.RetentionPolicyConfig

	doCompression     bool
	compressionPolicy config.CompressionPolicyConfig

	absBackupPathsToCompress []string
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

// initBackupRun sets up the runState for a backup run
func (e *Engine) initBackupRun(mode config.BackupMode) (*engineRunState, error) {

	// Capture a consistent UTC timestamp for the entire run to ensure unambiguous folder names
	// and avoid daylight saving time conflicts.
	runState := &engineRunState{
		absSource:     e.config.Paths.Source,
		absTargetBase: e.config.Paths.TargetBase,
		mode:          mode,
		timestampUTC:  time.Now().UTC(),
	}

	if runState.mode == config.SnapshotMode {
		// SNAPSHOT MODE

		runState.relCurrentPath = e.config.Paths.SnapshotSubDirs.Current
		runState.relArchivePath = e.config.Paths.SnapshotSubDirs.Archive

		runState.doSync = true
		runState.compressSyncResult = true
		// The relSyncTargetPath name must remain uniquely based on UTC time to avoid DST conflicts,
		// but we add the user's local offset to make the timezone clear to the user.
		timestamp := config.FormatTimestampWithOffset(runState.timestampUTC)
		backupDirName := e.config.Naming.Prefix + timestamp
		// we sync our snapshots directly to the archive dir for now!
		runState.relSyncTargetPath = filepath.Join(e.config.Paths.SnapshotSubDirs.Archive, backupDirName)
		runState.relSyncTargetContentPath = filepath.Join(runState.relSyncTargetPath, e.config.Paths.ContentSubDir)

		runState.doArchiving = false
		runState.compressArchivingResult = false

		runState.doCompression = e.config.Compression.Snapshot.Enabled
		runState.compressionPolicy = e.config.Compression.Snapshot

		runState.doRetention = e.config.Retention.Snapshot.Enabled
		runState.retentionPolicy = e.config.Retention.Snapshot

		if !e.config.DryRun && runState.doSync {
			absSyncTargetPath := filepath.Join(runState.absTargetBase, runState.relSyncTargetPath)
			err := os.MkdirAll(absSyncTargetPath, util.UserWritableDirPerms)
			if err != nil {
				return nil, err
			}
		}
	} else {
		// INCREMENTAL MODE
		runState.relCurrentPath = e.config.Paths.IncrementalSubDirs.Current
		runState.relArchivePath = e.config.Paths.IncrementalSubDirs.Archive

		runState.doSync = true
		runState.compressSyncResult = false
		runState.relSyncTargetPath = e.config.Paths.IncrementalSubDirs.Current
		runState.relSyncTargetContentPath = filepath.Join(runState.relSyncTargetPath, e.config.Paths.ContentSubDir)

		runState.doArchiving = true
		runState.archivingPolicy = e.config.Archive.Incremental
		runState.compressArchivingResult = true

		runState.doCompression = e.config.Compression.Incremental.Enabled
		runState.compressionPolicy = e.config.Compression.Incremental

		runState.doRetention = e.config.Retention.Incremental.Enabled
		runState.retentionPolicy = e.config.Retention.Incremental

		if !e.config.DryRun && runState.doSync {
			absSyncTargetPath := filepath.Join(runState.absTargetBase, runState.relSyncTargetPath)
			err := os.MkdirAll(absSyncTargetPath, util.UserWritableDirPerms)
			if err != nil {
				return nil, err
			}
		}
	}
	return runState, nil
}

// initPruneRun sets up the runState for a prune run for a BackupMode
func (e *Engine) initPruneRun(mode config.BackupMode) (*engineRunState, error) {

	// Capture a consistent UTC timestamp for the entire run to ensure unambiguous folder names
	// and avoid daylight saving time conflicts.
	runState := &engineRunState{
		absSource:     e.config.Paths.Source,
		absTargetBase: e.config.Paths.TargetBase,
		mode:          mode,
		timestampUTC:  time.Now().UTC(),
	}

	if runState.mode == config.SnapshotMode {
		// SNAPSHOT MODE
		runState.relCurrentPath = e.config.Paths.SnapshotSubDirs.Current
		runState.relArchivePath = e.config.Paths.SnapshotSubDirs.Archive

		runState.doSync = false
		runState.doArchiving = false
		runState.doCompression = false

		runState.doRetention = e.config.Retention.Snapshot.Enabled
		runState.retentionPolicy = e.config.Retention.Snapshot
	} else {
		// INCREMENTAL MODE
		runState.relCurrentPath = e.config.Paths.IncrementalSubDirs.Current
		runState.relArchivePath = e.config.Paths.IncrementalSubDirs.Archive

		runState.doSync = false
		runState.doArchiving = false
		runState.doCompression = false

		runState.doRetention = e.config.Retention.Incremental.Enabled
		runState.retentionPolicy = e.config.Retention.Incremental
	}
	return runState, nil
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
	if err := preflight.RunChecks(&e.config, preflight.PreflightChecks{
		SourceAccessible:   true,
		TargetAccessible:   true,
		TargetWriteable:    true,
		CaseMismatch:       true,
		PathNesting:        true,
		EnsureTargetExists: true,
	}); err != nil {
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
	if err := preflight.RunChecks(&e.config, preflight.PreflightChecks{
		SourceAccessible:   true,
		TargetAccessible:   true,
		TargetWriteable:    true,
		CaseMismatch:       true,
		PathNesting:        true,
		EnsureTargetExists: true,
	}); err != nil {
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
				plog.Info("post-backup hooks skipped due to cancellation.")
			} else {
				plog.Warn("post-backup hook failed", "error", err)
			}
		}
	}()

	// Intitialize the backup run
	r, err := e.initBackupRun(e.config.Mode)
	if err != nil {
		return err
	}

	// Log a single, comprehensive message about the upcoming backup process.
	logMsg := "Starting backup"
	if e.config.DryRun {
		logMsg = "Starting backup (DRY RUN)"
	}
	plog.Info(logMsg,
		"source", r.absSource,
		"target", r.absTargetBase,
		"mode", r.mode,
	)

	// Perform Archiving
	if err := e.performArchiving(ctx, r); err != nil {
		return fmt.Errorf("fatal error during archiving: %w", err)
	}

	// Perform the backup
	if err := e.performSync(ctx, r); err != nil {
		return fmt.Errorf("fatal error during sync: %w", err)
	}

	// Clean up outdated backups (archives and snapshots)
	if err := e.performRetention(ctx, r); err != nil {
		return fmt.Errorf("fatal error during retention: %w", err)
	}

	// Compress backups that are eligible
	if err := e.performCompression(ctx, r); err != nil {
		return fmt.Errorf("fatal error during compression: %w", err)
	}

	plog.Info("Backup completed")
	return nil
}

// ExecutePrune executes the retention policies to clean up outdated backups.
func (e *Engine) ExecutePrune(ctx context.Context) error {
	// Check for cancellation at the very beginning.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Perform preflight checks on the final, merged configuration.
	if err := preflight.RunChecks(&e.config, preflight.PreflightChecks{
		SourceAccessible:   false,
		TargetAccessible:   true,
		TargetWriteable:    true,
		CaseMismatch:       false,
		PathNesting:        false,
		EnsureTargetExists: false,
	}); err != nil {
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

	plog.Info("Starting prune", "target", e.config.Paths.TargetBase)

	// Prune Incremental Archives
	ipr, err := e.initPruneRun(config.IncrementalMode)
	if err != nil {
		return err
	}
	if err := e.performRetention(ctx, ipr); err != nil {
		return fmt.Errorf("prune incremental error: %w", err)
	}

	// Prune Snapshots
	spr, err := e.initPruneRun(config.SnapshotMode)
	if err != nil {
		return err
	}
	if err := e.performRetention(ctx, spr); err != nil {
		return fmt.Errorf("prune snapshot error: %w", err)
	}

	plog.Info("Prune completed")
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
func (e *Engine) performRetention(ctx context.Context, r *engineRunState) error {
	if !r.doRetention {
		return nil
	}

	absArchivePath := filepath.Join(r.absTargetBase, r.relArchivePath)
	if err := e.retentionManager.Apply(ctx, r.mode.String(), absArchivePath, r.retentionPolicy, r.relCurrentPath); err != nil {
		plog.Warn("Error applying retention policy", "mote", r.mode.String(), "error", err)
		// no error is returned as our backup is still good no need to fail here
	}
	return nil
}

// performCompression compresses backups based on the configured policies.
func (e *Engine) performCompression(ctx context.Context, r *engineRunState) error {
	if r.doCompression && len(r.absBackupPathsToCompress) > 0 {
		if err := e.compressionManager.Compress(ctx, r.absBackupPathsToCompress, r.compressionPolicy); err != nil {
			plog.Warn("Error during backup compression", "error", err)
		}
	}
	return nil
}

// performSync is the main entry point for synchronization.
func (e *Engine) performSync(ctx context.Context, r *engineRunState) error {

	if !r.doSync {
		return nil
	}

	mirror := r.mode == config.IncrementalMode

	// The actual content will be synced into a dedicated subdirectory.
	absSyncTarget := filepath.Join(r.absTargetBase, r.relSyncTargetPath)
	absSyncContentTarget := filepath.Join(r.absTargetBase, r.relSyncTargetContentPath)

	// Sync and check for errors after attempting the sync.
	if err := e.syncer.Sync(ctx, r.absSource, absSyncContentTarget, e.config.Paths.PreserveSourceDirectoryName, mirror, e.config.Paths.ExcludeFiles(), e.config.Paths.ExcludeDirs(), e.config.Metrics); err != nil {
		return err
	}

	// If the sync was successful, write the metafile to the target for retention purposes.
	if e.config.DryRun {
		plog.Info("[DRY RUN] Would write metafile", "directory", absSyncTarget)
		return nil
	} else {
		metadata := metafile.MetafileContent{
			Version:      e.version,
			TimestampUTC: r.timestampUTC,
			Mode:         r.mode.String(),
			Source:       r.absSource,
		}
		err := metafile.Write(absSyncTarget, metadata)
		if err != nil {
			return err
		}
	}

	if r.compressSyncResult {
		r.absBackupPathsToCompress = append(r.absBackupPathsToCompress, absSyncTarget)
	}
	return nil
}

// performArchiving is the main entry point for archive updates.
func (e *Engine) performArchiving(ctx context.Context, r *engineRunState) error {

	if !r.doArchiving {
		return nil
	}

	absCurrentPath := filepath.Join(e.config.Paths.TargetBase, r.relCurrentPath)
	absArchivePath := filepath.Join(r.absTargetBase, r.relArchivePath)

	// The archiver responsible for reading the metadata file and determining
	// if an archive is necessary.
	absArchivePathCreated, err := e.archiver.Archive(ctx, absArchivePath, absCurrentPath, r.timestampUTC, r.archivingPolicy, r.retentionPolicy)
	if err != nil {
		if err != patharchive.ErrNothingToArchive {
			return fmt.Errorf("error during backup archiving: %w", err)
		}
		return nil
	}

	// mark archivePath for compression
	if r.compressArchivingResult {
		r.absBackupPathsToCompress = append(r.absBackupPathsToCompress, absArchivePathCreated)
	}
	return nil
}
