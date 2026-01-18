package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/lockfile"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/patharchive"
	"github.com/paulschiretz/pgl-backup/pkg/planner"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
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

func (r *Runner) ExecuteBackup(ctx context.Context, absSourcePath, absTargetBasePath string, p *planner.BackupPlan) error {
	// Check for cancellation at the very beginning.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// save the execution timestamp
	timestampUTC := time.Now().UTC()

	// Run Preflight Validation
	if err := r.validator.Run(ctx, absSourcePath, absTargetBasePath, p.Preflight, timestampUTC); err != nil {
		return fmt.Errorf("preflight failed: %w", err)
	}

	// Acquire Lock on Target Directory.
	releaseLock, err := r.acquireTargetLock(ctx, absTargetBasePath)
	if err != nil {
		return err // A real error occurred during lock acquisition.
	}
	if releaseLock == nil {
		return nil // Lock was already held, exit gracefully.
	}
	defer releaseLock()

	// --- Pre-Backup Hooks ---
	plog.Info("Running pre-backup hooks")
	if err := r.runHooks(ctx, p.PreBackupHooks, "pre-backup", p.DryRun); err != nil {
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
		if err := r.runHooks(ctx, p.PostBackupHooks, "post-backup", p.DryRun); err != nil {
			if errors.Is(err, context.Canceled) {
				plog.Info("post-backup hooks skipped due to cancellation.")
			} else {
				plog.Warn("post-backup hook failed", "error", err)
			}
		}
	}()

	plog.Info("Starting backup", "source", absSourcePath, "target", absTargetBasePath, "mode", p.Mode)

	// Perform Archiving before Sync in Incremental mode
	if p.Archive.Enabled && p.Mode == planner.Incremental {
		toArchive, err := r.fetchBackup(absTargetBasePath, p.Paths.RelCurrentPathKey)
		if err != nil {
			if !os.IsNotExist(err) { // This is a normal condition on the first incremental run; no previous current backup to archive.
				if p.FailFast {
					return fmt.Errorf("Error reading backup for archive: %w", err)
				}
				plog.Warn("Error reading backup for archive, skipping", "error", err)
			}
		} else {
			if err := r.archiver.Archive(ctx, absTargetBasePath, p.Paths.RelArchivePathKey, p.Paths.BackupDirPrefix, toArchive, p.Archive, timestampUTC); err != nil {
				if err != patharchive.ErrNothingToArchive {
					if p.FailFast {
						return fmt.Errorf("Error during archive: %w", err)
					}
					plog.Warn("Error during archive, skipping archive", "error", err)
				}
			}
		}
	}

	// Perform the backup
	if p.Sync.Enabled {
		if err := r.syncer.Sync(ctx, absSourcePath, absTargetBasePath, p.Paths.RelCurrentPathKey, p.Paths.RelContentPathKey, p.Sync, timestampUTC); err != nil {
			return fmt.Errorf("error during sync: %w", err)
		}
	}

	// Perform Archiving for Snapshot mode. We are strict here with errors as Snapshot without Archiving makes no sense
	if p.Archive.Enabled && p.Mode == planner.Snapshot {
		toArchive := p.Sync.ResultInfo
		if toArchive.RelPathKey == "" {
			return fmt.Errorf("Error no snapshot syncResult to archive")
		}
		if err := r.archiver.Archive(ctx, absTargetBasePath, p.Paths.RelArchivePathKey, p.Paths.BackupDirPrefix, toArchive, p.Archive, timestampUTC); err != nil {
			return fmt.Errorf("Error during archive: %w", err)
		}
	}

	// Clean up outdated backups
	if p.Retention.Enabled {

		// Add our just created sync/archive relPathKey to the exclusion list for retention so they are never pruned
		// NOTE: This is needed in case our retention is enabled but 0,0,0,0,0 and we just created a snapshot.(otherwise we would imetialy remove it again)
		var relPathExclusionKeys []string
		if p.Archive.ResultInfo.RelPathKey != "" {
			relPathExclusionKeys = append(relPathExclusionKeys, p.Archive.ResultInfo.RelPathKey)
		}
		if p.Sync.ResultInfo.RelPathKey != "" {
			relPathExclusionKeys = append(relPathExclusionKeys, p.Sync.ResultInfo.RelPathKey)
		}

		// Fetch backups that might need pruning
		toRetent, err := r.fetchBackups(ctx, absTargetBasePath, p.Paths.RelArchivePathKey, p.Paths.BackupDirPrefix, relPathExclusionKeys)
		if err != nil {
			if p.FailFast {
				return fmt.Errorf("Error reading backups for prune: %w", err)
			}
			plog.Warn("Error reading backups for prune, skipping", "error", err)
		}
		if err := r.retainer.Prune(ctx, absTargetBasePath, toRetent, p.Retention, timestampUTC); err != nil {
			if p.FailFast {
				return fmt.Errorf("Error during prune: %w", err)
			}
			plog.Warn("Error during prune, skipping prune", "error", err)
		}
	}

	// Compress backups that are eligible
	if p.Compression.Enabled {
		var toCompress []metafile.MetafileInfo
		if p.Archive.ResultInfo.RelPathKey != "" {
			toCompress = append(toCompress, p.Archive.ResultInfo)
		}

		if err := r.compressor.Compress(ctx, absTargetBasePath, p.Paths.RelContentPathKey, toCompress, p.Compression, timestampUTC); err != nil {
			if p.FailFast {
				return fmt.Errorf("Error during compress: %w", err)
			}
			plog.Warn("Error during compress, skipping compress", "error", err)
		}
	}
	plog.Info("Backup completed")
	return nil
}

func (r *Runner) ExecutePrune(ctx context.Context, absTargetBasePath string, p *planner.PrunePlan) error {
	// Check for cancellation at the very beginning.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// save the execution timestamp
	timestampUTC := time.Now().UTC()

	// Run Preflight Validation
	if err := r.validator.Run(ctx, "", absTargetBasePath, p.Preflight, timestampUTC); err != nil {
		return fmt.Errorf("preflight failed: %w", err)
	}

	// Acquire Lock on Target Directory.
	releaseLock, err := r.acquireTargetLock(ctx, absTargetBasePath)
	if err != nil {
		return err // A real error occurred during lock acquisition.
	}
	if releaseLock == nil {
		return nil // Lock was already held, exit gracefully.
	}
	defer releaseLock()

	plog.Info("Starting prune", "target", absTargetBasePath)

	// Standalone prune logic
	if p.RetentionIncremental.Enabled {
		toRetent, err := r.fetchBackups(ctx, absTargetBasePath, p.PathsIncremental.RelArchivePathKey, p.PathsIncremental.BackupDirPrefix, []string{})
		if err != nil {
			return fmt.Errorf("fatal error during prune incremental: %w", err)
		}
		if err := r.retainer.Prune(ctx, absTargetBasePath, toRetent, p.RetentionIncremental, timestampUTC); err != nil {
			return fmt.Errorf("fatal error during prune incremental: %w", err)
		}
	}

	if p.RetentionSnapshot.Enabled {
		toRetent, err := r.fetchBackups(ctx, absTargetBasePath, p.PathsSnapshot.RelArchivePathKey, p.PathsSnapshot.BackupDirPrefix, []string{})
		if err != nil {
			return fmt.Errorf("fatal error during prune snapshot: %w", err)
		}
		if err := r.retainer.Prune(ctx, absTargetBasePath, toRetent, p.RetentionSnapshot, timestampUTC); err != nil {
			return fmt.Errorf("fatal error during prune snapshot: %w", err)
		}
	}
	plog.Info("Prune completed")
	return nil
}

// acquireTargetLock ensures the target directory exists and acquires a file lock within it.
// It returns a release function that must be called to unlock the directory.
func (r *Runner) acquireTargetLock(ctx context.Context, absTargetBasePath string) (func(), error) {
	appID := fmt.Sprintf("pgl-backup:%s", absTargetBasePath)

	plog.Debug("Attempting to acquire lock", "path", absTargetBasePath)
	lock, err := lockfile.Acquire(ctx, absTargetBasePath, appID)
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

// fetchBackups scans a directory for valid backup folders and parses their metadata
// It relies exclusively on the `.pgl-backup.meta.json` file; directories without a
// readable metafile are ignored.
// The relPathExclusionKeys are Relative to the absTargetBasePath, and filtered internally.
func (r *Runner) fetchBackups(ctx context.Context, absTargetBasePath, relArchivePathKey, dirNamePrefix string, relPathExclusionKeys []string) ([]metafile.MetafileInfo, error) {

	absArchivePath := util.DenormalizePath(filepath.Join(absTargetBasePath, relArchivePathKey))
	entries, err := os.ReadDir(absArchivePath)
	if err != nil {
		if os.IsNotExist(err) {
			plog.Debug("Archives directory does not exist yet, skipping.", "path", relArchivePathKey)
			return []metafile.MetafileInfo{}, nil // Not an error, just means no archives exist yet.
		}
		return []metafile.MetafileInfo{}, fmt.Errorf("failed to read archive directory %s: %w", relArchivePathKey, err)
	}

	excludedDirsInArchive := make(map[string]struct{})
	for _, relPathExclusionKey := range relPathExclusionKeys {
		// Calculate the relative path from the archive root to the exclusion target
		// Example: relArchivePathKey: "backups/daily"
		//          relPathExclusionKey: "backups/daily/old_data"
		//          relPathInArchive will be "old_data"
		pathInArchive, err := filepath.Rel(relArchivePathKey, relPathExclusionKey)
		if err != nil {
			continue // Paths weren't compatible
		}
		relPathInArchive := util.NormalizePath(pathInArchive)

		// filepath.Rel returns ".." if the path is outside the base
		if !strings.HasPrefix(relPathInArchive, "..") && relPathInArchive != "." {
			excludedDirsInArchive[relPathInArchive] = struct{}{}
		}
	}

	var foundBackups []metafile.MetafileInfo
	for _, entry := range entries {
		// Check for cancellation during the directory scan.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		dirName := entry.Name()
		if !entry.IsDir() || !strings.HasPrefix(dirName, dirNamePrefix) {
			continue
		}

		// Check if excluded
		if _, ok := excludedDirsInArchive[dirName]; ok {
			continue
		}

		relBackupPathkey := util.NormalizePath(filepath.Join(relArchivePathKey, dirName))
		foundBackup, err := r.fetchBackup(absTargetBasePath, relBackupPathkey)
		if err != nil {
			plog.Warn("Skipping backup directory; cannot read metadata", "directory", dirName, "reason", err)
			continue
		}
		foundBackups = append(foundBackups, foundBackup)
	}
	return foundBackups, nil
}

func (r *Runner) fetchBackup(absTargetBasePath, relPathKey string) (metafile.MetafileInfo, error) {
	absBackupPath := util.DenormalizePath(filepath.Join(absTargetBasePath, relPathKey))
	metadata, err := metafile.Read(absBackupPath)
	if err != nil {
		return metafile.MetafileInfo{}, err
	}
	return metafile.MetafileInfo{RelPathKey: relPathKey, Metadata: metadata}, nil
}

// runHooks executes a list of shell commands for a given hook type.
func (r *Runner) runHooks(ctx context.Context, commands []string, hookType string, dryRun bool) error {
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
		if dryRun {
			plog.Info("[DRY RUN] Would execute command", "command", command)
			continue
		}

		cmd := r.createHookCommand(ctx, command)

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
