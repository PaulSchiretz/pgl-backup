package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/hints"
	"github.com/paulschiretz/pgl-backup/pkg/lockfile"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/patharchive"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/pathretention"
	"github.com/paulschiretz/pgl-backup/pkg/pathsync"
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

func (r *Runner) ExecuteBackup(ctx context.Context, absBasePath, absSourcePath string, p *planner.BackupPlan) error {
	// Check for cancellation at the very beginning.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// save the execution timestamp
	timestampUTC := time.Now().UTC()

	// Run Preflight Validation, our absBasePath acts as target in backup mode
	if err := r.validator.Run(ctx, absSourcePath, absBasePath, p.Preflight, timestampUTC); err != nil {
		return fmt.Errorf("preflight failed: %w", err)
	}

	// Acquire Lock on Target Directory.
	releaseLock, err := r.acquireTargetLock(ctx, absBasePath)
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

	plog.Info("Starting backup", "base", absBasePath, "source", absSourcePath, "mode", p.Mode)

	syncErr := pathsync.ErrDisabled
	archiveErr := patharchive.ErrDisabled
	pruneErr := pathretention.ErrDisabled
	compressErr := pathcompression.ErrDisabled
	var syncResult, archiveResult metafile.MetafileInfo

	// Perform Archiving before Sync in Incremental mode
	if p.Archive.Enabled && p.Mode == planner.Incremental {
		var toArchive metafile.MetafileInfo
		toArchive, archiveErr = r.fetchBackup(absBasePath, p.Paths.RelCurrentPathKey)
		if archiveErr != nil {
			if !os.IsNotExist(archiveErr) { // This is a normal condition on the first incremental run; no previous current backup to archive.
				archiveErr = fmt.Errorf("error reading backup for archive: %w", archiveErr)
				if p.FailFast {
					return archiveErr
				}
				plog.Warn("Error reading backup for archive, skipping", "error", archiveErr)
			}
		} else {
			archiveResult, archiveErr = r.archiver.Archive(ctx, absBasePath, p.Paths.RelArchivePathKey, p.Paths.BackupNamePrefix, toArchive, p.Archive, timestampUTC)
			if archiveErr != nil {
				if hints.IsHint(archiveErr) {
					plog.Debug("Archiving skipped", "reason", archiveErr)
				} else {
					archiveErr = fmt.Errorf("error during archive: %w", archiveErr)
					if p.FailFast {
						return archiveErr
					}
					plog.Warn("Error during archive, skipping archive", "error", archiveErr)
				}
			} else if archiveResult.RelPathKey == "" {
				archiveErr = fmt.Errorf("archive succeeded but ResultInfo is empty")
				if p.FailFast {
					return archiveErr
				}
				plog.Error("Error after archive, consistency check failed", "error", archiveErr)
			}
		}
	}

	// Perform the backup
	if p.Sync.Enabled {
		syncResult, syncErr = r.syncer.Sync(ctx, absBasePath, absSourcePath, p.Paths.RelCurrentPathKey, p.Paths.RelContentPathKey, p.Sync, timestampUTC)
		if syncErr != nil {
			if hints.IsHint(syncErr) {
				plog.Debug("Sync skipped", "reason", syncErr)
			} else {
				syncErr = fmt.Errorf("error during sync: %w", syncErr)
				plog.Error("Sync failed", "error", syncErr)
			}
		} else {
			// Sync successful. Write the metafile using the info populated by Sync.
			if syncResult.RelPathKey == "" {
				syncErr = fmt.Errorf("sync succeeded but ResultInfo is empty")
				plog.Error("Metafile write failed", "error", syncErr)
			} else {
				absTargetCurrentPath := util.DenormalizePath(filepath.Join(absBasePath, syncResult.RelPathKey))

				if p.DryRun {
					plog.Info("[DRY RUN] Would write metafile", "directory", absTargetCurrentPath)
				} else {
					if syncErr = metafile.Write(absTargetCurrentPath, &syncResult.Metadata); syncErr != nil {
						syncErr = fmt.Errorf("failed to write metafile: %w", syncErr)
						plog.Error("Metafile write failed", "error", syncErr)
					}
				}
			}
		}
	}

	// Perform Archiving for Snapshot mode. We are strict here with errors as Snapshot without Archiving makes no sense
	if syncErr == nil && p.Archive.Enabled && p.Mode == planner.Snapshot {
		var toArchive metafile.MetafileInfo
		toArchive = syncResult
		archiveResult, archiveErr = r.archiver.Archive(ctx, absBasePath, p.Paths.RelArchivePathKey, p.Paths.BackupNamePrefix, toArchive, p.Archive, timestampUTC)
		if archiveErr != nil {
			if hints.Is(archiveErr, patharchive.ErrDisabled) {
				plog.Debug("Archiving skipped", "reason", archiveErr)
			} else {
				return fmt.Errorf("error during archive: %w", archiveErr)
			}
		} else if archiveResult.RelPathKey == "" {
			return fmt.Errorf("archive succeeded but ResultInfo is empty")
		}
	}

	// Clean up outdated backups
	if p.Retention.Enabled {
		// Add our just created sync/archive relPathKey to the exclusion list for retention so they are never pruned
		// NOTE: This is needed in case our retention is enabled but 0,0,0,0,0 and we just created a snapshot.(otherwise we would imetialy remove it again)
		var relPathExclusionKeys []string
		if archiveResult.RelPathKey != "" {
			relPathExclusionKeys = append(relPathExclusionKeys, archiveResult.RelPathKey)
		}
		if syncResult.RelPathKey != "" {
			relPathExclusionKeys = append(relPathExclusionKeys, syncResult.RelPathKey)
		}

		// Fetch backups that might need pruning
		var toRetent []metafile.MetafileInfo
		toRetent, pruneErr = r.fetchBackups(ctx, absBasePath, p.Paths.RelArchivePathKey, p.Paths.BackupNamePrefix, relPathExclusionKeys)
		if pruneErr != nil {
			if p.FailFast {
				return fmt.Errorf("error reading backups for prune: %w", pruneErr)
			}
			plog.Warn("Error reading backups for prune, skipping", "error", pruneErr)
		}
		if pruneErr = r.retainer.Prune(ctx, absBasePath, toRetent, p.Retention, timestampUTC); pruneErr != nil {
			if hints.IsHint(pruneErr) {
				plog.Debug("Retention skipped", "reason", pruneErr)
			} else {
				if p.FailFast {
					return fmt.Errorf("error during prune: %w", pruneErr)
				}
				plog.Warn("Error during prune, skipping prune", "error", pruneErr)
			}
		}
	}

	// Compress backups that are eligible
	if archiveErr == nil && p.Compression.Enabled {
		var toCompress metafile.MetafileInfo
		if archiveResult.RelPathKey != "" {
			toCompress = archiveResult
		}

		if compressErr = r.compressor.Compress(ctx, absBasePath, p.Paths.RelContentPathKey, toCompress, p.Compression, timestampUTC); compressErr != nil {
			if hints.IsHint(compressErr) {
				plog.Debug("Compression skipped", "reason", compressErr)
			} else {
				compressErr = fmt.Errorf("error during compress: %w", compressErr)
				if p.FailFast {
					return compressErr
				}
				plog.Warn("Error during compress, skipping compress", "error", compressErr)
			}
		}
	}

	if syncErr != nil && !hints.IsHint(syncErr) {
		return syncErr
	}

	plog.Info("Backup completed")
	return nil
}

func (r *Runner) ExecutePrune(ctx context.Context, absBasePath string, p *planner.PrunePlan) error {
	// Check for cancellation at the very beginning.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// save the execution timestamp
	timestampUTC := time.Now().UTC()

	// Run Preflight Validation, our absBasePath acts as target in prune mode
	if err := r.validator.Run(ctx, "", absBasePath, p.Preflight, timestampUTC); err != nil {
		return fmt.Errorf("preflight failed: %w", err)
	}

	// Acquire Lock on Target Directory.
	releaseLock, err := r.acquireTargetLock(ctx, absBasePath)
	if err != nil {
		return err // A real error occurred during lock acquisition.
	}
	if releaseLock == nil {
		return nil // Lock was already held, exit gracefully.
	}
	defer releaseLock()

	plog.Info("Starting prune", "base", absBasePath)

	// Standalone prune logic

	pruneIncremental := p.Mode == planner.Any || p.Mode == planner.Incremental
	pruneSnapshot := p.Mode == planner.Any || p.Mode == planner.Snapshot

	var pruneErr error
	if pruneIncremental && p.RetentionIncremental.Enabled {
		var toRetent []metafile.MetafileInfo
		toRetent, pruneErr = r.fetchBackups(ctx, absBasePath, p.PathsIncremental.RelArchivePathKey, p.PathsIncremental.BackupNamePrefix, []string{})
		if pruneErr != nil {
			return fmt.Errorf("fatal error during prune incremental: %w", pruneErr)
		}
		if pruneErr = r.retainer.Prune(ctx, absBasePath, toRetent, p.RetentionIncremental, timestampUTC); pruneErr != nil {
			if hints.IsHint(pruneErr) {
				plog.Debug("Incremental retention skipped", "reason", pruneErr)
			} else {
				return fmt.Errorf("fatal error during prune incremental: %w", pruneErr)
			}
		}
	}

	if pruneSnapshot && p.RetentionSnapshot.Enabled {
		var toRetent []metafile.MetafileInfo
		toRetent, pruneErr = r.fetchBackups(ctx, absBasePath, p.PathsSnapshot.RelArchivePathKey, p.PathsSnapshot.BackupNamePrefix, []string{})
		if pruneErr != nil {
			return fmt.Errorf("fatal error during prune snapshot: %w", pruneErr)
		}
		if pruneErr = r.retainer.Prune(ctx, absBasePath, toRetent, p.RetentionSnapshot, timestampUTC); pruneErr != nil {
			if hints.IsHint(pruneErr) {
				plog.Debug("Snapshot retention skipped", "reason", pruneErr)
			} else {
				return fmt.Errorf("fatal error during prune snapshot: %w", pruneErr)
			}
		}
	}
	plog.Info("Prune completed")
	return nil
}

func (r *Runner) ExecuteList(ctx context.Context, absBasePath string, p *planner.ListPlan) error {
	// Check for cancellation at the very beginning.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// save the execution timestamp
	timestampUTC := time.Now().UTC()

	// Run Preflight Validation, our absBasePath acts as target in prune mode
	if err := r.validator.Run(ctx, "", absBasePath, p.Preflight, timestampUTC); err != nil {
		return fmt.Errorf("preflight failed: %w", err)
	}

	// Acquire Lock on Target Directory.
	releaseLock, err := r.acquireTargetLock(ctx, absBasePath)
	if err != nil {
		return err // A real error occurred during lock acquisition.
	}
	if releaseLock == nil {
		return nil // Lock was already held, exit gracefully.
	}
	defer releaseLock()

	plog.Info("Starting list", "base", absBasePath)

	backups, err := r.ListBackups(ctx, absBasePath, p)
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	for _, b := range backups {
		msg := fmt.Sprintf("Backup from %s", b.Metadata.TimestampUTC.Local().Format(time.RFC1123))
		args := []interface{}{
			"uuid", b.Metadata.UUID,
			"name", filepath.Base(b.RelPathKey),
			"mode", b.Metadata.Mode,
			"timestampUTC", b.Metadata.TimestampUTC,
			"relPath", b.RelPathKey,
			"compressed", b.Metadata.IsCompressed,
		}

		if b.Metadata.IsCompressed {
			args = append(args, "compressionFormat", b.Metadata.CompressionFormat)
		}

		plog.Info(msg, args...)
	}

	// Standalone list logic
	plog.Info("List completed")
	return nil
}

func (r *Runner) ListBackups(ctx context.Context, absBasePath string, p *planner.ListPlan) ([]metafile.MetafileInfo, error) {
	var allBackups []metafile.MetafileInfo

	showIncremental := p.Mode == planner.Any || p.Mode == planner.Incremental
	showSnapshot := p.Mode == planner.Any || p.Mode == planner.Snapshot

	// 1. Incremental
	if showIncremental {
		if current, err := r.fetchBackup(absBasePath, p.PathsIncremental.RelCurrentPathKey); err == nil {
			allBackups = append(allBackups, current)
		}
		if archives, err := r.fetchBackups(ctx, absBasePath, p.PathsIncremental.RelArchivePathKey, p.PathsIncremental.BackupNamePrefix, nil); err == nil {
			allBackups = append(allBackups, archives...)
		}
	}

	// 2. Snapshot
	if showSnapshot {
		if current, err := r.fetchBackup(absBasePath, p.PathsSnapshot.RelCurrentPathKey); err == nil {
			allBackups = append(allBackups, current)
		}
		if archives, err := r.fetchBackups(ctx, absBasePath, p.PathsSnapshot.RelArchivePathKey, p.PathsSnapshot.BackupNamePrefix, nil); err == nil {
			allBackups = append(allBackups, archives...)
		}
	}

	// Sort by timestamp descending (newest first)
	sort.Slice(allBackups, func(i, j int) bool {
		return allBackups[i].Metadata.TimestampUTC.After(allBackups[j].Metadata.TimestampUTC)
	})

	return allBackups, nil
}

func (r *Runner) ExecuteRestore(ctx context.Context, absBasePath, backupName, absTargetPath string, p *planner.RestorePlan) error {
	// Check for cancellation at the very beginning.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if backupName == "" {
		return fmt.Errorf("backup name cannot be empty")
	}

	// save the execution timestamp
	timestampUTC := time.Now().UTC()

	// Run Preflight Validation, our absBasePath acts as source in restore mode
	if err := r.validator.Run(ctx, absBasePath, absTargetPath, p.Preflight, timestampUTC); err != nil {
		return fmt.Errorf("preflight failed: %w", err)
	}

	// Acquire Lock on Backup Repository (Source) to prevent concurrent modifications (like prune).
	releaseLock, err := r.acquireTargetLock(ctx, absBasePath)
	if err != nil {
		return err
	}
	if releaseLock == nil {
		return nil // Lock was already held
	}
	defer releaseLock()

	// --- Pre-Restore Hooks ---
	plog.Info("Running pre-restore hooks")
	if err := r.runHooks(ctx, p.PreRestoreHooks, "pre-restore", p.DryRun); err != nil {
		errMsg := "pre-restore hook failed"
		if errors.Is(err, context.Canceled) {
			errMsg = "pre-restore hook canceled"
		}
		return fmt.Errorf("%s: %w", errMsg, err)
	}

	// --- Post-Restore Hooks (deferred) ---
	defer func() {
		plog.Info("Running post-restore hooks")
		if err := r.runHooks(ctx, p.PostRestoreHooks, "post-restore", p.DryRun); err != nil {
			if errors.Is(err, context.Canceled) {
				plog.Info("post-restore hooks skipped due to cancellation.")
			} else {
				plog.Warn("post-restore hook failed", "error", err)
			}
		}
	}()

	var toRestore metafile.MetafileInfo
	var relContentPathKey string
	var lastErr error

	searchIncrementals := p.Mode == planner.Any || p.Mode == planner.Incremental
	searchSnapshots := p.Mode == planner.Any || p.Mode == planner.Snapshot

	// Helper to attempt fetch from a specific path configuration
	tryFetch := func(paths planner.PathKeys) (metafile.MetafileInfo, error) {
		var relBackupPathkey string
		if backupName == paths.RelCurrentPathKey || strings.EqualFold(strings.TrimSpace(backupName), "current") {
			relBackupPathkey = util.NormalizePath(paths.RelCurrentPathKey)
		} else {
			relBackupPathkey = util.NormalizePath(filepath.Join(paths.RelArchivePathKey, backupName))
		}
		return r.fetchBackup(absBasePath, relBackupPathkey)
	}

	// 1. Search Incremental
	if searchIncrementals {
		toRestore, lastErr = tryFetch(p.PathsIncremental)
		if lastErr == nil {
			relContentPathKey = p.PathsIncremental.RelContentPathKey
			plog.Debug("Found backup in incremental storage", "path", toRestore.RelPathKey)
		} else if !os.IsNotExist(lastErr) {
			return fmt.Errorf("failed to read backup metadata: %w", lastErr)
		}
	}

	// 2. Search Snapshot (if not found)
	if toRestore.RelPathKey == "" && searchSnapshots {
		toRestore, lastErr = tryFetch(p.PathsSnapshot)
		if lastErr == nil {
			relContentPathKey = p.PathsSnapshot.RelContentPathKey
			plog.Debug("Found backup in snapshot storage", "path", toRestore.RelPathKey)
		} else if !os.IsNotExist(lastErr) {
			return fmt.Errorf("failed to read backup metadata: %w", lastErr)
		}
	}

	if toRestore.RelPathKey == "" {
		return fmt.Errorf("backup %q not found. Please verify the backup-name is correct", backupName)
	}

	absBackupPath := util.DenormalizePath(filepath.Join(absBasePath, toRestore.RelPathKey))
	plog.Info("Starting restore", "backup", absBackupPath, "destination", absTargetPath)

	if toRestore.Metadata.IsCompressed {
		// Extract
		if err := r.compressor.Extract(ctx, absBasePath, toRestore, absTargetPath, p.Extraction, timestampUTC); err != nil {
			return fmt.Errorf("restore extraction failed: %w", err)
		}
	} else {
		// Sync (Flat file restore)
		// We sync FROM backup content TO absTargetPath.
		if err := r.syncer.Restore(ctx, absBasePath, relContentPathKey, toRestore, absTargetPath, p.Sync); err != nil {
			return fmt.Errorf("restore sync failed: %w", err)
		}
	}

	plog.Info("Restore completed")
	return nil
}

// acquireTargetLock ensures the target directory exists and acquires a file lock within it.
// It returns a release function that must be called to unlock the directory.
func (r *Runner) acquireTargetLock(ctx context.Context, absBasePath string) (func(), error) {
	appID := fmt.Sprintf("pgl-backup:%s", absBasePath)

	plog.Debug("Attempting to acquire lock", "path", absBasePath)
	lock, err := lockfile.Acquire(ctx, absBasePath, appID)
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
// The relPathExclusionKeys are Relative to the absBasePath, and filtered internally.
func (r *Runner) fetchBackups(ctx context.Context, absBasePath, relArchivePathKey, backupNamePrefix string, relPathExclusionKeys []string) ([]metafile.MetafileInfo, error) {

	absArchivePath := util.DenormalizePath(filepath.Join(absBasePath, relArchivePathKey))
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
		if !entry.IsDir() || !strings.HasPrefix(dirName, backupNamePrefix) {
			continue
		}

		// Check if excluded
		if _, ok := excludedDirsInArchive[dirName]; ok {
			continue
		}

		relBackupPathkey := util.NormalizePath(filepath.Join(relArchivePathKey, dirName))
		foundBackup, err := r.fetchBackup(absBasePath, relBackupPathkey)
		if err != nil {
			plog.Warn("Skipping backup directory; cannot read metadata", "directory", dirName, "reason", err)
			continue
		}
		foundBackups = append(foundBackups, foundBackup)
	}
	return foundBackups, nil
}

func (r *Runner) fetchBackup(absBasePath, relPathKey string) (metafile.MetafileInfo, error) {
	absBackupPath := util.DenormalizePath(filepath.Join(absBasePath, relPathKey))
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
