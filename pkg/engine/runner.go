package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/hints"
	"github.com/paulschiretz/pgl-backup/pkg/hook"
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

	// Hooks
	if err := r.runPreHook(ctx, "backup", p.HookRunner, timestampUTC); err != nil {
		// Pre-hook errors are fatal.
		errMsg := "pre-backup hook failed"
		if errors.Is(err, context.Canceled) {
			errMsg = "pre-backup hook canceled"
		}
		return fmt.Errorf("%s: %w", errMsg, err)
	}
	defer func() {
		if err := r.runPostHook(ctx, "backup", p.HookRunner, timestampUTC); err != nil {
			// Post-hook errors are non-fatal to the overall backup status, but should be logged.
			errMsg := "post-backup hook failed"
			if errors.Is(err, context.Canceled) {
				plog.Info("post-backup hooks skipped due to cancellation.")
				return
			}
			plog.Warn(errMsg, "error", err)
		}
	}()

	plog.Info("Starting backup", "base", absBasePath, "source", absSourcePath, "mode", p.Mode)

	// Dispatch Backup Run
	switch p.Mode {
	case planner.Incremental:
		return r.executeIncrementalBackup(ctx, absBasePath, absSourcePath, p, timestampUTC)
	case planner.Snapshot:
		return r.executeSnapshotBackup(ctx, absBasePath, absSourcePath, p, timestampUTC)
	}

	plog.Info("Backup completed")
	return nil
}

func (r *Runner) executeIncrementalBackup(ctx context.Context, absBasePath, absSourcePath string, p *planner.BackupPlan, timestampUTC time.Time) error {

	// This function follows a "fail-forward" strategy. It attempts to execute all steps
	// (Archive, Sync, Retention, Compression) in sequence. If a non-critical step like
	// Retention or Compression fails, the error is logged, but the process continues.
	// The overall backup is only considered a failure if a critical step (Sync or Archive) fails.
	// We always want to complete the whole flow (Archive -> Sync -> Retention -> Compression).
	// Each step handles invalid inputs internally (e.g. empty paths) and returns early if needed,
	// or logs errors if they are non-fatal.
	var syncResult, archiveResult metafile.MetafileInfo

	// 1. Archive
	var archiveErr error
	if p.Archive.Enabled {
		var toArchive metafile.MetafileInfo
		toArchive, archiveErr = r.fetchBackup(absBasePath, p.Paths.RelCurrentPathKey)
		if archiveErr != nil {
			if os.IsNotExist(archiveErr) {
				// IMPORTANT: If we never did a backup before our archive does not exist! This is not an error!
				archiveErr = nil
			} else {
				archiveErr = fmt.Errorf("error reading backup to archive: %w", archiveErr)
			}
		} else {
			archiveResult, archiveErr = r.runArchive(ctx, absBasePath, p.Paths, p.Archive, toArchive, timestampUTC)
			if archiveErr != nil {
				if errors.Is(archiveErr, context.Canceled) {
					return archiveErr
				}
				// We still might need to run another step, so just log and continue
				plog.Error("Archive failed", "error", archiveErr)
			}
		}
	}

	// 2. Sync
	var syncErr error
	if p.Sync.Enabled {
		syncResult, syncErr = r.runSync(ctx, absBasePath, absSourcePath, p.Paths, p.Sync, timestampUTC)
		if syncErr != nil {
			if errors.Is(syncErr, context.Canceled) {
				return syncErr
			}
			// We still might need to run another step, so just log and continue
			plog.Error("Sync failed", "error", syncErr)
		}
	}

	// 3. Retention
	var pruneErr error
	if p.Retention.Enabled {
		var toExclude []metafile.MetafileInfo
		// Add our just created sync/archive results to the exclusion list for prune so they are never pruned
		// NOTE: This is needed in case our retention is enabled but 0,0,0,0,0 and we just created a snapshot. (we avoid deleting it rightaway)
		if archiveResult.RelPathKey != "" {
			toExclude = append(toExclude, archiveResult)
		}
		if syncResult.RelPathKey != "" {
			toExclude = append(toExclude, syncResult)
		}

		pruneErr = r.runPrune(ctx, absBasePath, p.Paths, p.Retention, toExclude, timestampUTC)
		if pruneErr != nil {
			if errors.Is(pruneErr, context.Canceled) {
				return pruneErr
			}
			// We still might need to run another step, so just log and continue
			plog.Error("Prune failed", "error", pruneErr)
		}
	}

	// 4. Compression
	var compressErr error
	if p.Compression.Enabled {
		compressErr = r.runCompress(ctx, absBasePath, p.Paths, p.Compression, archiveResult, timestampUTC)
		if compressErr != nil {
			if errors.Is(compressErr, context.Canceled) {
				return compressErr
			}
			// We still might need to run another step, so just log and continue
			plog.Error("Compress failed", "error", compressErr)
		}
	}

	// If Sync or Archive failed, the run is considered a failure.
	if syncErr != nil && !hints.IsHint(syncErr) {
		return syncErr
	}
	if archiveErr != nil && !hints.IsHint(archiveErr) {
		return archiveErr
	}
	return nil
}

func (r *Runner) executeSnapshotBackup(ctx context.Context, absBasePath, absSourcePath string, p *planner.BackupPlan, timestampUTC time.Time) error {
	// This function follows a "fail-forward" strategy. It attempts to execute all steps
	// (Archive, Sync, Retention, Compression) in sequence. If a non-critical step like
	// Retention or Compression fails, the error is logged, but the process continues.
	// The overall backup is only considered a failure if a critical step (Sync or Archive) fails.
	// We always want to complete the whole flow (Sync -> Archive -> Retention -> Compression).
	// Each step handles invalid inputs internally (e.g. empty paths) and returns early if needed,
	// or logs errors if they are non-fatal.
	var syncResult, archiveResult metafile.MetafileInfo

	// 1. Sync
	var syncErr error
	if p.Sync.Enabled {
		syncResult, syncErr = r.runSync(ctx, absBasePath, absSourcePath, p.Paths, p.Sync, timestampUTC)
		if syncErr != nil {
			if errors.Is(syncErr, context.Canceled) {
				return syncErr
			}
			// We still might need to run another step, so just log and continue
			plog.Error("Sync failed", "error", syncErr)
		}
	}

	// 2. Archive
	var archiveErr error
	if p.Archive.Enabled {
		archiveResult, archiveErr = r.runArchive(ctx, absBasePath, p.Paths, p.Archive, syncResult, timestampUTC)
		if archiveErr != nil {
			if errors.Is(archiveErr, context.Canceled) {
				return archiveErr
			}
			// We still might need to run another step, so just log and continue
			plog.Error("Archive failed", "error", archiveErr)
		}
	}

	// 3. Retention
	var pruneErr error
	if p.Retention.Enabled {
		var toExclude []metafile.MetafileInfo
		// Add our just created sync/archive results to the exclusion list for prune so they are never pruned
		// NOTE: This is needed in case our retention is enabled but 0,0,0,0,0 and we just created a snapshot. (we avoid deleting it rightaway)
		if archiveResult.RelPathKey != "" {
			toExclude = append(toExclude, archiveResult)
		}
		if syncResult.RelPathKey != "" {
			toExclude = append(toExclude, syncResult)
		}

		pruneErr = r.runPrune(ctx, absBasePath, p.Paths, p.Retention, toExclude, timestampUTC)
		if pruneErr != nil {
			if errors.Is(pruneErr, context.Canceled) {
				return pruneErr
			}
			// We still might need to run another step, so just log and continue
			plog.Error("Prune failed", "error", pruneErr)
		}
	}

	// 4. Compression
	var compressErr error
	if p.Compression.Enabled {
		compressErr = r.runCompress(ctx, absBasePath, p.Paths, p.Compression, archiveResult, timestampUTC)
		if compressErr != nil {
			if errors.Is(compressErr, context.Canceled) {
				return compressErr
			}
			// We still might need to run another step, so just log and continue
			plog.Error("Compress failed", "error", compressErr)
		}
	}

	if syncErr != nil && !hints.IsHint(syncErr) {
		return syncErr
	}
	if archiveErr != nil && !hints.IsHint(archiveErr) {
		return archiveErr
	}
	return nil
}

func (r *Runner) ExecutePrune(ctx context.Context, absBasePath string, p *planner.PrunePlan) error {
	// We always want to complete the whole flow (Incremental -> Snapshot).
	// Each step handles invalid inputs internally (e.g. empty paths) and returns early if needed,
	// or logs errors if they are non-fatal.

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

	var pruneIncErr, pruneSnapErr error

	if pruneIncremental && p.RetentionIncremental.Enabled {
		pruneIncErr = r.runPrune(ctx, absBasePath, p.PathsIncremental, p.RetentionIncremental, []metafile.MetafileInfo{}, timestampUTC)
		if pruneIncErr != nil {
			if errors.Is(pruneIncErr, context.Canceled) {
				return pruneIncErr
			}
			plog.Error("Prune incremental failed", "error", pruneIncErr)
		}
	}

	if pruneSnapshot && p.RetentionSnapshot.Enabled {
		pruneSnapErr = r.runPrune(ctx, absBasePath, p.PathsSnapshot, p.RetentionSnapshot, []metafile.MetafileInfo{}, timestampUTC)
		if pruneSnapErr != nil {
			if errors.Is(pruneSnapErr, context.Canceled) {
				return pruneSnapErr
			}
			plog.Error("Prune snapshot failed", "error", pruneSnapErr)
		}
	}

	if pruneIncErr != nil {
		return fmt.Errorf("fatal error during prune incremental: %w", pruneIncErr)
	}
	if pruneSnapErr != nil {
		return fmt.Errorf("fatal error during prune snapshot: %w", pruneSnapErr)
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
		msg := fmt.Sprintf("Backup found from %s", b.Metadata.TimestampUTC.Local().Format(time.RFC1123))
		args := []any{
			"uuid", b.Metadata.UUID,
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
		if archives, err := r.fetchBackups(ctx, absBasePath, p.PathsIncremental.RelArchivePathKey, p.PathsIncremental.ArchiveEntryPrefix, nil); err == nil {
			allBackups = append(allBackups, archives...)
		}
	}

	// 2. Snapshot
	if showSnapshot {
		if current, err := r.fetchBackup(absBasePath, p.PathsSnapshot.RelCurrentPathKey); err == nil {
			allBackups = append(allBackups, current)
		}
		if archives, err := r.fetchBackups(ctx, absBasePath, p.PathsSnapshot.RelArchivePathKey, p.PathsSnapshot.ArchiveEntryPrefix, nil); err == nil {
			allBackups = append(allBackups, archives...)
		}
	}

	// Sort by timestamp
	slices.SortFunc(allBackups, func(a, b metafile.MetafileInfo) int {
		if p.SortOrder == planner.Asc {
			return a.Metadata.TimestampUTC.Compare(b.Metadata.TimestampUTC)
		}
		return b.Metadata.TimestampUTC.Compare(a.Metadata.TimestampUTC)
	})

	return allBackups, nil
}

func (r *Runner) ExecuteRestore(ctx context.Context, absBasePath, uuid, absTargetPath string, p *planner.RestorePlan) error {
	// Check for cancellation at the very beginning.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if uuid == "" {
		return fmt.Errorf("backup UUID cannot be empty")
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

	// Hooks
	if err := r.runPreHook(ctx, "restore", p.HookRunner, timestampUTC); err != nil {
		errMsg := "pre-restore hook failed"
		if errors.Is(err, context.Canceled) {
			errMsg = "pre-restore hook canceled"
		}
		// Pre-hook errors are fatal.
		return fmt.Errorf("%s: %w", errMsg, err)
	}
	defer func() {
		if err := r.runPostHook(ctx, "restore", p.HookRunner, timestampUTC); err != nil {
			// Post-hook errors are fatal.
			errMsg := "post-restore hook failed"
			if errors.Is(err, context.Canceled) {
				errMsg = "post-restore hook canceled"
			}
			plog.Error(errMsg, "error", err)
		}
	}()

	var toRestore metafile.MetafileInfo
	var relContentPathKey string

	searchIncrementals := p.Mode == planner.Any || p.Mode == planner.Incremental
	searchSnapshots := p.Mode == planner.Any || p.Mode == planner.Snapshot

	// Helper to scan a specific path configuration for the UUID
	scanLocation := func(paths planner.PathKeys) error {
		// 1. Check Current
		if current, err := r.fetchBackup(absBasePath, paths.RelCurrentPathKey); err == nil {
			if current.Metadata.UUID == uuid {
				toRestore = current
				relContentPathKey = paths.RelContentPathKey
				return nil // Found
			}
		}

		// 2. Check Archives
		archives, err := r.fetchBackups(ctx, absBasePath, paths.RelArchivePathKey, paths.ArchiveEntryPrefix, nil)
		if err != nil {
			return err
		}
		for _, b := range archives {
			if b.Metadata.UUID == uuid {
				toRestore = b
				relContentPathKey = paths.RelContentPathKey
				return nil // Found
			}
		}
		return nil
	}

	// 1. Search Incremental
	if searchIncrementals && toRestore.RelPathKey == "" {
		_ = scanLocation(p.PathsIncremental)
	}
	// 2. Search Snapshot
	if searchSnapshots && toRestore.RelPathKey == "" {
		_ = scanLocation(p.PathsSnapshot)
	}

	if toRestore.RelPathKey == "" {
		return fmt.Errorf("backup with UUID %q not found", uuid)
	}

	absBackupPath := util.DenormalizedAbsPath(absBasePath, toRestore.RelPathKey)
	plog.Info("Starting restore", "backup", absBackupPath, "destination", absTargetPath)

	if toRestore.Metadata.IsCompressed {
		if err := r.runExtract(ctx, absBasePath, absTargetPath, p.Extraction, toRestore, timestampUTC); err != nil {
			return fmt.Errorf("restore extraction failed: %w", err)
		}
	} else {
		// Sync (Flat file restore)
		// We sync FROM backup content TO absTargetPath.
		if err := r.runRestore(ctx, absBasePath, relContentPathKey, absTargetPath, p.Sync, toRestore, timestampUTC); err != nil {
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
		if lockErr, ok := errors.AsType[*lockfile.ErrLockActive](err); ok {
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
func (r *Runner) fetchBackups(ctx context.Context, absBasePath, relArchivePathKey, archiveEntryPrefix string, relPathExclusionKeys []string) ([]metafile.MetafileInfo, error) {

	absArchivePath := util.DenormalizedAbsPath(absBasePath, relArchivePathKey)
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
		// Since we are dealing with normalized paths, we can use string operations
		// instead of filepath.Rel to avoid OS-specific separator issues and overhead.
		prefix := relArchivePathKey + "/"
		if strings.HasPrefix(relPathExclusionKey, prefix) {
			relPathInArchive := strings.TrimPrefix(relPathExclusionKey, prefix)
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
		// Ensure we are matching against the directory name specifically.
		// We use path.Base to handle cases where the prefix might inadvertently contain
		// path separators or trailing slashes (e.g. "backup/"), ensuring we match the name.
		// We check for empty string explicitly because path.Base("") returns ".".
		matchPrefix := archiveEntryPrefix
		if matchPrefix != "" {
			matchPrefix = path.Base(matchPrefix)
		}

		if !entry.IsDir() || !strings.HasPrefix(dirName, matchPrefix) {
			continue
		}

		// Check if excluded
		if _, ok := excludedDirsInArchive[dirName]; ok {
			continue
		}

		// Since relArchivePathKey is already normalized (forward slashes), we can use path.Join
		// to join it with the directory name without OS-specific separator issues.
		relBackupPathkey := path.Join(relArchivePathKey, dirName)
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
	absBackupPath := util.DenormalizedAbsPath(absBasePath, relPathKey)
	metadata, err := metafile.Read(absBackupPath)
	if err != nil {
		return metafile.MetafileInfo{}, err
	}
	return metafile.MetafileInfo{RelPathKey: relPathKey, Metadata: metadata}, nil
}

func (r *Runner) runArchive(ctx context.Context, absBasePath string, paths planner.PathKeys, plan *patharchive.Plan, toArchive metafile.MetafileInfo, timestampUTC time.Time) (metafile.MetafileInfo, error) {

	if !plan.Enabled {
		return metafile.MetafileInfo{}, nil
	}

	result, err := r.archiver.Archive(ctx, absBasePath, paths.RelArchivePathKey, paths.ArchiveEntryPrefix, toArchive, plan, timestampUTC)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return metafile.MetafileInfo{}, err
		}
		if hints.IsHint(err) {
			plog.Debug("Archiving skipped", "reason", err)
			return metafile.MetafileInfo{}, nil
		}
		return metafile.MetafileInfo{}, fmt.Errorf("error during archive: %w", err)
	}

	// Consistency check
	if result.RelPathKey == "" {
		return metafile.MetafileInfo{}, fmt.Errorf("archive succeeded but result is empty")
	}
	return result, nil
}

func (r *Runner) runSync(ctx context.Context, absBasePath, absSourcePath string, paths planner.PathKeys, plan *pathsync.Plan, timestampUTC time.Time) (metafile.MetafileInfo, error) {
	result, err := r.syncer.Sync(ctx, absBasePath, absSourcePath, paths.RelCurrentPathKey, paths.RelContentPathKey, plan, timestampUTC)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return metafile.MetafileInfo{}, err
		}

		if hints.IsHint(err) {
			plog.Debug("Sync skipped", "reason", err)
			return metafile.MetafileInfo{}, nil
		}
		return metafile.MetafileInfo{}, fmt.Errorf("error during sync: %w", err)
	}

	// Consistency check
	if result.RelPathKey == "" {
		return metafile.MetafileInfo{}, fmt.Errorf("sync succeeded but result is empty")
	}

	// Write the metafile using the info populated by Sync.
	absTargetCurrentPath := util.DenormalizedAbsPath(absBasePath, result.RelPathKey)
	if plan.DryRun {
		plog.Info("[DRY RUN] Would write metafile", "directory", absTargetCurrentPath)
	} else {
		if err := metafile.Write(absTargetCurrentPath, &result.Metadata); err != nil {
			return metafile.MetafileInfo{}, fmt.Errorf("sync failed to write metafile: %w", err)
		}
	}
	return result, nil
}

func (r *Runner) runPrune(ctx context.Context, absBasePath string, paths planner.PathKeys, plan *pathretention.Plan, toExclude []metafile.MetafileInfo, timestampUTC time.Time) error {

	var relPathExclusionKeys []string
	for _, item := range toExclude {
		if item.RelPathKey != "" {
			relPathExclusionKeys = append(relPathExclusionKeys, item.RelPathKey)
		}
	}

	toRetent, err := r.fetchBackups(ctx, absBasePath, paths.RelArchivePathKey, paths.ArchiveEntryPrefix, relPathExclusionKeys)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return fmt.Errorf("error reading backups for prune: %w", err)
	}

	if err = r.retainer.Prune(ctx, absBasePath, toRetent, plan, timestampUTC); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		if hints.IsHint(err) {
			plog.Debug("Prune skipped", "reason", err)
			return nil
		}
		return fmt.Errorf("error during prune: %w", err)
	}
	return nil
}

func (r *Runner) runCompress(ctx context.Context, absBasePath string, paths planner.PathKeys, plan *pathcompression.CompressPlan,
	toCompress metafile.MetafileInfo, timestampUTC time.Time) error {

	if err := r.compressor.Compress(ctx, absBasePath, paths.RelContentPathKey, toCompress, plan, timestampUTC); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		if hints.IsHint(err) {
			plog.Debug("Compression skipped", "reason", err)
			return nil
		}
		return fmt.Errorf("error during compress: %w", err)
	}
	return nil
}

func (r *Runner) runRestore(ctx context.Context, absBasePath, relContentPathKey, absTargetPath string, plan *pathsync.Plan,
	toRestore metafile.MetafileInfo, timestampUTC time.Time) error {

	if err := r.syncer.Restore(ctx, absBasePath, relContentPathKey, toRestore, absTargetPath, plan, timestampUTC); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		if hints.IsHint(err) {
			plog.Debug("Restore skipped", "reason", err)
			return nil
		}
		return fmt.Errorf("error during restore: %w", err)
	}
	return nil
}

func (r *Runner) runExtract(ctx context.Context, absBasePath, absTargetPath string, plan *pathcompression.ExtractPlan,
	toExtract metafile.MetafileInfo, timestampUTC time.Time) error {

	if err := r.compressor.Extract(ctx, absBasePath, toExtract, absTargetPath, plan, timestampUTC); err != nil {

		if errors.Is(err, context.Canceled) {
			return err
		}
		if hints.IsHint(err) {
			plog.Debug("Extraction skipped", "reason", err)
			return nil
		}
		return fmt.Errorf("error during extraction: %w", err)
	}
	return nil
}

func (r *Runner) runPreHook(ctx context.Context, hookName string, plan *hook.Plan, timestampUTC time.Time) error {
	if err := r.hookRunner.RunPreHook(ctx, hookName, plan, timestampUTC); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}

		if hints.IsHint(err) {
			plog.Debug("Pre-Hook skipped", "reason", err)
			return nil
		}
		return fmt.Errorf("error during pre-hook: %w", err)
	}
	return nil
}

func (r *Runner) runPostHook(ctx context.Context, hookName string, plan *hook.Plan, timestampUTC time.Time) error {
	if err := r.hookRunner.RunPostHook(ctx, hookName, plan, timestampUTC); err != nil {
		if errors.Is(err, context.Canceled) {
			return err // Pass through cancellation and hints
		}

		if hints.IsHint(err) {
			plog.Debug("Post-Hook skipped", "reason", err)
			return nil
		}
		return fmt.Errorf("error during post-hook: %w", err)
	}
	return nil
}
