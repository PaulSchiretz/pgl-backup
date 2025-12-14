package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/filelock"
	"pixelgardenlabs.io/pgl-backup/pkg/patharchive"
	"pixelgardenlabs.io/pgl-backup/pkg/pathsync"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
	"pixelgardenlabs.io/pgl-backup/pkg/preflight"
	"pixelgardenlabs.io/pgl-backup/pkg/util"
)

// --- ARCHITECTURAL OVERVIEW: Archive vs. Retention Time Handling ---
//
// This engine employs two distinct time-handling strategies for creating and deleting backups,
// each designed to address a separate user concern:
//
// 1. Archive (Snapshot Creation) - Predictable Creation
//    - Goal: To honor the user's configured `ArchiveInterval` as literally as possible.
//    - Logic: The `shouldArchive` function calculates time-based "bucketing" based on the
//      **local system's midnight** for day-or-longer intervals. This gives the user direct,
//      predictable control over the *frequency* of new archives, anchored to their local day.
//
// 2. Retention (Snapshot Deletion) - Consistent History
//    - Goal: To organize the backup history into intuitive, calendar-based slots for cleanup.
//    - Logic: The `determineBackupsToKeep` function uses fixed calendar concepts (e.g.,
//      `time.ISOWeek()`, `time.Format("2006-01-02")`) by examining the **UTC time** stored in
//      the backup's metadata (`BackupTime`). This ensures that "keep one backup from last week"
//      always refers to a standard calendar week as defined in the UTC timezone, providing a
//      clean, portable history.
//
// By decoupling these two concepts, the system provides the best of both worlds: a predictable
// creation schedule based on local time duration, and a clean, consistent historical view
// based on standard UTC calendar periods.

// Constants for time formats used in retention bucketing
const (
	hourFormat  = "2006-01-02-15" // YYYY-MM-DD-HH
	dayFormat   = "2006-01-02"    // YYYY-MM-DD
	weekFormat  = "%d-%d"         // Sprintf format for "YYYY-WW" using year and ISO week number (weeks start on Monday). Go's time package does not have a layout code (like WW). The only way to get the ISO week is to call the time.ISOWeek() method
	monthFormat = "2006-01"       // YYYY-MM
	yearFormat  = "2006"          // YYYY
)

// backupInfo holds the parsed time and name of a backup directory.
type backupInfo struct {
	Time time.Time
	Name string
}

// runMetadata holds metadata for a single execution of the backup engine.
type runMetadata struct {
	Version    string    `json:"version"`
	BackupTime time.Time `json:"backupTime"`
	Mode       string    `json:"mode"`
	Source     string    `json:"source"`
}

// runState holds the mutable state for a single execution of the backup engine.
// This makes the Engine itself stateless and safe for concurrent use if needed.
type runState struct {
	target       string
	timestampUTC time.Time
}

// Engine orchestrates the entire backup process.
type Engine struct {
	config   config.Config
	version  string
	syncer   pathsync.Syncer
	archiver patharchive.Archiver
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
		hookCommandExecutor: exec.CommandContext, // Default to the real implementation.
	}
}

// acquireTargetLock ensures the target directory exists and acquires a file lock within it.
// It returns a release function that must be called to unlock the directory.
func (e *Engine) acquireTargetLock(ctx context.Context) (func(), error) {
	lockFilePath := filepath.Join(e.config.Paths.TargetBase, config.LockFileName)
	appID := fmt.Sprintf("pgl-backup:%s", e.config.Paths.Source)

	plog.Debug("Attempting to acquire lock", "path", lockFilePath)
	lock, err := filelock.Acquire(ctx, lockFilePath, appID)
	if err != nil {
		var lockErr *filelock.ErrLockActive
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

	plog.Info("Initializing new backup target")

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
	currentRun := &runState{timestampUTC: time.Now().UTC()}

	// --- 1. Pre-backup tasks (archive) and destination calculation ---
	if err := e.prepareDestination(ctx, currentRun); err != nil {
		return err
	}

	// Log a single, comprehensive message about the upcoming backup process.
	logMsg := "Starting backup process"
	if e.config.DryRun {
		logMsg = "Starting backup process (DRY RUN)"
	}
	plog.Info(logMsg,
		"source", e.config.Paths.Source,
		"destination", currentRun.target,
		"mode", e.config.Mode,
	)

	// --- 2. Perform the backup ---
	if err := e.performSync(ctx, currentRun); err != nil {
		return fmt.Errorf("fatal backup error during sync: %w", err) // This is a fatal error, so we return it.
	}

	plog.Info("Backup operation completed.")

	// --- 3. Clean up outdated backups (archives and snapshots) ---
	// We apply all enabled retention policies regardless of the current backup mode.
	// This ensures the backup target is always kept in a consistent state according
	// to the user's configuration and prevents "retention debt" if one mode is
	// run less frequently than another.

	// Apply retention for incremental archives, if enabled.
	if e.config.IncrementalRetentionPolicy.Enabled {
		archivesDir := filepath.Join(e.config.Paths.TargetBase, e.config.Paths.ArchivesSubDir)
		incrementalDirName := e.config.Paths.IncrementalSubDir
		if err := e.applyRetentionFor(ctx, "incremental", archivesDir, e.config.IncrementalRetentionPolicy, incrementalDirName); err != nil {
			plog.Warn("Error applying incremental retention policy", "error", err)
		}
	}

	// Apply retention for snapshots, if enabled.
	if e.config.SnapshotRetentionPolicy.Enabled {
		snapshotsDir := filepath.Join(e.config.Paths.TargetBase, e.config.Paths.SnapshotsSubDir)
		if err := e.applyRetentionFor(ctx, "snapshot", snapshotsDir, e.config.SnapshotRetentionPolicy, ""); err != nil {
			plog.Warn("Error applying snapshot retention policy", "error", err)
		}
	}

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

// prepareDestination calculates the target directory for the backup, performing
// a archive if necessary for incremental backups.
func (e *Engine) prepareDestination(ctx context.Context, currentRun *runState) error {
	if e.config.Mode == config.SnapshotMode {
		// SNAPSHOT MODE
		//
		// The directory name must remain uniquely based on UTC time to avoid DST conflicts,
		// but we add the user's local offset to make the timezone clear to the user.
		timestamp := config.FormatTimestampWithOffset(currentRun.timestampUTC)
		backupDirName := e.config.Naming.Prefix + timestamp
		snapshotsSubDir := filepath.Join(e.config.Paths.TargetBase, e.config.Paths.SnapshotsSubDir)
		if !e.config.DryRun {
			os.MkdirAll(snapshotsSubDir, util.UserWritableDirPerms)
		}
		currentRun.target = filepath.Join(snapshotsSubDir, backupDirName)
	} else {
		// INCREMENTAL MODE
		if err := e.performArchiving(ctx, currentRun); err != nil {
			return fmt.Errorf("error during backup archiving: %w", err)
		}
		incrementalDir := filepath.Join(e.config.Paths.TargetBase, e.config.Paths.IncrementalSubDir)
		currentRun.target = incrementalDir
	}
	return nil
}

// performSync is the main entry point for synchronization.
func (e *Engine) performSync(ctx context.Context, currentRun *runState) error {
	source := e.config.Paths.Source
	destination := currentRun.target
	mirror := e.config.Mode == config.IncrementalMode

	// If configured, append the source's base directory name to the destination path.
	if e.config.Paths.PreserveSourceDirectoryName {
		var nameToAppend string

		// Check if the path is a root path (e.g., "/" or "C:\")
		if filepath.Dir(source) == source {
			// Handle Windows Drive Roots (e.g., "D:\") -> "D"
			vol := filepath.VolumeName(source)
			// On Windows, for "C:\", VolumeName is "C:", we trim the colon.
			// On Unix, for "/", VolumeName is "", so nameToAppend remains empty.
			if vol != "" && strings.HasSuffix(vol, ":") {
				nameToAppend = strings.TrimSuffix(vol, ":")
			}
		} else {
			// Standard folder
			nameToAppend = filepath.Base(source)
		}

		// Append if valid
		if nameToAppend != "" && nameToAppend != "." && nameToAppend != string(filepath.Separator) {
			destination = filepath.Join(destination, nameToAppend)
		}
	}

	// Sync and check for errors after attempting the sync.
	if syncErr := e.syncer.Sync(ctx, source, destination, mirror, e.config.Paths.ExcludeFiles(), e.config.Paths.ExcludeDirs(), e.config.Metrics); syncErr != nil {
		return fmt.Errorf("sync failed: %w", syncErr)
	}

	// If the sync was successful, write the metafile for retention purposes.
	return writeBackupMetafile(currentRun.target, e.version, e.config.Mode.String(), source, currentRun.timestampUTC, e.config.DryRun)
}

// performArchiving is the main entry point for archive updates.
func (e *Engine) performArchiving(ctx context.Context, currentRun *runState) error {
	incrementalDirName := e.config.Paths.IncrementalSubDir
	currentBackupPath := filepath.Join(e.config.Paths.TargetBase, incrementalDirName)

	metaData, err := readBackupMetafile(currentBackupPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("could not read previous backup metadata for archive check: %w", err)
	}

	// Only perform archiving if a previous backup exists.
	if metaData != nil {
		// Use the precise time from the file content, not the file's modification time.
		lastBackupTime := metaData.BackupTime
		if err := e.archiver.Archive(ctx, currentBackupPath, lastBackupTime, currentRun.timestampUTC); err != nil {
			return fmt.Errorf("error during backup archiving: %w", err)
		}
	}
	return nil
}

// applyRetentionFor scans a given directory and deletes backups
// that are no longer needed according to the configured retention policy.
func (e *Engine) applyRetentionFor(ctx context.Context, policyTitle string, targetDir string, retentionPolicy config.BackupRetentionPolicyConfig, excludeDir string) error {
	if retentionPolicy.Hours <= 0 && retentionPolicy.Days <= 0 && retentionPolicy.Weeks <= 0 && retentionPolicy.Months <= 0 && retentionPolicy.Years <= 0 {
		plog.Debug(fmt.Sprintf("Retention policy for %s is disabled (all values are zero). Skipping cleanup.", policyTitle))
		return nil
	}

	plog.Info(fmt.Sprintf("Cleaning outdated %s backups", policyTitle))
	plog.Debug("Applying retention policy", "policy", policyTitle, "directory", targetDir)
	// --- 1. Get a sorted list of all valid, historical backups ---
	allBackups, err := e.fetchSortedBackups(ctx, targetDir, excludeDir)
	if err != nil {
		return err
	}
	if len(allBackups) == 0 {
		plog.Debug(fmt.Sprintf("No %s backups found to apply retention to", policyTitle))
		return nil
	}

	// --- 2. Apply retention rules to find which backups to keep ---
	backupsToKeep := e.determineBackupsToKeep(allBackups, retentionPolicy, policyTitle)

	// --- 3. Collect all backups that are not in our final `backupsToKeep` set ---
	var dirsToDelete []string
	for _, backup := range allBackups {
		dirName := backup.Name
		if _, shouldKeep := backupsToKeep[dirName]; !shouldKeep {
			dirsToDelete = append(dirsToDelete, filepath.Join(targetDir, dirName))
		}
	}

	if len(dirsToDelete) == 0 && !e.config.DryRun {
		plog.Debug("No outdated backups to delete", "policy", policyTitle)
		return nil
	}

	plog.Info("Preparing to delete outdated backups", "policy", policyTitle, "count", len(dirsToDelete))

	// --- 4. Delete backups in parallel using a worker pool ---
	// This is especially effective for network drives where latency is a factor.
	numWorkers := e.config.Engine.Performance.DeleteWorkers // Use the configured number of workers.
	var wg sync.WaitGroup
	deleteDirTasksChan := make(chan string, len(dirsToDelete))

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for dirToDelete := range deleteDirTasksChan {
				// Check for cancellation before each deletion.
				select {
				case <-ctx.Done():
					// Don't process any more jobs if context is cancelled.
					return
				default:
				}

				plog.Debug("Deleting outdated backup", "policy", policyTitle, "path", dirToDelete, "worker", workerID)
				if e.config.DryRun {
					plog.Info("[DRY RUN] Would delete directory", "policy", policyTitle, "path", dirToDelete)
					continue
				}
				if err := os.RemoveAll(dirToDelete); err != nil {
					plog.Warn("Failed to delete outdated backup directory", "policy", policyTitle, "path", dirToDelete, "error", err)
				}
			}
		}(i + 1)
	}

	// Feed the jobs channel with all the directories to be deleted.
	for _, dir := range dirsToDelete {
		select {
		case <-ctx.Done():
			// If context is cancelled while feeding jobs, stop sending more.
			plog.Info("Cancellation received, stopping deletion process.")
			close(deleteDirTasksChan) // Close channel to unblock any waiting workers.
			return ctx.Err()
		case deleteDirTasksChan <- dir:
		}
	}
	close(deleteDirTasksChan) // All jobs have been sent.

	wg.Wait()

	return nil
}

// determineBackupsToKeep applies the retention policy to a sorted list of backups.
func (e *Engine) determineBackupsToKeep(allBackups []backupInfo, retentionPolicy config.BackupRetentionPolicyConfig, policyTitle string) map[string]bool {
	backupsToKeep := make(map[string]bool)

	// Keep track of which periods we've already saved a backup for.
	savedHourly := make(map[string]bool)
	savedDaily := make(map[string]bool)
	savedWeekly := make(map[string]bool)
	savedMonthly := make(map[string]bool)
	savedYearly := make(map[string]bool)

	for _, b := range allBackups {
		// The rules are processed from shortest to longest duration.
		// Once a backup is kept, it's not considered for longer-duration rules.
		// This "promotes" a backup to the highest-frequency slot it qualifies for.

		// Rule: Keep N hourly backups
		hourKey := b.Time.Format(hourFormat)
		if retentionPolicy.Hours > 0 && len(savedHourly) < retentionPolicy.Hours && !savedHourly[hourKey] {
			backupsToKeep[b.Name] = true
			savedHourly[hourKey] = true
			continue // Promoted to hourly, skip other rules
		}

		// Rule: Keep N daily backups
		dayKey := b.Time.Format(dayFormat)
		if retentionPolicy.Days > 0 && len(savedDaily) < retentionPolicy.Days && !savedDaily[dayKey] {
			backupsToKeep[b.Name] = true
			savedDaily[dayKey] = true
			continue // Promoted to daily
		}

		// Rule: Keep N weekly backups
		year, week := b.Time.ISOWeek()
		weekKey := fmt.Sprintf(weekFormat, year, week)
		if retentionPolicy.Weeks > 0 && len(savedWeekly) < retentionPolicy.Weeks && !savedWeekly[weekKey] {
			backupsToKeep[b.Name] = true
			savedWeekly[weekKey] = true
			continue // Promoted to weekly
		}

		// Rule: Keep N monthly backups
		monthKey := b.Time.Format(monthFormat)
		if retentionPolicy.Months > 0 && len(savedMonthly) < retentionPolicy.Months && !savedMonthly[monthKey] {
			backupsToKeep[b.Name] = true
			savedMonthly[monthKey] = true
			continue // Promoted to monthly
		}

		// Rule: Keep N yearly backups
		yearKey := b.Time.Format(yearFormat)
		if retentionPolicy.Years > 0 && len(savedYearly) < retentionPolicy.Years && !savedYearly[yearKey] {
			backupsToKeep[b.Name] = true
			savedYearly[yearKey] = true
		}
	}

	// Build a descriptive log message for the retention plan
	// Note we add slow fill warnings cause:
	// If the user asked for Daily backups for instance, but the interval is > 24h (e.g. Weekly),
	// add a note so they understand why they don't see 7 daily backups immediately.
	var planParts []string

	if retentionPolicy.Hours > 0 {
		msg := fmt.Sprintf("%d hourly", len(savedHourly))
		if e.archiver.IsSlowFillingArchive(1 * time.Hour) {
			msg += " (slow-fill)"
		}
		planParts = append(planParts, msg)
	}
	if retentionPolicy.Days > 0 {
		msg := fmt.Sprintf("%d daily", len(savedDaily))
		if e.archiver.IsSlowFillingArchive(24 * time.Hour) {
			msg += " (slow-fill)"
		}
		planParts = append(planParts, msg)
	}
	if retentionPolicy.Weeks > 0 {
		msg := fmt.Sprintf("%d weekly", len(savedWeekly))
		if e.archiver.IsSlowFillingArchive(7 * 24 * time.Hour) {
			msg += " (slow-fill)"
		}
		planParts = append(planParts, msg)
	}
	if retentionPolicy.Months > 0 {
		msg := fmt.Sprintf("%d monthly", len(savedMonthly))
		if e.archiver.IsSlowFillingArchive(30 * 24 * time.Hour) { // Approximation
			msg += " (slow-fill)"
		}
		planParts = append(planParts, msg)
	}
	if retentionPolicy.Years > 0 {
		msg := fmt.Sprintf("%d yearly", len(savedYearly))
		if e.archiver.IsSlowFillingArchive(365 * 24 * time.Hour) { // Approximation
			msg += " (slow-fill)"
		}
		planParts = append(planParts, msg)
	}
	plog.Debug("Retention plan", "policy", policyTitle, "details", strings.Join(planParts, ", "))
	plog.Debug("Total unique backups to be kept", "policy", policyTitle, "count", len(backupsToKeep))

	return backupsToKeep
}

// writeBackupMetafile creates and writes the .pgl-backup.meta.json file into a given directory.
func writeBackupMetafile(dirPath, version, mode, source string, backupTime time.Time, dryRun bool) error {
	if dryRun {
		plog.Info("[DRY RUN] Would write metafile", "directory", dirPath)
		return nil
	}

	metaFilePath := filepath.Join(dirPath, config.MetaFileName)
	metaData := runMetadata{
		Version:    version,
		BackupTime: backupTime,
		Mode:       mode,
		Source:     source,
	}

	jsonData, err := json.MarshalIndent(metaData, "", "  ")
	if err != nil {
		return fmt.Errorf("could not marshal meta data: %w", err)
	}

	// Use group-writable permissions for the metafile. Unlike the top-level config and lock files,
	// the metafile is part of the backup data itself. In multi-user environments, allowing
	// group members to write to backup contents is a common and useful scenario.
	// The config/lock files remain user-only writable for security.
	if err := os.WriteFile(metaFilePath, jsonData, util.UserGroupWritableFilePerms); err != nil {
		return fmt.Errorf("could not write meta file %s: %w", metaFilePath, err)
	}

	return nil
}

// readBackupMetafile opens and parses the .pgl-backup.meta.json file within a given directory.
// It returns the parsed metadata or an error if the file cannot be read.
func readBackupMetafile(dirPath string) (*runMetadata, error) {
	metaFilePath := filepath.Join(dirPath, config.MetaFileName)
	metaFile, err := os.Open(metaFilePath)
	if err != nil {
		// Note: os.IsNotExist errors are handled by the caller.
		return nil, err // Return the original error so os.IsNotExist works.
	}
	defer metaFile.Close()

	var metaData runMetadata
	decoder := json.NewDecoder(metaFile)
	if err := decoder.Decode(&metaData); err != nil {
		return nil, fmt.Errorf("could not parse metafile %s: %w. It may be corrupt", metaFilePath, err)
	}

	return &metaData, nil
}

// fetchSortedBackups scans a directory for valid backup folders, parses their
// metadata to get an accurate timestamp, and returns them sorted from newest to oldest.
// It relies exclusively on the `.pgl-backup.meta.json` file; directories without a
// readable metafile are ignored for retention purposes.
func (e *Engine) fetchSortedBackups(ctx context.Context, baseDir, excludeDir string) ([]backupInfo, error) {
	prefix := e.config.Naming.Prefix

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			plog.Debug("Archives directory does not exist yet, no retention policy to apply.", "path", baseDir)
			return nil, nil // Not an error, just means no archives exist yet.
		}
		return nil, fmt.Errorf("failed to read backup directory %s: %w", baseDir, err)
	}

	var backups []backupInfo
	for _, entry := range entries {
		// Check for cancellation during the directory scan.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		dirName := entry.Name()
		if !entry.IsDir() || !strings.HasPrefix(dirName, prefix) || dirName == excludeDir {
			continue
		}

		backupPath := filepath.Join(baseDir, dirName)
		metaData, err := readBackupMetafile(backupPath)
		if err != nil {
			plog.Warn("Skipping directory for retention check", "directory", dirName, "reason", err)
			continue
		}

		// The metafile is the sole source of truth for the backup time.
		backups = append(backups, backupInfo{Time: metaData.BackupTime, Name: dirName})
	}

	// Sort all backups from newest to oldest for consistent processing.
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].Time.After(backups[j].Time)
	})

	return backups, nil
}
