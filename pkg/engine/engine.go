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
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/filelock"
	"pixelgardenlabs.io/pgl-backup/pkg/pathsync"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
	"pixelgardenlabs.io/pgl-backup/pkg/preflight"
)

// --- ARCHITECTURAL OVERVIEW: Rollover vs. Retention Time Handling ---
//
// This engine employs two distinct time-handling strategies for creating and deleting backups,
// each designed to address a separate user concern:
//
// 1. Rollover (Snapshot Creation) - Predictable Creation
//    - Goal: To honor the user's configured `RolloverInterval` as literally as possible.
//    - Logic: The `shouldRollover` function calculates time-based "bucketing" based on the
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
	config  config.Config
	version string
	syncer  pathsync.Syncer
	// hookCommandExecutor allows mocking os/exec for testing hooks.
	hookCommandExecutor func(ctx context.Context, name string, arg ...string) *exec.Cmd
}

// New creates a new backup engine with the given configuration and version.
func New(cfg config.Config, version string) *Engine {
	return &Engine{
		config:              cfg,
		version:             version,
		syncer:              pathsync.NewPathSyncer(cfg), // Default to the real implementation.
		hookCommandExecutor: exec.CommandContext,         // Default to the real implementation.
	}
}

// acquireTargetLock ensures the target directory exists and acquires a file lock within it.
// It returns a release function that must be called to unlock the directory.
func (e *Engine) acquireTargetLock(ctx context.Context) (func(), error) {
	lockFilePath := filepath.Join(e.config.Paths.TargetBase, config.LockFileName)
	appID := fmt.Sprintf("pgl-backup:%s", e.config.Paths.Source)

	plog.Info("Attempting to acquire lock", "path", lockFilePath)
	lock, err := filelock.Acquire(ctx, lockFilePath, appID)
	if err != nil {
		var lockErr *filelock.ErrLockActive
		if errors.As(err, &lockErr) {
			plog.Warn("Operation is already running for this target.", "details", lockErr.Error())
			return nil, nil // Return nil error to indicate a graceful exit.
		}
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	plog.Info("Lock acquired successfully.")

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

	plog.Info("--- Initializing New Backup Target ---")

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

	// Generate the pgl-backup.conf file in the target directory.
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
	if err := e.runHooks(ctx, e.config.Hooks.PreBackup, "pre-backup"); err != nil {
		return fmt.Errorf("pre-backup hook failed: %w", err) // This is a fatal error.
	}

	// --- Post-Backup Hooks (deferred) ---
	// These will run at the end of the function, even if the backup fails.
	defer func() {
		plog.Info("--- Running Post-Backup Hooks ---")
		if err := e.runHooks(ctx, e.config.Hooks.PostBackup, "post-backup"); err != nil {
			if errors.Is(err, context.Canceled) {
				plog.Info("Post-backup hooks skipped due to cancellation.")
			} else {
				plog.Warn("Post-backup hook failed", "error", err)
			}
		}
	}()

	if e.config.DryRun {
		plog.Info("--- Starting Backup (DRY RUN) ---")
	} else {
		plog.Info("--- Starting Backup ---")
	}

	e.checkRetentionGranularity()

	// Capture a consistent UTC timestamp for the entire run to ensure unambiguous folder names
	// and avoid daylight saving time conflicts.
	currentRun := &runState{timestampUTC: time.Now().UTC()}

	plog.Info("Backup source", "path", e.config.Paths.Source)
	plog.Info("Backup mode", "mode", e.config.Mode)

	// --- 1. Pre-backup tasks (rollover) and destination calculation ---
	if err := e.prepareDestination(ctx, currentRun); err != nil {
		return err
	}

	plog.Info("Backup destination", "path", currentRun.target)
	plog.Info("------------------------------")

	// --- 2. Perform the backup ---
	if err := e.performSync(ctx, currentRun); err != nil {
		return fmt.Errorf("fatal backup error during sync: %w", err) // This is a fatal error, so we return it.
	}

	plog.Info("Backup operation completed.")

	// --- 3. Clean up old backups ---
	if err := e.applyRetentionPolicy(ctx); err != nil {
		// We log this as a non-fatal error because the main backup was successful.
		plog.Warn("Error applying retention policy", "error", err)
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
// a rollover if necessary for incremental backups.
func (e *Engine) prepareDestination(ctx context.Context, currentRun *runState) error {
	if e.config.Mode == config.SnapshotMode {
		// SNAPSHOT MODE
		//
		// The directory name must remain uniquely based on UTC time to avoid DST conflicts,
		// but we add the user's local offset to make the timezone clear to the user.
		timestamp := config.FormatTimestampWithOffset(currentRun.timestampUTC)
		backupDirName := e.config.Naming.Prefix + timestamp
		currentRun.target = filepath.Join(e.config.Paths.TargetBase, backupDirName)
	} else {
		// INCREMENTAL MODE (DEFAULT)
		if err := e.performRollover(ctx, currentRun); err != nil {
			return fmt.Errorf("error during backup rollover: %w", err)
		}
		currentIncrementalDirName := e.config.Naming.Prefix + e.config.Naming.IncrementalModeSuffix
		currentRun.target = filepath.Join(e.config.Paths.TargetBase, currentIncrementalDirName)
	}
	return nil
}

// performSync is the main entry point for synchronization.
func (e *Engine) performSync(ctx context.Context, currentRun *runState) error {
	source := e.config.Paths.Source
	destination := currentRun.target
	preserveSourceDirName := e.config.Paths.PreserveSourceDirectoryName
	mirror := e.config.Mode == config.IncrementalMode

	// If configured, append the source's base directory name to the destination path.
	if preserveSourceDirName {
		var nameToAppend string

		// Check if the path is a root path (e.g., "/" or "C:\")
		if filepath.Dir(source) == source {
			// Handle Windows Drive Roots (e.g. "D:\") -> "D"
			vol := filepath.VolumeName(source)
			if vol != "" {
				nameToAppend = strings.TrimSuffix(vol, ":")
			}
			// If vol is empty (Unix "/"), we append nothing.
		} else {
			// Standard folder
			nameToAppend = filepath.Base(source)
		}

		// Append if valid
		if nameToAppend != "" && nameToAppend != "." && nameToAppend != string(filepath.Separator) {
			destination = filepath.Join(destination, nameToAppend)
		}
	}

	// Combine system-required ignored files with user-defined ones for the sync operation.
	excludeFiles := config.GetSystemExcludeFilePatterns()
	excludeFiles = append(excludeFiles, e.config.Paths.ExcludeFiles...)
	excludeDirs := e.config.Paths.ExcludeDirs

	// Sync and check for errors after attempting the sync.
	if syncErr := e.syncer.Sync(ctx, source, destination, preserveSourceDirName, mirror, excludeFiles, excludeDirs); syncErr != nil {
		return fmt.Errorf("sync failed: %w", syncErr)
	}

	// If the sync was successful, write the metafile for retention purposes.
	return writeBackupMetafile(currentRun.target, e.version, e.config.Mode.String(), source, currentRun.timestampUTC, e.config.DryRun)
}

// performRollover checks if the incremental backup directory is from a previous day > RolloverInterval.
// If so, it renames it to a permanent timestamped archive.
func (e *Engine) performRollover(ctx context.Context, currentRun *runState) error {
	currentDirName := e.config.Naming.Prefix + e.config.Naming.IncrementalModeSuffix
	currentBackupPath := filepath.Join(e.config.Paths.TargetBase, currentDirName)

	metaData, err := readBackupMetafile(currentBackupPath)
	if os.IsNotExist(err) {
		return nil // No previous backup, nothing to roll over.
	}

	// Use the precise time from the file content, not the file's modification time.
	lastBackupTime := metaData.BackupTime

	if e.shouldRollover(lastBackupTime, currentRun) {
		plog.Info("Rollover threshold crossed, creating new archive.",
			"last_backup_time", lastBackupTime,
			"current_time_utc", currentRun.timestampUTC,
			"rollover_interval", e.config.RolloverInterval)

		// Check for cancellation before performing the rename.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// The directory name must remain uniquely based on UTC time to avoid DST conflicts,
		// but we add the user's local offset to make the timezone clear to the user.
		archiveTimestamp := config.FormatTimestampWithOffset(lastBackupTime)
		archiveDirName := fmt.Sprintf("%s%s", e.config.Naming.Prefix, archiveTimestamp)
		archivePath := filepath.Join(e.config.Paths.TargetBase, archiveDirName)

		// Sanity check: ensure the destination for the rollover does not already exist.
		if _, err := os.Stat(archivePath); err == nil {
			return fmt.Errorf("rollover destination %s already exists, cannot proceed", archivePath)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("could not check rollover destination %s: %w", archivePath, err)
		}

		plog.Info("Rolling over previous day's backup", "destination", archivePath)
		if e.config.DryRun {
			plog.Info("[DRY RUN] Would rename", "from", currentBackupPath, "to", archivePath)
			return nil
		} else if err := os.Rename(currentBackupPath, archivePath); err != nil {
			return fmt.Errorf("failed to roll over backup: %w", err)
		}
	}

	return nil
}

// shouldRollover determines if a new backup archive should be created based on the
// configured interval and the time of the last backup.
//
// DESIGN NOTE on time zones:
// For intervals of 24 hours or longer, this function intentionally calculates
// rollover boundaries based on the **local system's midnight** (`time.Local`).
// This ensures that rollovers align with a user's calendar day ("start a new
// weekly backup on Sunday night"), even though all stored timestamps are UTC.
// The conversion handles Daylight Saving Time (DST) shifts correctly by checking
// for midnight-to-midnight boundary crossings (epoch day counting).
func (e *Engine) shouldRollover(lastBackupTime time.Time, currentRun *runState) bool {
	// Handle the default (0 = 24h) for comparison logic
	effectiveInterval := e.config.RolloverInterval

	// NOTE: For multi-day intervals, the full implementation requires normalizing to local midnight
	// and calculating epoch day buckets to correctly handle DST and guarantee a new snapshot
	// at the start of every N-day cycle.

	// Multi-Day Intervals (Weekly, Every 3 Days, etc.)
	if effectiveInterval >= 24*time.Hour {
		// We want to ignore hours/minutes and just compare "Day Numbers".

		// Ensure Rollover happens at Local Midnight ---
		// To align the rollover boundary with the user's local calendar day (e.g.,
		// a daily backup always rolls over at 00:00 local time, regardless of DST),
		// we must perform the "day number" comparison in the system's local timezone.
		// 1. Normalize both times to the system's local midnight.
		loc := time.Local

		y1, m1, d1 := lastBackupTime.In(loc).Date() // Convert last backup to current/local time
		lastDayMidnight := time.Date(y1, m1, d1, 0, 0, 0, 0, loc)

		y2, m2, d2 := currentRun.timestampUTC.In(loc).Date()
		currentDayMidnight := time.Date(y2, m2, d2, 0, 0, 0, 0, loc)

		// 2. Calculate days since a fixed anchor (Unix Epoch Local).
		//    We use 24h logic here because we have already normalized to midnight.
		// Force the math to occur in the current system's timezone
		anchor := time.Date(1970, 1, 1, 0, 0, 0, 0, loc)

		lastDayNum := int64(lastDayMidnight.Sub(anchor).Hours() / 24)
		currentDayNum := int64(currentDayMidnight.Sub(anchor).Hours() / 24)

		// 3. Calculate the Bucket Size in Days (e.g., 168h / 24h = 7 days)
		daysInBucket := int64(effectiveInterval / (24 * time.Hour))

		// 4. Check if we have crossed a bucket boundary
		//    Example: Interval = 7 days.
		//    Day 10 / 7 = 1.  Day 12 / 7 = 1.  (No Rollover)
		//    Day 13 / 7 = 1.  Day 14 / 7 = 2.  (Rollover!)
		return (currentDayNum / daysInBucket) != (lastDayNum / daysInBucket)
	}
	// Sub-Daily Intervals (Hourly, 6-Hourly)
	// Use standard truncation for clean UTC time buckets.
	lastBackupBoundary := lastBackupTime.Truncate(effectiveInterval)
	currentBackupBoundary := currentRun.timestampUTC.Truncate(effectiveInterval)

	return !currentBackupBoundary.Equal(lastBackupBoundary)
}

// checkRetentionGranularity warns the user if their retention policy expects
// backups more frequently than the rollover interval allows.
func (e *Engine) checkRetentionGranularity() {
	policy := e.config.RetentionPolicy
	effectiveInterval := e.config.RolloverInterval

	// 1. Check Hourly Mismatch
	if policy.Hours > 0 && effectiveInterval > 1*time.Hour {
		plog.Warn("Configuration Mismatch: Hourly retention is enabled, but rollover is too slow.",
			"keep_hourly", policy.Hours,
			"rollover_interval", effectiveInterval,
			"impact", "Hourly slots will fill at the speed of the rollover interval.")
	}

	// 2. Check Daily Mismatch
	if policy.Days > 0 && effectiveInterval > 24*time.Hour {
		plog.Warn("Configuration Mismatch: Daily retention is enabled, but rollover is too slow.",
			"keep_daily", policy.Days,
			"rollover_interval", effectiveInterval,
			"impact", "Daily slots will be filled by Weekly/Monthly backups, delaying the 'Weekly' retention rule.")
	}

	// 3. Check Weekly Mismatch
	if policy.Weeks > 0 && effectiveInterval > 168*time.Hour {
		plog.Warn("Configuration Mismatch: Weekly retention is enabled, but rollover is too slow.",
			"keep_weekly", policy.Weeks,
			"rollover_interval", effectiveInterval)
	}

	// 4. Check Monthly Mismatch
	// We use 30 days (720h) as the rough approximation for a month.
	// If the rollover is slower than 30 days (e.g., 60 days), we cannot satisfy "Keep N Monthly".
	avgMonth := 30 * 24 * time.Hour
	if policy.Months > 0 && effectiveInterval > avgMonth {
		plog.Warn("Configuration Mismatch: Monthly retention is enabled, but rollover is too slow.",
			"keep_monthly", policy.Months,
			"rollover_interval", effectiveInterval,
			"impact", "Backups occur less frequently than once a month; some calendar months will have no backup.")
	}
}

// applyRetentionPolicy scans the backup target directory and deletes snapshots
// that are no longer needed according to the configured retention policy.
func (e *Engine) applyRetentionPolicy(ctx context.Context) error {
	currentDirName := e.config.Naming.Prefix + e.config.Naming.IncrementalModeSuffix
	baseDir := e.config.Paths.TargetBase
	retentionPolicy := e.config.RetentionPolicy

	if retentionPolicy.Hours <= 0 && retentionPolicy.Days <= 0 && retentionPolicy.Weeks <= 0 && retentionPolicy.Months <= 0 {
		plog.Info("Retention policy is disabled. Skipping cleanup.")
		return nil
	}
	plog.Info("--- Cleaning Up Old Backups ---")
	plog.Info("Applying retention policy", "directory", baseDir)
	// --- 1. Get a sorted list of all valid, historical backups ---
	allBackups, err := e.fetchSortedBackups(ctx, baseDir, currentDirName)
	if err != nil {
		return err
	}

	// --- 2. Apply retention rules to find which backups to keep ---
	backupsToKeep := e.determineBackupsToKeep(allBackups, retentionPolicy)

	// --- 3. Delete all backups that are not in our final `backupsToKeep` set ---
	for _, backup := range allBackups {
		// Check for cancellation before each deletion.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		dirName := backup.Name
		if _, shouldKeep := backupsToKeep[dirName]; !shouldKeep {
			dirToDelete := filepath.Join(baseDir, dirName)
			plog.Info("Deleting redundant or old backup", "path", dirToDelete)
			if e.config.DryRun {
				plog.Info("[DRY RUN] Would delete directory", "path", dirToDelete)
				continue
			}
			if err := os.RemoveAll(dirToDelete); err != nil {
				plog.Warn("Failed to delete old backup directory", "path", dirToDelete, "error", err)
			}
		}
	}

	return nil
}

// determineBackupsToKeep applies the retention policy to a sorted list of backups.
func (e *Engine) determineBackupsToKeep(allBackups []backupInfo, retentionPolicy config.BackupRetentionPolicyConfig) map[string]bool {
	backupsToKeep := make(map[string]bool)

	// Keep track of which periods we've already saved a backup for.
	savedHourly := make(map[string]bool)
	savedDaily := make(map[string]bool)
	savedWeekly := make(map[string]bool)
	savedMonthly := make(map[string]bool)

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
		}
	}

	// Build a descriptive log message for the retention plan
	// Note we add slow fill warnings cause:
	// If the user asked for Daily backups for instance, but the interval is > 24h (e.g. Weekly),
	// add a note so they understand why they don't see 7 daily backups immediately.
	var planParts []string
	if retentionPolicy.Hours > 0 {
		msg := fmt.Sprintf("%d hourly", len(savedHourly))
		if e.config.RolloverInterval > 1*time.Hour {
			msg += " (slow-fill)"
		}
		planParts = append(planParts, msg)
	}
	if retentionPolicy.Days > 0 {
		msg := fmt.Sprintf("%d daily", len(savedDaily))
		if e.config.RolloverInterval > 24*time.Hour {
			msg += " (slow-fill)"
		}
		planParts = append(planParts, msg)
	}
	if retentionPolicy.Weeks > 0 {
		msg := fmt.Sprintf("%d weekly", len(savedWeekly))
		// 168 hours is exactly 7 days
		if e.config.RolloverInterval > 168*time.Hour {
			msg += " (slow-fill)"
		}
		planParts = append(planParts, msg)
	}
	if retentionPolicy.Months > 0 {
		msg := fmt.Sprintf("%d monthly", len(savedMonthly))
		// Use 30 days (720 hours) as the monthly threshold
		avgMonth := 30 * 24 * time.Hour
		if e.config.RolloverInterval > avgMonth {
			msg += " (slow-fill)"
		}
		planParts = append(planParts, msg)
	}
	plog.Info("Retention plan", "details", strings.Join(planParts, ", "))
	plog.Info("Total unique backups to be kept", "count", len(backupsToKeep))

	return backupsToKeep
}

// writeBackupMetafile creates and writes the .pgl-backup.meta file into a given directory.
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

	if err := os.WriteFile(metaFilePath, jsonData, 0664); err != nil {
		return fmt.Errorf("could not write meta file %s: %w", metaFilePath, err)
	}

	return nil
}

// readBackupMetafile opens and parses the .pgl-backup.meta file within a given directory.
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
// It relies exclusively on the `.pgl-backup.meta` file; directories without a
// readable metafile are ignored for retention purposes.
func (e *Engine) fetchSortedBackups(ctx context.Context, baseDir, excludeDir string) ([]backupInfo, error) {
	prefix := e.config.Naming.Prefix

	entries, err := os.ReadDir(baseDir)
	if err != nil {
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
