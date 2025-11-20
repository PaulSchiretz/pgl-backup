package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/pathsync"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// backupInfo holds the parsed time and name of a backup directory.
type backupInfo struct {
	Time time.Time
	Name string
}

// runMetadata holds metadata about a specific backup run.
type runMetadata struct {
	Version    string    `json:"version"`
	BackupTime time.Time `json:"backupTime"`
	Mode       string    `json:"mode"`
	Source     string    `json:"source"`
}

// Engine orchestrates the entire backup process.
type Engine struct {
	config           config.Config
	version          string
	currentTarget    string
	currentTimestamp time.Time // The timestamp of the current backup run for consistency.
}

// New creates a new backup engine with the given configuration and version.
func New(cfg config.Config, version string) *Engine {
	return &Engine{
		config:  cfg,
		version: version,
	}
}

// Execute runs the entire backup job from start to finish.
func (e *Engine) Execute(ctx context.Context) error {
	// Check for cancellation at the very beginning.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if e.config.DryRun {
		plog.Info("--- Starting Backup (DRY RUN) ---")
	} else {
		plog.Info("--- Starting Backup ---")
	}

	e.checkRetentionGranularity()

	e.currentTimestamp = time.Now() // Capture a consistent timestamp for the entire run.

	plog.Info("Backup source", "path", e.config.Paths.Source)
	plog.Info("Backup mode", "mode", e.config.Mode)

	// --- 1. Pre-backup tasks (rollover) and destination calculation ---
	if err := e.prepareDestination(ctx); err != nil {
		return err
	}

	plog.Info("Backup destination", "path", e.currentTarget)
	plog.Info("------------------------------")

	// --- 2. Perform the backup ---
	if err := e.performSync(ctx); err != nil {
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

// prepareDestination calculates the target directory for the backup, performing
// a rollover if necessary for incremental backups.
func (e *Engine) prepareDestination(ctx context.Context) error {
	if e.config.Mode == config.SnapshotMode {
		// SNAPSHOT MODE
		timestamp := e.currentTimestamp.Format(e.config.Naming.TimeFormat)
		backupDirName := e.config.Naming.Prefix + timestamp
		e.currentTarget = filepath.Join(e.config.Paths.TargetBase, backupDirName)
	} else {
		// INCREMENTAL MODE (DEFAULT)
		if err := e.performRollover(ctx); err != nil {
			return fmt.Errorf("error during backup rollover: %w", err)
		}
		currentIncrementalDirName := e.config.Naming.Prefix + e.config.Naming.IncrementalModeSuffix
		e.currentTarget = filepath.Join(e.config.Paths.TargetBase, currentIncrementalDirName)
	}
	return nil
}

// performSync is the main entry point for synchronization.
func (e *Engine) performSync(ctx context.Context) error {
	pathSyncer := pathsync.NewPathSyncer(e.config)
	source := e.config.Paths.Source
	mirror := e.config.Mode == config.IncrementalMode

	// Combine system-required ignored files with user-defined ones. The meta file is critical.
	excludeFiles := []string{config.MetaFileName}
	excludeFiles = append(excludeFiles, e.config.Paths.ExcludeFiles...)
	excludeDirs := e.config.Paths.ExcludeDirs

	// Sync and check for errors after attempting the sync.
	if syncErr := pathSyncer.Sync(ctx, source, e.currentTarget, mirror, excludeFiles, excludeDirs); syncErr != nil {
		return fmt.Errorf("sync failed: %w", syncErr)
	}

	// If the sync was successful, write the metafile for retention purposes.
	return writeBackupMetafile(e.currentTarget, e.version, e.config.Mode.String(), source, e.currentTimestamp, e.config.DryRun)
}

// performRollover checks if the incremental backup directory is from a previous day.
// If so, it renames it to a permanent timestamped archive.
func (e *Engine) performRollover(ctx context.Context) error {
	currentDirName := e.config.Naming.Prefix + e.config.Naming.IncrementalModeSuffix
	currentBackupPath := filepath.Join(e.config.Paths.TargetBase, currentDirName)

	metaData, err := readBackupMetafile(currentBackupPath)
	if os.IsNotExist(err) {
		return nil // No previous backup, nothing to roll over.
	}

	// Use the precise time from the file content, not the file's modification time.
	lastBackupTime := metaData.BackupTime

	if e.shouldRollover(lastBackupTime) {
		plog.Info("Rollover threshold crossed, creating new archive.",
			"last_backup_time", lastBackupTime,
			"current_time", e.currentTimestamp,
			"rollover_interval", e.config.RolloverInterval)

		// Check for cancellation before performing the rename.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		archiveTimestamp := lastBackupTime.Format(e.config.Naming.TimeFormat)
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
func (e *Engine) shouldRollover(lastBackupTime time.Time) bool {
	interval := e.config.RolloverInterval
	currentTimestamp := e.currentTimestamp

	// Multi-Day Intervals (Weekly, Every 3 Days, etc.)
	if interval >= 24*time.Hour {
		// We want to ignore hours/minutes and just compare "Day Numbers".

		// 1. Normalize both times to Local Midnight (strip hours/min/sec)
		// Force the math to occur in the current system's timezone
		loc := e.currentTimestamp.Location()

		y1, m1, d1 := lastBackupTime.In(loc).Date() // Convert last backup to current/local time
		lastDayMidnight := time.Date(y1, m1, d1, 0, 0, 0, 0, loc)

		y2, m2, d2 := e.currentTimestamp.Date()
		currentDayMidnight := time.Date(y2, m2, d2, 0, 0, 0, 0, loc)

		// 2. Calculate days since a fixed anchor (Unix Epoch Local).
		//    We use 24h logic here because we have already normalized to midnight.
		// Force the math to occur in the current system's timezone
		anchor := time.Date(1970, 1, 1, 0, 0, 0, 0, loc)

		lastDayNum := int64(lastDayMidnight.Sub(anchor).Hours() / 24)
		currentDayNum := int64(currentDayMidnight.Sub(anchor).Hours() / 24)

		// 3. Calculate the Bucket Size in Days (e.g., 168h / 24h = 7 days)
		daysInBucket := int64(interval / (24 * time.Hour))

		// 4. Check if we have crossed a bucket boundary
		//    Example: Interval = 7 days.
		//    Day 10 / 7 = 1.  Day 12 / 7 = 1.  (No Rollover)
		//    Day 13 / 7 = 1.  Day 14 / 7 = 2.  (Rollover!)
		return (currentDayNum / daysInBucket) != (lastDayNum / daysInBucket)
	}
	// Sub-Daily Intervals (Hourly, 6-Hourly)
	// Use standard truncation for clean UTC time buckets.
	lastBackupBoundary := lastBackupTime.Truncate(interval)
	currentBackupBoundary := currentTimestamp.Truncate(interval)

	return !currentBackupBoundary.Equal(lastBackupBoundary)
}

// checkRetentionGranularity warns the user if their retention policy expects
// backups more frequently than the rollover interval allows.
func (e *Engine) checkRetentionGranularity() {
	policy := e.config.RetentionPolicy
	interval := e.config.RolloverInterval

	// Handle the default (0 = 24h) for comparison logic
	effectiveInterval := interval
	if effectiveInterval <= 0 {
		effectiveInterval = 24 * time.Hour
	}

	// 1. Check Hourly Mismatch
	if policy.Hours > 0 && effectiveInterval > 1*time.Hour {
		plog.Warn("Configuration Mismatch: Hourly retention is enabled, but rollover is too slow.",
			"keep_hourly", policy.Hours,
			"rollover_interval", interval,
			"impact", "Hourly slots will fill at the speed of the rollover interval.")
	}

	// 2. Check Daily Mismatch
	if policy.Days > 0 && effectiveInterval > 24*time.Hour {
		plog.Warn("Configuration Mismatch: Daily retention is enabled, but rollover is too slow.",
			"keep_daily", policy.Days,
			"rollover_interval", interval,
			"impact", "Daily slots will be filled by Weekly/Monthly backups, delaying the 'Weekly' retention rule.")
	}

	// 3. Check Weekly Mismatch
	if policy.Weeks > 0 && effectiveInterval > 168*time.Hour {
		plog.Warn("Configuration Mismatch: Weekly retention is enabled, but rollover is too slow.",
			"keep_weekly", policy.Weeks,
			"rollover_interval", interval)
	}

	// 4. Check Monthly Mismatch
	// We use 30 days (720h) as the rough approximation for a month.
	// If the rollover is slower than 30 days (e.g., 60 days), we cannot satisfy "Keep N Monthly".
	avgMonth := 30 * 24 * time.Hour
	if policy.Months > 0 && effectiveInterval > avgMonth {
		plog.Warn("Configuration Mismatch: Monthly retention is enabled, but rollover is too slow.",
			"keep_monthly", policy.Months,
			"rollover_interval", interval,
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
		hourKey := b.Time.Format("2006-01-02-15") // YYYY-MM-DD-HH
		if retentionPolicy.Hours > 0 && len(savedHourly) < retentionPolicy.Hours && !savedHourly[hourKey] {
			backupsToKeep[b.Name] = true
			savedHourly[hourKey] = true
			continue // Promoted to hourly, skip other rules
		}

		// Rule: Keep N daily backups
		dayKey := b.Time.Format("2006-01-02")
		if retentionPolicy.Days > 0 && len(savedDaily) < retentionPolicy.Days && !savedDaily[dayKey] {
			backupsToKeep[b.Name] = true
			savedDaily[dayKey] = true
			continue // Promoted to daily
		}

		// Rule: Keep N weekly backups
		year, week := b.Time.ISOWeek()
		weekKey := fmt.Sprintf("%d-%d", year, week)
		if retentionPolicy.Weeks > 0 && len(savedWeekly) < retentionPolicy.Weeks && !savedWeekly[weekKey] {
			backupsToKeep[b.Name] = true
			savedWeekly[weekKey] = true
			continue // Promoted to weekly
		}

		// Rule: Keep N monthly backups
		monthKey := b.Time.Format("2006-01")
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
