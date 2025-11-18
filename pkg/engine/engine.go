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
	// Sync and check for errors after attempting the sync.
	if syncErr := pathSyncer.Sync(ctx, source, e.currentTarget, mirror); syncErr != nil {
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

	// Check if the last backup was on a different day.
	isDifferentDay := e.currentTimestamp.Year() != lastBackupTime.Year() || e.currentTimestamp.YearDay() != lastBackupTime.YearDay()

	if isDifferentDay {
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
	var planParts []string
	if retentionPolicy.Hours > 0 {
		planParts = append(planParts, fmt.Sprintf("%d hourly", len(savedHourly)))
	}
	if retentionPolicy.Days > 0 {
		planParts = append(planParts, fmt.Sprintf("%d daily", len(savedDaily)))
	}
	if retentionPolicy.Weeks > 0 {
		planParts = append(planParts, fmt.Sprintf("%d weekly", len(savedWeekly)))
	}
	if retentionPolicy.Months > 0 {
		planParts = append(planParts, fmt.Sprintf("%d monthly", len(savedMonthly)))
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

	metaFilePath := filepath.Join(dirPath, ".pgl-backup.meta")
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
	metaFilePath := filepath.Join(dirPath, ".pgl-backup.meta")
	metaFile, err := os.Open(metaFilePath)
	if err != nil {
		// Note: os.IsNotExist errors are handled by the caller.
		return nil, fmt.Errorf("could not open metafile in %s: %w", dirPath, err)
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
