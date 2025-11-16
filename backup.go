package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// backupInfo holds the parsed time and name of a backup directory.
type backupInfo struct {
	Time time.Time
	Name string
}

// backupRunMeta holds metadata about a specific backup run.
type backupRunMeta struct {
	Version    string    `json:"version"`
	BackupTime time.Time `json:"backupTime"`
}

// handleSync is the main entry point for synchronization. It decides whether to use
// the native Go implementation or the faster Robocopy implementation based on the
// operating system and configuration.
func handleSync(config backupConfig) error {
	src := config.Paths.Source
	dst := config.Paths.CurrentTarget
	mirror := config.Mode == IncrementalMode // Mirror mode deletes extra files in destination.

	// Validation for all sync paths.
	if err := validateSyncPaths(src, dst); err != nil {
		return err
	}

	switch config.Engine.Type {
	case RobocopyEngine:
		// Sync and check for errors after attempting the sync.
		if syncErr := handleSyncRobocopy(src, dst, mirror, config.DryRun, config.Quiet); syncErr != nil {
			return fmt.Errorf("robocopy sync failed: %w", syncErr)
		}
	case NativeEngine:
		log.Println("Using native Go implementation for synchronization...")
		// Sync and check for errors after attempting the sync.
		if syncErr := handleSyncNative(src, dst, mirror, config.DryRun, config.Quiet, config.Engine.NativeEngineWorkers); syncErr != nil {
			return fmt.Errorf("native sync failed: %w", syncErr)
		}
	default:
		return fmt.Errorf("unknown sync engine configured: %v", config.Engine.Type)
	}

	// If the sync was successful, update the metafile timestamp in incremental mode.
	if config.Mode == IncrementalMode {
		if config.DryRun {
			log.Printf("[DRY RUN] Would update metafile in %s", dst)
			return nil
		}
		metaFilePath := filepath.Join(dst, ".ppBackup.meta")
		metaData := backupRunMeta{
			Version:    version,
			BackupTime: time.Now(),
		}

		jsonData, err := json.MarshalIndent(metaData, "", "  ")
		if err != nil {
			log.Printf("Warning: could not marshal meta data: %v", err)
			return nil // Don't let a metafile error fail the backup.
		}

		if err := os.WriteFile(metaFilePath, jsonData, 0664); err != nil {
			log.Printf("Warning: could not write meta file: %v", err)
		}
	}

	return nil
}

// handleRollover checks if the incremental backup directory is from a previous day.
// If so, it renames it to a permanent timestamped archive.
func handleRollover(config backupConfig) error {
	currentDirName := config.Naming.Prefix + config.Naming.IncrementalModeSuffix
	currentBackupPath := filepath.Join(config.Paths.TargetBase, currentDirName)
	metaFilePath := filepath.Join(currentBackupPath, ".ppBackup.meta")

	metaFile, err := os.Open(metaFilePath)
	if os.IsNotExist(err) {
		return nil // No previous backup, nothing to roll over.
	}
	if err != nil {
		return fmt.Errorf("could not open metafile in %s: %w", currentBackupPath, err)
	}
	defer metaFile.Close()

	var metaData backupRunMeta
	decoder := json.NewDecoder(metaFile)
	if err := decoder.Decode(&metaData); err != nil {
		return fmt.Errorf("could not parse metafile %s: %w. It may be corrupt", metaFilePath, err)
	}

	// Use the precise time from the file content, not the file's modification time.
	lastBackupTime := metaData.BackupTime

	now := time.Now()

	// Check if the last backup was on a different day.
	isDifferentDay := now.Year() != lastBackupTime.Year() || now.YearDay() != lastBackupTime.YearDay()

	if isDifferentDay {
		archiveTimestamp := lastBackupTime.Format(config.Naming.TimeFormat)
		archiveDirName := fmt.Sprintf("%s%s", config.Naming.Prefix, archiveTimestamp)
		archivePath := filepath.Join(config.Paths.TargetBase, archiveDirName)

		log.Printf("Rolling over previous day's backup to: %s", archivePath)
		if config.DryRun {
			log.Printf("[DRY RUN] Would rename %s to %s", currentBackupPath, archivePath)
			// In a dry run, we must exit here to prevent the next sync from using the wrong directory.
			// We simulate a successful rollover for the rest of the dry run logic.
			return nil
		} else if err := os.Rename(currentBackupPath, archivePath); err != nil {
			return fmt.Errorf("failed to roll over backup: %w", err)
		}
	}

	return nil
}

// handleRetention scans the backup target directory and deletes snapshots
// that are no longer needed according to the configured retention policy.
func handleRetention(config backupConfig) error {
	currentDirName := config.Naming.Prefix + config.Naming.IncrementalModeSuffix
	baseDir := config.Paths.TargetBase
	policy := config.Retention

	if policy.Hours <= 0 && policy.Days <= 0 && policy.Weeks <= 0 && policy.Months <= 0 {
		log.Println("Retention policy is disabled. Skipping cleanup.")
		return nil
	}
	log.Printf("Checking for old backups to clean up in %s...", baseDir)

	// --- 1. Get a sorted list of all valid, historical backups ---
	allBackups, err := fetchSortedBackups(baseDir, config.Naming.Prefix, config.Naming.TimeFormat, currentDirName)
	if err != nil {
		return err
	}

	// --- 2. Apply retention rules to find which backups to keep ---
	backupsToKeep := make(map[string]bool)
	now := time.Now()

	// Keep track of which periods we've already saved a backup for.
	savedHour := make(map[string]bool)
	savedDay := make(map[string]bool)
	savedWeek := make(map[string]bool)
	savedMonth := make(map[string]bool)

	for _, b := range allBackups {
		if policy.Hours > 0 {
			// Rule: Hourly backups for the current day
			if b.Time.Year() == now.Year() && b.Time.YearDay() == now.YearDay() {
				hourKey := b.Time.Format("2006-01-02-15") // YYYY-MM-DD-HH
				if len(savedHour) < policy.Hours && !savedHour[hourKey] {
					backupsToKeep[b.Name] = true
					savedHour[hourKey] = true
				}
			}
		}

		// Rule: Daily backups for the last N days
		if policy.Days > 0 {
			dayKey := b.Time.Format("2006-01-02")
			if len(savedDay) < policy.Days && !savedDay[dayKey] {
				backupsToKeep[b.Name] = true
				savedDay[dayKey] = true
			}
		}

		// Rule: Weekly backups for the last N weeks (keep the newest backup for each week)
		if policy.Weeks > 0 {
			year, week := b.Time.ISOWeek()
			weekKey := fmt.Sprintf("%d-%d", year, week)
			if len(savedWeek) < policy.Weeks && !savedWeek[weekKey] {
				backupsToKeep[b.Name] = true
				savedWeek[weekKey] = true
			}
		}

		// Rule: Monthly backups for the last N months (keep the newest backup for each month)
		if policy.Months > 0 {
			monthKey := b.Time.Format("2006-01")
			if len(savedMonth) < policy.Months && !savedMonth[monthKey] {
				backupsToKeep[b.Name] = true
				savedMonth[monthKey] = true
			}
		}
	}

	// Build a descriptive log message for the retention plan
	var planParts []string
	if policy.Hours > 0 {
		planParts = append(planParts, fmt.Sprintf("%d hourly (for today)", len(savedHour)))
	}
	if policy.Days > 0 {
		planParts = append(planParts, fmt.Sprintf("%d daily", len(savedDay)))
	}
	if policy.Weeks > 0 {
		planParts = append(planParts, fmt.Sprintf("%d weekly", len(savedWeek)))
	}
	if policy.Months > 0 {
		planParts = append(planParts, fmt.Sprintf("%d monthly", len(savedMonth)))
	}
	log.Printf("Retention plan: %s snapshots to be kept.", strings.Join(planParts, ", "))
	log.Printf("Total backups to be kept: %d", len(backupsToKeep))

	// --- 3. Delete all backups that are not in our final `backupsToKeep` set ---
	for _, backup := range allBackups {
		dirName := backup.Name
		if _, shouldKeep := backupsToKeep[dirName]; !shouldKeep {
			dirToDelete := filepath.Join(baseDir, dirName)
			log.Printf("DELETING redundant or old backup: %s", dirToDelete)
			if config.DryRun {
				log.Printf("[DRY RUN] Would delete directory: %s", dirToDelete)
				continue
			}
			if err := os.RemoveAll(dirToDelete); err != nil {
				log.Printf("Warning: failed to delete old backup directory %s: %v", dirToDelete, err)
			}
		}
	}

	return nil
}

// fetchSortedBackups scans a directory for valid backup folders, parses their
// timestamps, and returns them sorted from newest to oldest.
func fetchSortedBackups(baseDir, prefix, timeFormat, excludeDir string) ([]backupInfo, error) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read backup directory %s: %w", baseDir, err)
	}

	var backups []backupInfo
	for _, entry := range entries {
		dirName := entry.Name()
		if !entry.IsDir() || !strings.HasPrefix(dirName, prefix) || dirName == excludeDir {
			continue
		}

		timestampStr := strings.TrimPrefix(dirName, prefix)
		backupTime, err := time.Parse(timeFormat, timestampStr)
		if err != nil {
			log.Printf("Skipping directory with invalid format: %s", dirName)
			continue
		}

		backups = append(backups, backupInfo{Time: backupTime, Name: dirName})
	}

	// Sort all backups from newest to oldest for consistent processing.
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].Time.After(backups[j].Time)
	})

	return backups, nil
}
