package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// backupInfo holds the parsed time and name of a backup directory.
type backupInfo struct {
	Time time.Time
	Name string
}

// handleSync is the main entry point for synchronization. It decides whether to use
// the native Go implementation or the faster Robocopy implementation based on the
// operating system and configuration.
func handleSync(config backupConfig) error {
	src := config.Paths.Source
	dst := config.Paths.CurrentTarget
	mirror := config.Mode == incrementalMode // Mirror mode deletes extra files in destination.

	// Validation for all sync paths.
	if err := validateSyncPaths(src, dst); err != nil {
		return err
	}

	useRobocopy := config.UseRobocopy && runtime.GOOS == "windows"
	if useRobocopy {
		log.Println("Using robocopy for synchronization.")
		syncErr := handleSyncRobocopy(src, dst, mirror)

		if syncErr != nil {
			return fmt.Errorf("robocopy sync failed: %w", syncErr)
		}
	} else {
		log.Println("Using native Go implementation for synchronization.")
		if mirror {
			log.Println("Warning: Native Go sync does not support mirror (delete) operations. Only additions/updates will be performed.")
		}
		syncErr := handleSyncNative(src, dst)
		// Check for fatal errors after attempting the sync.
		if syncErr != nil {
			return fmt.Errorf("native sync failed: %w", syncErr)
		}
	}

	// If the sync was successful, update the metafile timestamp in incremental mode.
	if config.Mode == incrementalMode {
		metaFilePath := filepath.Join(dst, ".ppbackup_meta")
		if f, err := os.Create(metaFilePath); err != nil {
			log.Printf("Warning: could not update metafile timestamp: %v", err)
		} else {
			f.Close()
		}
	}

	return nil
}

// handleRollover checks if the incremental backup directory is from a previous day.
// If so, it renames it to a permanent timestamped archive.
func handleRollover(config backupConfig) error {
	currentDirName := config.Naming.Prefix + config.Naming.IncrementalModeSuffix
	currentBackupPath := filepath.Join(config.Paths.TargetBase, currentDirName)
	metaFilePath := filepath.Join(currentBackupPath, ".ppbackup_meta")

	meta, err := os.Stat(metaFilePath)
	if os.IsNotExist(err) {
		return nil // No previous backup, nothing to roll over.
	}
	if err != nil {
		return fmt.Errorf("could not stat metafile in %s: %w", currentBackupPath, err)
	}

	lastBackupTime := meta.ModTime()
	now := time.Now()

	// Check if the last backup was on a different day.
	isDifferentDay := now.Year() != lastBackupTime.Year() || now.YearDay() != lastBackupTime.YearDay()

	if isDifferentDay {
		archiveTimestamp := lastBackupTime.Format(config.Naming.TimeFormat)
		archiveDirName := fmt.Sprintf("%s%s", config.Naming.Prefix, archiveTimestamp)
		archivePath := filepath.Join(config.Paths.TargetBase, archiveDirName)

		log.Printf("Rolling over previous day's backup to: %s", archivePath)
		if err := os.Rename(currentBackupPath, archivePath); err != nil {
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
	log.Printf("Checking for old backups to clean up in %s...", baseDir)

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return fmt.Errorf("failed to read backup directory %s: %w", baseDir, err)
	}

	// --- 1. Collect all valid backups ---
	var allBackups []backupInfo
	allBackupNames := make(map[string]bool)

	for _, entry := range entries {
		dirName := entry.Name()
		if !entry.IsDir() || !strings.HasPrefix(dirName, config.Naming.Prefix) || dirName == currentDirName {
			continue
		}

		// --- 2. Validate the directory name format ---
		timestampStr := strings.TrimPrefix(dirName, config.Naming.Prefix)
		backupTime, err := time.Parse(config.Naming.TimeFormat, timestampStr)
		if err != nil {
			log.Printf("Skipping directory with invalid format: %s", dirName)
			continue // Not a valid backup name format, so we ignore it.
		}

		allBackupNames[dirName] = true
		allBackups = append(allBackups, backupInfo{Time: backupTime, Name: dirName})
	}

	// --- 2. Apply retention rules to find which backups to keep ---
	backupsToKeep := make(map[string]bool)
	now := time.Now()

	// Sort all backups once, from newest to oldest, for efficient processing.
	sort.Slice(allBackups, func(i, j int) bool {
		return allBackups[i].Time.After(allBackups[j].Time)
	})

	// Keep track of which periods we've already saved a backup for.
	savedHour := make(map[string]bool)
	savedDay := make(map[string]bool)
	savedWeek := make(map[string]bool)
	savedMonth := make(map[string]bool)

	for _, b := range allBackups {
		if config.Retention.Hours > 0 {
			// Rule: Hourly backups for the current day
			if b.Time.Year() == now.Year() && b.Time.YearDay() == now.YearDay() {
				hourKey := b.Time.Format("2006-01-02-15") // YYYY-MM-DD-HH
				if len(savedHour) < config.Retention.Hours && !savedHour[hourKey] {
					backupsToKeep[b.Name] = true
					savedHour[hourKey] = true
				}
			}
		}

		// Rule: Daily backups for the last N days
		if config.Retention.Days > 0 {
			dayKey := b.Time.Format("2006-01-02")
			if len(savedDay) < config.Retention.Days && !savedDay[dayKey] {
				backupsToKeep[b.Name] = true
				savedDay[dayKey] = true
			}
		}

		// Rule: Weekly backups for the last N weeks (keep the newest backup for each week)
		if config.Retention.Weeks > 0 {
			year, week := b.Time.ISOWeek()
			weekKey := fmt.Sprintf("%d-%d", year, week)
			if len(savedWeek) < config.Retention.Weeks && !savedWeek[weekKey] {
				backupsToKeep[b.Name] = true
				savedWeek[weekKey] = true
			}
		}

		// Rule: Monthly backups for the last N months (keep the newest backup for each month)
		if config.Retention.Months > 0 {
			monthKey := b.Time.Format("2006-01")
			if len(savedMonth) < config.Retention.Months && !savedMonth[monthKey] {
				backupsToKeep[b.Name] = true
				savedMonth[monthKey] = true
			}
		}
	}

	// Build a descriptive log message for the retention plan
	var planParts []string
	if config.Retention.Hours > 0 {
		planParts = append(planParts, fmt.Sprintf("%d hourly (for today)", len(savedHour)))
	}
	if config.Retention.Days > 0 {
		planParts = append(planParts, fmt.Sprintf("%d daily", len(savedDay)))
	}
	if config.Retention.Weeks > 0 {
		planParts = append(planParts, fmt.Sprintf("%d weekly", len(savedWeek)))
	}
	if config.Retention.Months > 0 {
		planParts = append(planParts, fmt.Sprintf("%d monthly", len(savedMonth)))
	}
	log.Printf("Retention plan: %s snapshots to be kept.", strings.Join(planParts, ", "))
	log.Printf("Total backups to be kept: %d", len(backupsToKeep))

	// --- 3. Delete all backups that are not in our final `backupsToKeep` set ---
	for dirName := range allBackupNames {
		if _, shouldKeep := backupsToKeep[dirName]; !shouldKeep {
			dirToDelete := filepath.Join(baseDir, dirName)
			log.Printf("DELETING redundant or old backup: %s", dirToDelete)
			if err := os.RemoveAll(dirToDelete); err != nil {
				log.Printf("Warning: failed to delete old backup directory %s: %v", dirToDelete, err)
			}
		}
	}

	return nil
}
