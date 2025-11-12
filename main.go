package main

import (
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// copyFile handles copying a regular file from src to dst and preserves permissions.
func copyFile(src, dst string) error {

	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", src, err)
	}
	defer in.Close()

	// 1. Create a temporary file in the destination directory.
	// This ensures that we don't overwrite the destination until the copy is complete
	// and that the rename operation will be on the same filesystem (making it atomic).
	dstDir := filepath.Dir(dst)
	out, err := os.CreateTemp(dstDir, "ppbackup-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary file in %s: %w", dstDir, err)
	}
	// Clean up the temporary file if any step fails before the final rename.
	defer os.Remove(out.Name())
	defer out.Close()

	// 2. Copy content
	if _, err = io.Copy(out, in); err != nil {
		return fmt.Errorf("failed to copy content from %s to %s: %w", src, out.Name(), err)
	}

	// 3. Copy file mode (permissions)
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to get stat for source file %s: %w", src, err)
	}
	if err := out.Chmod(info.Mode()); err != nil {
		return fmt.Errorf("failed to set permissions on temporary file %s: %w", out.Name(), err)
	}

	// 4. Atomically move the temporary file to the final destination.
	return os.Rename(out.Name(), dst)
}

// SyncDirTree incrementally copies files from src to dst, only if the source file
// is newer than the destination file. It also logs the copied files.
func SyncDirTree(src, dst string, logFunc func(string)) error {
	// --- 1. PRE-CHECK: Check if the destination directory exists and is writable ---
	// We check if we can create a temporary file in the destination.
	// We use MkdirAll first in case the destination directory does not exist yet.
	if err := os.MkdirAll(dst, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory %s: %w", dst, err)
	}

	tempFile := filepath.Join(dst, "test_write.tmp")
	if f, err := os.Create(tempFile); err != nil {
		return fmt.Errorf("destination directory %s is not writable: %w", dst, err)
	} else {
		f.Close()
		// Clean up the temporary file after successful check
		os.Remove(tempFile)
	}
	// -------------------------------------------------------------------------------

	log.Printf("Starting incremental sync from %s to %s. Destination is writable.", src, dst)

	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err // Stop if there's an error accessing a path
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dst, relPath)
		fileType := d.Type()

		if fileType.IsDir() {
			// HANDLE DIRECTORIES
			info, err := d.Info()
			if err != nil {
				return err
			}
			return os.MkdirAll(dstPath, info.Mode())

		} else if fileType.IsRegular() {
			// HANDLE REGULAR FILES
			srcInfo, err := d.Info()
			if err != nil {
				return err
			}

			// Check if the destination file exists and if it's older.
			dstInfo, err := os.Stat(dstPath)
			if err == nil {
				// Destination exists, compare modification times and size.
				if !srcInfo.ModTime().After(dstInfo.ModTime()) && srcInfo.Size() == dstInfo.Size() {
					return nil // Skip if source is not newer.
				}
			}

			// If we reach here, we need to copy the file.
			if err := copyFile(path, dstPath); err != nil {
				return err
			}

			// Log the copied file
			logFunc(relPath)
			return nil

		} else {
			// SKIP ALL OTHER TYPES (including Symbolic Links)
			// log.Printf("Skipping non-regular file or directory: %s (Type: %s)", path, fileType.String())
			return nil
		}
	})
}

// syncDirTreeRobocopy uses the Windows `robocopy` utility to perform a highly
// efficient and robust directory mirror. It is much faster for incremental
// backups than a manual walk. It returns a list of copied files.
func syncDirTreeRobocopy(src, dst string, mirror bool) ([]string, error) {
	// Ensure the destination directory exists. Robocopy can create it, but
	// it's good practice to ensure the parent exists and is writable.
	if err := os.MkdirAll(dst, 0755); err != nil {
		return nil, fmt.Errorf("failed to create destination directory %s: %w", dst, err)
	}

	// Robocopy command arguments:
	// /MIR :: MIRror a directory tree (equivalent to /E plus /PURGE).
	// /E :: copy subdirectories, including Empty ones.
	// /PURGE :: delete destination files/dirs that no longer exist in source.
	// /R:3 :: Retry 3 times on failed copies.
	// /W:5 :: Wait 5 seconds between retries.
	// /NP :: No Progress - don't display % copied.
	// /NJH :: No Job Header.
	// /NJS :: No Job Summary.
	// /NDL :: No Directory List - don't log directory names.
	// /L :: List only - don't copy, delete, or timestamp any files. We use this for a dry run.
	args := []string{src, dst, "/R:3", "/W:5", "/NP", "/NJH", "/NJS", "/NDL", "/L"}
	if mirror {
		args = append(args, "/MIR")
	} else {
		args = append(args, "/E")
	}
	cmd := exec.Command("robocopy", args...)

	log.Println("Performing dry run with robocopy to find changed files...")
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Robocopy exit codes < 8 indicate success (with files copied, etc.)
		// We check the exit code to see if it's a real error.
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() >= 8 {
				return nil, fmt.Errorf("robocopy dry run failed with exit code %d: %s", exitError.ExitCode(), string(output))
			}
		} else {
			return nil, fmt.Errorf("failed to execute robocopy dry run: %w", err)
		}
	}

	copiedFiles := strings.Fields(strings.TrimSpace(string(output)))

	log.Println("Performing actual sync with robocopy...")
	// Remove the /L (List only) flag for the actual copy operation.
	actualArgs := []string{src, dst, "/R:3", "/W:5"}
	if mirror {
		actualArgs = append(actualArgs, "/MIR")
	} else {
		actualArgs = append(actualArgs, "/E")
	}
	cmd = exec.Command("robocopy", actualArgs...)

	return copiedFiles, cmd.Run()
}

// backupInfo holds the parsed time and name of a backup directory.
type backupInfo struct {
	Time time.Time
	Name string
}

// handleRollover checks if the incremental backup directory is from a previous day.
// If so, it renames it to a permanent timestamped archive.
func handleRollover(baseDir, prefix, currentDirName string) error {
	currentBackupPath := filepath.Join(baseDir, currentDirName)
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
		archiveTimestamp := lastBackupTime.Format(backupTimeFormat)
		archiveDirName := fmt.Sprintf("%s%s", prefix, archiveTimestamp)
		archivePath := filepath.Join(baseDir, archiveDirName)

		log.Printf("Rolling over previous day's backup to: %s", archivePath)
		if err := os.Rename(currentBackupPath, archivePath); err != nil {
			return fmt.Errorf("failed to roll over backup: %w", err)
		}
	}

	return nil
}

// cleanupOldBackups scans a directory, finds all subdirectories (assumed to be backups),
// sorts them by name (chronologically), and removes the oldest ones, keeping only// a tiered set of backups (hourly, daily, weekly, monthly).
func cleanupOldBackups(baseDir, prefix string, keepDaily, keepWeekly, keepMonthly int, currentDirName string) error {
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
		if !entry.IsDir() || !strings.HasPrefix(dirName, prefix) || dirName == currentDirName {
			continue
		}

		// --- 2. Validate the directory name format ---
		timestampStr := strings.TrimPrefix(dirName, prefix)
		backupTime, err := time.Parse(backupTimeFormat, timestampStr)
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
	savedDay := make(map[string]bool)
	savedWeek := make(map[string]bool)
	savedMonth := make(map[string]bool)

	for _, b := range allBackups {
		if keepHourly {
			// Rule: Hourly backups for the current day
			if b.Time.Year() == now.Year() && b.Time.Month() == now.Month() && b.Time.Day() == now.Day() {
				// This check is implicitly handled by the backupsToKeep map, but is good for clarity
				if _, exists := backupsToKeep[b.Name]; !exists {
					backupsToKeep[b.Name] = true
				}
			}
		}

		// Rule: Daily backups for the last N days
		if keepDailyEnabled {
			dayKey := b.Time.Format("2006-01-02")
			if len(savedDay) < keepDaily && !savedDay[dayKey] {
				backupsToKeep[b.Name] = true
				savedDay[dayKey] = true
			}
		}

		// Rule: Weekly backups for the last N weeks (keep the last backup of Sunday)
		if keepWeeklyEnabled {
			if b.Time.Weekday() == time.Sunday {
				year, week := b.Time.ISOWeek()
				weekKey := fmt.Sprintf("%d-%d", year, week)
				if len(savedWeek) < keepWeekly && !savedWeek[weekKey] {
					backupsToKeep[b.Name] = true
					savedWeek[weekKey] = true
				}
			}
		}

		// Rule: Monthly backups for the last N months (keep the last backup of the month)
		if keepMonthlyEnabled {
			if b.Time.Day() == time.Date(b.Time.Year(), b.Time.Month()+1, 0, 0, 0, 0, 0, b.Time.Location()).Day() {
				monthKey := b.Time.Format("2006-01")
				if len(savedMonth) < keepMonthly && !savedMonth[monthKey] {
					backupsToKeep[b.Name] = true
					savedMonth[monthKey] = true
				}
			}
		}
	}

	// Build a descriptive log message for the retention plan
	var planParts []string
	if keepHourly {
		planParts = append(planParts, "Hourly (for today)")
	}
	if keepDailyEnabled {
		planParts = append(planParts, fmt.Sprintf("%d daily", len(savedDay)))
	}
	if keepWeeklyEnabled {
		planParts = append(planParts, fmt.Sprintf("%d weekly", len(savedWeek)))
	}
	if keepMonthlyEnabled {
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

const backupPrefix = "5ive_Backup_"
const keepHourly = false        // Set to true to keep the latest backup per hour for the current day
const keepDailyEnabled = true   // Set to false to disable daily backups
const keepWeeklyEnabled = true  // Set to false to disable weekly backups
const keepMonthlyEnabled = true // Set to false to disable monthly backups
const keepDays = 7
const keepWeeks = 4
const keepMonths = 12
const backupTimeFormat = "2006-01-02-15-04-05-000"

func main() {
	// Define command-line flags for source and destination directories.
	// The hardcoded values are now used as defaults.
	srcFlag := flag.String("source", "./src_backup", "Source directory to copy from")
	destFlag := flag.String("target", "./dest_backup_mirror", "Base destination directory for backups")
	snapshotFlag := flag.Bool("snapshot", false, "Perform a full snapshot backup. Default is incremental.")

	// Add a flag for robocopy, defaulting to true only on Windows.
	var useRobocopyDefault = false
	if runtime.GOOS == "windows" {
		useRobocopyDefault = true
	}
	useRobocopyFlag := flag.Bool("robocopy", useRobocopyDefault, "Use robocopy for faster sync on Windows (no effect on other OS)")
	flag.Parse()

	// Create a slice to store the log of copied files
	var copiedFiles []string
	fileLogger := func(relPath string) {
		copiedFiles = append(copiedFiles, relPath)
	}

	// --- 1. Define the destination for this specific backup run ---
	baseDestDir := *destFlag
	currentIncrementalDirName := backupPrefix + "current"

	fmt.Printf("--- Starting Backup ---\n")
	fmt.Printf("Source: %s\n", *srcFlag)

	// --- 2. Perform the backup ---
	if *snapshotFlag {
		// SNAPSHOT MODE
		fmt.Println("Mode: Snapshot")
		timestamp := time.Now().Format(backupTimeFormat)
		backupDirName := fmt.Sprintf("%s%s", backupPrefix, timestamp)
		currentDestDir := filepath.Join(baseDestDir, backupDirName)
		fmt.Printf("Destination: %s\n", currentDestDir)
		fmt.Println("------------------------------")

		if *useRobocopyFlag && runtime.GOOS == "windows" {
			log.Println("Using robocopy for snapshot.")
			// For a snapshot, we do a simple copy (/E), not a mirror (/MIR).
			robocopiedFiles, err := syncDirTreeRobocopy(*srcFlag, currentDestDir, false)
			if err != nil {
				if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() >= 8 {
					log.Fatalf("Fatal backup error during robocopy snapshot: %v", err)
				}
			}
			copiedFiles = robocopiedFiles
		} else {
			// In snapshot mode, we use the original blind copy.
			log.Println("Using manual Go implementation for snapshot.")
			if err := SyncDirTree(*srcFlag, currentDestDir, fileLogger); err != nil {
				log.Fatalf("Fatal backup error during snapshot: %v", err)
			}
		}

	} else {
		// INCREMENTAL MODE (DEFAULT)
		// This is the default behavior when --snapshot is not provided.
		// It performs a fast, incremental backup to a 'current' directory,
		// which is rolled over daily.

		fmt.Println("Mode: Incremental")
		if err := handleRollover(baseDestDir, backupPrefix, currentIncrementalDirName); err != nil {
			log.Fatalf("Error during backup rollover: %v", err)
		}

		currentDestDir := filepath.Join(baseDestDir, currentIncrementalDirName)
		fmt.Printf("Destination: %s\n", currentDestDir)
		fmt.Println("------------------------------")

		if *useRobocopyFlag && runtime.GOOS == "windows" {
			// Use the much faster robocopy for incremental sync on Windows.
			log.Println("Using robocopy for synchronization.")
			robocopiedFiles, err := syncDirTreeRobocopy(*srcFlag, currentDestDir, true)
			if err != nil {
				// Check for robocopy's specific success exit codes.
				// Exit codes < 8 are considered success.
				if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() >= 8 {
					log.Fatalf("Fatal backup error during robocopy sync: %v", err)
				}
			}
			// The file list is gathered from the dry run.
			copiedFiles = robocopiedFiles
		} else {
			// Use the pure Go implementation for synchronization.
			log.Println("Using manual Go implementation for synchronization.")
			if err := SyncDirTree(*srcFlag, currentDestDir, fileLogger); err != nil {
				log.Fatalf("Fatal backup error during sync: %v", err)
			}
		}

		// Touch a meta file to update the modification time of the backup set.
		metaFilePath := filepath.Join(currentDestDir, ".ppbackup_meta")
		if f, err := os.Create(metaFilePath); err != nil {
			log.Printf("Warning: could not update metafile timestamp: %v", err)
		} else {
			f.Close()
		}
	}

	fmt.Println("\nBackup operation completed.")

	// --- Print the Log of Copied Files ---
	fmt.Println("\n--- Log of Copied Files ---")
	if len(copiedFiles) == 0 {
		fmt.Println("No new or modified files were copied.")
	} else {
		for _, file := range copiedFiles {
			fmt.Printf("COPIED: %s\n", file)
		}
	}

	// --- 3. Clean up old backups ---
	fmt.Println("\n--- Cleaning Up Old Backups ---")
	if err := cleanupOldBackups(baseDestDir, backupPrefix, keepDays, keepWeeks, keepMonths, currentIncrementalDirName); err != nil {
		// We log this as a non-fatal error because the main backup was successful.
		log.Printf("Error during cleanup: %v", err)
	}
}
