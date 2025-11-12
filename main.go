package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// parseAndBuildConfig parses command-line flags and constructs the backupConfig.
func parseAndBuildConfig() backupConfig {
	// Define command-line flags for source and destination directories.
	// The hardcoded values are now used as defaults.
	srcFlag := flag.String("source", defaultConfig.Paths.Source, "Source directory to copy from")
	destFlag := flag.String("target", defaultConfig.Paths.TargetBase, "Base destination directory for backups")
	snapshotFlag := flag.Bool("snapshot", false, "Perform a full snapshot backup. Default is incremental.")

	// Add a flag for robocopy, defaulting to true only on Windows.
	var useRobocopyDefault = false
	if runtime.GOOS == "windows" {
		useRobocopyDefault = true
	}
	useRobocopyFlag := flag.Bool("robocopy", useRobocopyDefault, "Use robocopy for faster sync on Windows (no effect on other OS)")
	flag.Parse()

	// Create a final config for this run, overriding defaults with flag values.
	runConfig := defaultConfig
	runConfig.Paths.Source = *srcFlag
	runConfig.Paths.TargetBase = *destFlag
	if *snapshotFlag {
		runConfig.Mode = snapshotMode
	} else {
		runConfig.Mode = incrementalMode
	}

	runConfig.UseRobocopy = *useRobocopyFlag
	return runConfig
}

// runBackupJob executes the backup process based on the provided configuration.
func runBackupJob(runConfig backupConfig) ([]string, error) {
	// Create a slice to store the log of copied files
	var copiedFiles []string
	fileLogger := func(relPath string) {
		copiedFiles = append(copiedFiles, relPath)
	}

	fmt.Printf("--- Starting Backup ---\n")
	fmt.Printf("Source: %s\n", runConfig.Paths.Source)
	fmt.Printf("Mode: %s\n", runConfig.Mode)

	// --- 1. Pre-backup tasks (rollover) and destination calculation ---
	if runConfig.Mode == snapshotMode {
		// SNAPSHOT MODE
		timestamp := time.Now().Format(runConfig.Naming.TimeFormat)
		backupDirName := fmt.Sprintf("%s%s", runConfig.Naming.Prefix, timestamp)
		runConfig.Paths.CurrentTarget = filepath.Join(runConfig.Paths.TargetBase, backupDirName)
	} else {
		// INCREMENTAL MODE (DEFAULT)
		if err := handleRollover(runConfig); err != nil {
			return nil, fmt.Errorf("error during backup rollover: %w", err)
		}
		currentIncrementalDirName := runConfig.Naming.Prefix + runConfig.Naming.IncrementalModeSuffix
		runConfig.Paths.CurrentTarget = filepath.Join(runConfig.Paths.TargetBase, currentIncrementalDirName)
	}

	fmt.Printf("Destination: %s\n", runConfig.Paths.CurrentTarget)
	fmt.Println("------------------------------")

	// --- 2. Perform the backup ---
	if runConfig.Mode == snapshotMode {
		if runConfig.UseRobocopy && runtime.GOOS == "windows" {
			log.Println("Using robocopy for snapshot.")
			robocopiedFiles, err := syncDirTreeRobocopy(runConfig.Paths.Source, runConfig.Paths.CurrentTarget, false)
			if err != nil && (!isRobocopySuccess(err)) {
				return nil, fmt.Errorf("fatal backup error during robocopy snapshot: %w", err)
			}
			copiedFiles = robocopiedFiles
		} else {
			log.Println("Using manual Go implementation for snapshot.")
			if err := syncDirTree(runConfig.Paths.Source, runConfig.Paths.CurrentTarget, fileLogger); err != nil {
				return nil, fmt.Errorf("fatal backup error during snapshot: %w", err)
			}
		}
	} else { // Incremental Mode
		if runConfig.UseRobocopy && runtime.GOOS == "windows" {
			log.Println("Using robocopy for synchronization.")
			robocopiedFiles, err := syncDirTreeRobocopy(runConfig.Paths.Source, runConfig.Paths.CurrentTarget, true)
			if err != nil && (!isRobocopySuccess(err)) {
				return nil, fmt.Errorf("fatal backup error during robocopy sync: %w", err)
			}
			copiedFiles = robocopiedFiles
		} else {
			log.Println("Using manual Go implementation for synchronization.")
			if err := syncDirTree(runConfig.Paths.Source, runConfig.Paths.CurrentTarget, fileLogger); err != nil {
				return nil, fmt.Errorf("fatal backup error during sync: %w", err)
			}
		}

		// Touch a meta file to update the modification time of the backup set.
		metaFilePath := filepath.Join(runConfig.Paths.CurrentTarget, ".ppbackup_meta")
		if f, err := os.Create(metaFilePath); err != nil {
			log.Printf("Warning: could not update metafile timestamp: %v", err)
		} else {
			f.Close()
		}
	}

	fmt.Println("\nBackup operation completed.")
	return copiedFiles, nil
}

func main() {
	// 1. Parse flags and build the configuration for this run.
	runConfig := parseAndBuildConfig()

	// 2. Run the backup job.
	copiedFiles, err := runBackupJob(runConfig)
	if err != nil {
		log.Fatalf("Fatal backup error: %v", err)
	}

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
	if err := cleanupOldBackups(runConfig); err != nil { // cleanupOldBackups is still called directly from main
		// We log this as a non-fatal error because the main backup was successful.
		log.Printf("Error during cleanup: %v", err)
	}
}
