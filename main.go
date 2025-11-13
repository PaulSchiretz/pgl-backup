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

// parseBackupConfig parses command-line flags and constructs the backupConfig.
func parseBackupConfig() backupConfig {
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
func runBackupJob(runConfig backupConfig) error {
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
			return fmt.Errorf("error during backup rollover: %w", err)
		}
		currentIncrementalDirName := runConfig.Naming.Prefix + runConfig.Naming.IncrementalModeSuffix
		runConfig.Paths.CurrentTarget = filepath.Join(runConfig.Paths.TargetBase, currentIncrementalDirName)
	}

	fmt.Printf("Destination: %s\n", runConfig.Paths.CurrentTarget)
	fmt.Println("------------------------------")

	// --- 2. Perform the backup ---
	if err := handleSync(runConfig); err != nil {
		return fmt.Errorf("fatal backup error during sync: %w", err)
	}

	fmt.Println("\nBackup operation completed.")

	// --- 3. Clean up old backups ---
	fmt.Println("\n--- Cleaning Up Old Backups ---")
	if err := handleRetention(runConfig); err != nil {
		// We log this as a non-fatal error because the main backup was successful.
		log.Printf("Error applying retention policy: %v", err)
	}
	return nil
}

// run encapsulates the main application logic and returns an error if something
// goes wrong, allowing the main function to handle exit codes.
func run() error {
	// 1. Parse flags and build the configuration for this run.
	runConfig := parseBackupConfig()

	// 2. Run the backup job.
	if err := runBackupJob(runConfig); err != nil {
		return fmt.Errorf("fatal backup error: %w", err)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
}
