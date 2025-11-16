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

// version holds the application's version string.
// It's a `var` so it can be set at compile time using ldflags.
// Example: go build -ldflags="-X main.version=1.0.0"
var version = "dev"

// init is called before main. We use it to set up a custom, more descriptive
// help message for the command-line flags.
func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s (version %s):\n", os.Args[0], version)
		fmt.Fprintf(flag.CommandLine.Output(), "A simple and powerful file backup utility with snapshot and incremental modes.\n\n")
		flag.PrintDefaults()
	}
}

// runBackupJob executes the backup process based on the provided configuration.
func runBackupJob(runConfig backupConfig) error {
	if runConfig.DryRun {
		log.Println("--- Starting Backup (DRY RUN) ---")
	} else {
		log.Println("--- Starting Backup ---")
	}
	log.Printf("Source: %s", runConfig.Paths.Source)
	log.Printf("Mode: %s", runConfig.Mode)

	// --- 1. Pre-backup tasks (rollover) and destination calculation ---
	if runConfig.Mode == SnapshotMode {
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

	log.Printf("Destination: %s", runConfig.Paths.CurrentTarget)
	log.Println("------------------------------")

	// --- 2. Perform the backup ---
	if err := handleSync(runConfig); err != nil {
		return fmt.Errorf("fatal backup error during sync: %w", err)
	}

	log.Println("Backup operation completed.")

	// --- 3. Clean up old backups ---
	if err := handleRetention(runConfig); err != nil {
		// We log this as a non-fatal error because the main backup was successful.
		log.Printf("Error applying retention policy: %v", err)
	}
	return nil
}

// finalizeConfig takes the base configuration (from file or default) and the parsed
// command-line flags, and constructs the final configuration for the backup job.
func finalizeConfig(baseConfig backupConfig, src, target, mode, syncEngine *string, quiet, dryrun *bool, nativeEngineWorkers *int) (backupConfig, error) {
	runConfig := baseConfig
	runConfig.Paths.Source = *src
	runConfig.Paths.TargetBase = *target

	switch *mode {
	case "snapshot":
		runConfig.Mode = SnapshotMode
	case "incremental":
		runConfig.Mode = IncrementalMode
	default:
		return backupConfig{}, fmt.Errorf("invalid mode: %q. Must be 'incremental' or 'snapshot'", *mode)
	}

	switch *syncEngine {
	case "native":
		runConfig.Engine.Type = NativeEngine
	case "robocopy":
		runConfig.Engine.Type = RobocopyEngine
	default:
		return backupConfig{}, fmt.Errorf("invalid syncEngine: %q. Must be 'native' or 'robocopy'", *syncEngine)
	}

	runConfig.DryRun = *dryrun
	runConfig.Quiet = *quiet
	runConfig.Engine.NativeEngineWorkers = *nativeEngineWorkers

	if runConfig.Engine.NativeEngineWorkers < 1 {
		return backupConfig{}, fmt.Errorf("nativeEngineWorkers must be at least 1")
	}

	// Final sanity check: ensure robocopy is disabled if not on Windows.
	if runtime.GOOS != "windows" && runConfig.Engine.Type == RobocopyEngine {
		log.Println("Robocopy is not available on this OS. Forcing 'native' sync engine.")
		runConfig.Engine.Type = NativeEngine
	}

	return runConfig, nil
}

// run encapsulates the main application logic and returns an error if something
// goes wrong, allowing the main function to handle exit codes.
func run() error {
	// 1. Load base configuration from file, if it exists.
	loadedConfig, err := loadConfig()
	if err != nil {
		log.Printf("Warning: could not parse ppBackup.conf: %v. Using defaults.", err)
		loadedConfig = newDefaultConfig()
	}

	// 2. Define all command-line flags, using the loaded config for defaults.
	srcFlag := flag.String("source", loadedConfig.Paths.Source, "Source directory to copy from")
	destFlag := flag.String("target", loadedConfig.Paths.TargetBase, "Base destination directory for backups")
	modeFlag := flag.String("mode", loadedConfig.Mode.String(), "Set the backup mode: 'incremental' or 'snapshot'.")
	quietFlag := flag.Bool("quiet", loadedConfig.Quiet, "Suppress individual file operation logs.")
	dryRunFlag := flag.Bool("dryrun", loadedConfig.DryRun, "Show what would be done without making any changes.")
	initFlag := flag.Bool("init", false, "Generate a default ppBackup.conf file and exit.")
	versionFlag := flag.Bool("version", false, "Print the application version and exit.")
	syncEngineFlag := flag.String("syncEngine", loadedConfig.Engine.Type.String(), "Sync engine to use: 'native' or 'robocopy' (Windows only).")
	nativeEngineWorkersFlag := flag.Int("nativeEngineWorkers", loadedConfig.Engine.NativeEngineWorkers, "Number of worker goroutines for native sync.")

	flag.Parse()

	// 3. Handle immediate action flags that do not perform a backup.
	if *versionFlag {
		fmt.Printf("ppBackup version %s\n", version)
		return nil
	}
	if *initFlag {
		return generateConfig()
	}

	// 4. If not an action flag, build the final config and run the backup job.
	runConfig, err := finalizeConfig(loadedConfig, srcFlag, destFlag, modeFlag, syncEngineFlag, quietFlag, dryRunFlag, nativeEngineWorkersFlag)
	if err != nil {
		return err // Return configuration error
	}
	if err := runBackupJob(runConfig); err != nil {
		return fmt.Errorf("fatal backup error: %w", err)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
