package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/engine"
	"pixelgardenlabs.io/pgl-backup/pkg/filelock"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// version holds the application's version string.
// It's a `var` so it can be set at compile time using ldflags.
// Example: go build -ldflags="-X main.version=1.0.0"
var version = "dev"

// action defines a special command to execute instead of a backup.
type action int

const (
	actionRunBackup action = iota // The default action is to run a backup.
	actionShowVersion
	actionInitConfig
)

// init is called before main. We use it to set up a custom, more descriptive
// help message for the command-line flags.
func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s (version %s):\n", os.Args[0], version)
		fmt.Fprintf(flag.CommandLine.Output(), "A simple and powerful file backup utility with snapshot and incremental modes.\n\n")
		flag.PrintDefaults()
	}
}

// parseFlagConfig defines and parses command-line flags, and constructs a
// configuration object containing only the values provided by those flags.
func parseFlagConfig() (config.Config, action, error) {
	// Define flags with zero-value defaults. We will merge them later.
	srcFlag := flag.String("source", "", "Source directory to copy from")
	targetFlag := flag.String("target", "", "Base destination directory for backups")
	modeFlag := flag.String("mode", "", "Backup mode: 'incremental' or 'snapshot'.")
	quietFlag := flag.Bool("quiet", false, "Suppress individual file operation logs.")
	dryRunFlag := flag.Bool("dryrun", false, "Show what would be done without making any changes.")
	initFlag := flag.Bool("init", false, "Generate a default pgl-backup.conf file and exit.")
	versionFlag := flag.Bool("version", false, "Print the application version and exit.")
	syncEngineFlag := flag.String("sync-engine", "", "Sync engine to use: 'native' or 'robocopy' (Windows only).")
	nativeEngineWorkersFlag := flag.Int("native-engine-workers", 0, "Number of worker goroutines for native sync.")
	nativeRetryCountFlag := flag.Int("native-retry-count", 0, "Number of retries for failed file copies in native engine.")
	nativeRetryWaitFlag := flag.Int("native-retry-wait", 0, "Seconds to wait between retries in native engine.")
	excludeFilesFlag := flag.String("exclude-files", "", "Comma-separated list of file names to exclude (supports glob patterns).")
	excludeDirsFlag := flag.String("exclude-dirs", "", "Comma-separated list of directory names to exclude (supports glob patterns).")
	preserveSourceNameFlag := flag.Bool("preserve-source-name", false, "Preserve the source directory's name in the destination path.")

	flag.Parse()

	// Create a config struct populated *only* with values from flags.
	flagConfig := config.Config{
		Paths: config.BackupPathConfig{
			Source:                      *srcFlag,
			TargetBase:                  *targetFlag,
			PreserveSourceDirectoryName: *preserveSourceNameFlag,
		},
		Engine: config.BackupEngineConfig{
			NativeEngineWorkers:          *nativeEngineWorkersFlag,
			NativeEngineRetryCount:       *nativeRetryCountFlag,
			NativeEngineRetryWaitSeconds: *nativeRetryWaitFlag,
		},
		DryRun: *dryRunFlag,
		Quiet:  *quietFlag,
	}

	// If the exclude-files flag was set, parse it and override the config.
	if *excludeFilesFlag != "" {
		parts := strings.Split(*excludeFilesFlag, ",")
		flagConfig.Paths.ExcludeFiles = make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				flagConfig.Paths.ExcludeFiles = append(flagConfig.Paths.ExcludeFiles, trimmed)
			}
		}
	}

	// If the exclude-dirs flag was set, parse it and override the config.
	if *excludeDirsFlag != "" {
		parts := strings.Split(*excludeDirsFlag, ",")
		flagConfig.Paths.ExcludeDirs = make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				flagConfig.Paths.ExcludeDirs = append(flagConfig.Paths.ExcludeDirs, trimmed)
			}
		}
	}

	// Parse string flags into their corresponding enum types.
	if *modeFlag != "" {
		mode, err := config.BackupModeFromString(*modeFlag)
		if err != nil {
			return config.Config{}, actionRunBackup, err
		}
		flagConfig.Mode = mode
	}

	if *syncEngineFlag != "" {
		engineType, err := config.SyncEngineFromString(*syncEngineFlag)
		if err != nil {
			return config.Config{}, actionRunBackup, err
		}
		flagConfig.Engine.Type = engineType
	}

	// Final sanity check: ensure robocopy is disabled if not on Windows.
	if runtime.GOOS != "windows" && flagConfig.Engine.Type == config.RobocopyEngine {
		plog.Warn("Robocopy is not available on this OS. Forcing 'native' sync engine.")
		flagConfig.Engine.Type = config.NativeEngine
	}

	// Determine which action to take based on flags.
	if *versionFlag {
		return flagConfig, actionShowVersion, nil
	}
	if *initFlag {
		return flagConfig, actionInitConfig, nil
	}
	return flagConfig, actionRunBackup, nil
}

// acquireTargetLock ensures the target directory exists and acquires a file lock within it.
// It returns a release function that must be called to unlock the directory.
func acquireTargetLock(ctx context.Context, targetPath, sourcePath string) (func(), error) {
	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create target directory %s: %w", targetPath, err)
	}

	lockFilePath := filepath.Join(targetPath, config.LockFileName)
	appID := fmt.Sprintf("pgl-backup:%s", sourcePath)

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

// executeBackup handles the complete workflow for running a backup.
func executeBackup(ctx context.Context, flagConfig config.Config) error {
	// Now, load the config from the (now locked) target directory, or load the defaults.
	loadedConfig, err := config.Load(flagConfig.Paths.TargetBase)
	if err != nil {
		return fmt.Errorf("failed to load configuration from target: %w", err)
	}

	// Merge the flag values over the loaded config to get the final run config.
	runConfig := config.MergeConfigWithFlags(loadedConfig, flagConfig)

	// If not in quiet mode, log the final configuration for user confirmation.
	runConfig.LogSummary()

	// Perform final validation on the merged configuration.
	if err := runConfig.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	backupEngine := engine.New(runConfig, version)
	return backupEngine.Execute(ctx)
}

// run encapsulates the main application logic and returns an error if something
// goes wrong, allowing the main function to handle exit codes.
func run(ctx context.Context) error {
	// --- 1. Parse command-line flags ---
	// This is done only once. It gives us the user's explicit command-line intent.
	flagConfig, action, err := parseFlagConfig()
	if err != nil {
		return err
	}

	// Handle actions that don't need a config file or lock.
	if action == actionShowVersion {
		fmt.Printf("pgl-backup version %s\n", version)
		return nil
	}

	// For init or backup, the target flag is mandatory.
	if flagConfig.Paths.TargetBase == "" {
		return fmt.Errorf("the -target flag is required for this operation")
	}

	// --- 2. Acquire Lock on Target Directory ---
	releaseLock, err := acquireTargetLock(ctx, flagConfig.Paths.TargetBase, flagConfig.Paths.Source)
	if err != nil {
		return err // A real error occurred during lock acquisition.
	}
	if releaseLock == nil {
		return nil // Lock was already held, exit gracefully.
	}
	defer releaseLock()

	// --- 3. Execute the requested action ---
	switch action {
	case actionShowVersion:
		// This case is handled above, but we keep it here for exhaustive switch.
		return nil // Should not be reached.
	case actionInitConfig:
		// Create a base default config.
		baseConfig := config.NewDefault()
		// Merge the flags provided by the user on top of the defaults.
		configToGenerate := config.MergeConfigWithFlags(baseConfig, flagConfig)
		return config.Generate(flagConfig.Paths.TargetBase, configToGenerate)
	case actionRunBackup:
		return executeBackup(ctx, flagConfig)
	default:
		return fmt.Errorf("internal error: unknown action %d", action)
	}
}

func main() {
	// Set up a context that is canceled when an interrupt signal is received.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for interrupt signals (like Ctrl+C) in a separate goroutine.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		cancel()
	}()

	if err := run(ctx); err != nil {
		plog.Error("Application failed", "error", err)
		os.Exit(1)
	}
}
