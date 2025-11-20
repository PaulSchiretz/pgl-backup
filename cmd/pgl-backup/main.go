package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/engine"
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

// buildRunConfig defines and parses command-line flags, using a base
// configuration for defaults. It then constructs and returns the final,
// effective configuration for the application to use.
func buildRunConfig(baseConfig config.Config) (config.Config, action, error) {
	// Define flags, using the base config for default values.
	srcFlag := flag.String("source", baseConfig.Paths.Source, "Source directory to copy from")
	targetFlag := flag.String("target", baseConfig.Paths.TargetBase, "Base destination directory for backups")
	modeFlag := flag.String("mode", baseConfig.Mode.String(), "Set the backup mode: 'incremental' or 'snapshot'.")
	quietFlag := flag.Bool("quiet", baseConfig.Quiet, "Suppress individual file operation logs.")
	dryRunFlag := flag.Bool("dryrun", baseConfig.DryRun, "Show what would be done without making any changes.")
	initFlag := flag.Bool("init", false, "Generate a default pgl-backup.conf file and exit.")
	versionFlag := flag.Bool("version", false, "Print the application version and exit.")
	syncEngineFlag := flag.String("sync-engine", baseConfig.Engine.Type.String(), "Sync engine to use: 'native' or 'robocopy' (Windows only).")
	nativeEngineWorkersFlag := flag.Int("native-engine-workers", baseConfig.Engine.NativeEngineWorkers, "Number of worker goroutines for native sync.")
	nativeRetryCountFlag := flag.Int("native-retry-count", baseConfig.Engine.NativeEngineRetryCount, "Number of retries for failed file copies in native engine.")
	nativeRetryWaitFlag := flag.Int("native-retry-wait", baseConfig.Engine.NativeEngineRetryWaitSeconds, "Seconds to wait between retries in native engine.")
	excludeFilesFlag := flag.String("exclude-files", strings.Join(baseConfig.Paths.ExcludeFiles, ","), "Comma-separated list of file names to exclude (supports glob patterns).")
	excludeDirsFlag := flag.String("exclude-dirs", strings.Join(baseConfig.Paths.ExcludeDirs, ","), "Comma-separated list of directory names to exclude (supports glob patterns).")

	flag.Parse()

	// Overwrite the base config with any values provided by flags.
	baseConfig.Paths.Source = *srcFlag
	baseConfig.Paths.TargetBase = *targetFlag
	baseConfig.DryRun = *dryRunFlag
	baseConfig.Quiet = *quietFlag
	baseConfig.Engine.NativeEngineWorkers = *nativeEngineWorkersFlag
	baseConfig.Engine.NativeEngineRetryCount = *nativeRetryCountFlag
	baseConfig.Engine.NativeEngineRetryWaitSeconds = *nativeRetryWaitFlag

	// If the ignore flag was set, parse it and override the config.
	if *excludeFilesFlag != strings.Join(baseConfig.Paths.ExcludeFiles, ",") {
		baseConfig.Paths.ExcludeFiles = strings.Split(strings.TrimSpace(*excludeFilesFlag), ",")
	}

	// If the ignore flag was set, parse it and override the config.
	if *excludeDirsFlag != strings.Join(baseConfig.Paths.ExcludeDirs, ",") {
		baseConfig.Paths.ExcludeDirs = strings.Split(strings.TrimSpace(*excludeDirsFlag), ",")
	}

	// Parse string flags into their corresponding enum types.
	mode, err := config.BackupModeFromString(*modeFlag)
	if err != nil {
		return config.Config{}, actionRunBackup, err
	}
	baseConfig.Mode = mode

	engineType, err := config.SyncEngineFromString(*syncEngineFlag)
	if err != nil {
		return config.Config{}, actionRunBackup, err
	}
	baseConfig.Engine.Type = engineType

	// Final sanity check: ensure robocopy is disabled if not on Windows.
	if runtime.GOOS != "windows" && baseConfig.Engine.Type == config.RobocopyEngine {
		plog.Warn("Robocopy is not available on this OS. Forcing 'native' sync engine.")
		baseConfig.Engine.Type = config.NativeEngine
	}

	// Determine which action to take based on flags.
	if *versionFlag {
		return baseConfig, actionShowVersion, nil
	}
	if *initFlag {
		return baseConfig, actionInitConfig, nil
	}
	return baseConfig, actionRunBackup, nil
}

// run encapsulates the main application logic and returns an error if something
// goes wrong, allowing the main function to handle exit codes.
func run(ctx context.Context) error {
	loadedConfig, err := config.Load()
	// If a config file exists but is invalid, we should fail fast.
	// Running with defaults when a config is present but broken is unexpected.
	// We ignore os.IsNotExist, as that simply means we'll use the defaults.
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to load configuration file 'pgl-backup.conf': %w. Please fix the file or remove it to use defaults", err)
	}

	runConfig, actionToRun, err := buildRunConfig(loadedConfig)
	if err != nil {
		return err
	}

	switch actionToRun {
	case actionShowVersion:
		fmt.Printf("pgl-backup version %s\n", version)
		return nil
	case actionInitConfig:
		return config.Generate()
	case actionRunBackup:
		// If not in quiet mode, log the final configuration for user confirmation.
		runConfig.LogSummary()

		// Perform final validation on the merged configuration.
		if err := runConfig.Validate(); err != nil {
			return fmt.Errorf("invalid configuration: %w", err)
		}

		backupEngine := engine.New(runConfig, version)
		return backupEngine.Execute(ctx)
	default:
		return fmt.Errorf("internal error: unknown action %d", actionToRun)
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
