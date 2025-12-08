package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/engine"
	"pixelgardenlabs.io/pgl-backup/pkg/flagparse"
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
func parseFlagConfig() (action, map[string]interface{}, error) {
	// Define flags with zero-value defaults. We will merge them later.
	srcFlag := flag.String("source", "", "Source directory to copy from")
	targetFlag := flag.String("target", "", "Base destination directory for backups")
	modeFlag := flag.String("mode", "", "Backup mode: 'incremental' or 'snapshot'.")
	logLevelFlag := flag.String("log-level", "info", "Set the logging level: 'debug', 'info', 'warn', 'error'.")
	failFastFlag := flag.Bool("fail-fast", false, "Stop the backup immediately on the first file sync error.")
	dryRunFlag := flag.Bool("dry-run", false, "Show what would be done without making any changes.")
	metricsFlag := flag.Bool("metrics", false, "Enable detailed performance and file-counting metrics.")
	initFlag := flag.Bool("init", false, "Generate a default pgl-backup.conf file and exit.")
	versionFlag := flag.Bool("version", false, "Print the application version and exit.")
	syncEngineFlag := flag.String("sync-engine", "", "Sync engine to use: 'native' or 'robocopy' (Windows only).")
	syncWorkersFlag := flag.Int("sync-workers", 0, "Number of worker goroutines for file synchronization.")
	mirrorWorkersFlag := flag.Int("mirror-workers", 0, "Number of worker goroutines for file deletions in mirror mode.")
	deleteWorkersFlag := flag.Int("delete-workers", 0, "Number of worker goroutines for deleting outdated backups.")
	retryCountFlag := flag.Int("retry-count", 0, "Number of retries for failed file copies.")
	retryWaitFlag := flag.Int("retry-wait", 0, "Seconds to wait between retries.")
	copyBufferKBFlag := flag.Int("copy-buffer-kb", 0, "Size of the I/O buffer in kilobytes for file copies.")
	excludeFilesFlag := flag.String("exclude-files", "", "Comma-separated list of file names to exclude (supports glob patterns).")
	excludeDirsFlag := flag.String("exclude-dirs", "", "Comma-separated list of directory names to exclude (supports glob patterns).")
	preserveSourceNameFlag := flag.Bool("preserve-source-name", true, "Preserve the source directory's name in the destination path. Set to false to sync contents directly.")
	preBackupHooksFlag := flag.String("pre-backup-hooks", "", "Comma-separated list of commands to run before the backup.")
	postBackupHooksFlag := flag.String("post-backup-hooks", "", "Comma-separated list of commands to run after the backup.")

	flag.Parse()

	// Create a map of the flags that were explicitly set by the user, along with their values.
	// This map is used to selectively override the base configuration.
	setFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		setFlags[f.Name] = true // Mark the flag as set.
	})

	// Now, iterate over the set flags and populate a new map with their actual values.
	flagMap := make(map[string]interface{})
	for name := range setFlags {
		switch name {
		case "mode":
			mode, err := config.BackupModeFromString(*modeFlag)
			if err != nil {
				return actionRunBackup, nil, err
			}
			flagMap[name] = mode
		case "sync-engine":
			engineType, err := config.SyncEngineFromString(*syncEngineFlag)
			if err != nil {
				return actionRunBackup, nil, err
			}
			flagMap[name] = engineType
		case "exclude-files":
			flagMap[name] = flagparse.ParseExcludeList(*excludeFilesFlag)
		case "exclude-dirs":
			flagMap[name] = flagparse.ParseExcludeList(*excludeDirsFlag)
		case "pre-backup-hooks":
			flagMap[name] = flagparse.ParseCmdList(*preBackupHooksFlag)
		case "post-backup-hooks":
			flagMap[name] = flagparse.ParseCmdList(*postBackupHooksFlag)
		case "source":
			flagMap[name] = *srcFlag
		case "target":
			flagMap[name] = *targetFlag
		case "log-level":
			flagMap[name] = *logLevelFlag
		case "fail-fast":
			flagMap[name] = *failFastFlag
		case "dry-run":
			flagMap[name] = *dryRunFlag
		case "metrics":
			flagMap[name] = *metricsFlag
		case "preserve-source-name":
			// The flag's value is only used if it was explicitly set.
			// The default is handled by the base config.
			// We can just assign the value directly.
			flagMap[name] = *preserveSourceNameFlag
		case "sync-workers":
			flagMap[name] = *syncWorkersFlag
		case "mirror-workers":
			flagMap[name] = *mirrorWorkersFlag
		case "delete-workers":
			flagMap[name] = *deleteWorkersFlag
		case "retry-count":
			flagMap[name] = *retryCountFlag
		case "retry-wait":
			flagMap[name] = *retryWaitFlag
		case "copy-buffer-kb":
			flagMap[name] = *copyBufferKBFlag
		}
	}

	// Final sanity check: if robocopy was requested on a non-windows OS, force native.
	if runtime.GOOS != "windows" {
		if val, ok := flagMap["sync-engine"]; ok && val.(config.SyncEngine) == config.RobocopyEngine {
			plog.Warn("Robocopy is not available on this OS. Forcing 'native' sync engine.")
			flagMap["sync-engine"] = config.NativeEngine
		}
	}

	// Determine which action to take based on flags.
	if *versionFlag {
		return actionShowVersion, flagMap, nil
	}
	if *initFlag {
		return actionInitConfig, flagMap, nil
	}
	return actionRunBackup, flagMap, nil
}

// run encapsulates the main application logic and returns an error if something
// goes wrong, allowing the main function to handle exit codes.
func run(ctx context.Context) error {
	plog.Info("pgl-backup starting", "version", version, "pid", os.Getpid())

	// --- 1. Parse command-line flags ---
	// This is done only once. It gives us the user's explicit command-line intent.
	action, flagMap, err := parseFlagConfig()
	if err != nil {
		return err
	}

	switch action {
	case actionShowVersion:
		fmt.Printf("pgl-backup version %s\n", version)
		return nil
	case actionInitConfig:
		// For init, source and target flags are mandatory.
		if _, ok := flagMap["target"]; !ok {
			return fmt.Errorf("the -target flag is required for the init operation")
		}
		if _, ok := flagMap["source"]; !ok {
			return fmt.Errorf("the -source flag is required for the init operation")
		}

		// Create a config from defaults merged with user flags.
		runConfig := config.MergeConfigWithFlags(config.NewDefault(), flagMap)

		initEngine := engine.New(runConfig, version)
		return initEngine.InitializeBackupTarget(ctx)
	case actionRunBackup:
		// For backup, the target flag is mandatory.
		targetPath, ok := flagMap["target"].(string)
		if !ok || targetPath == "" {
			return fmt.Errorf("the -target flag is required to run a backup")
		}

		// Load config from the target directory, or use defaults if not found.
		loadedConfig, err := config.Load(targetPath)
		if err != nil {
			return fmt.Errorf("failed to load configuration from target: %w", err)
		}

		// Merge the flag values over the loaded config to get the final run config.
		runConfig := config.MergeConfigWithFlags(loadedConfig, flagMap)

		// Set the global log level based on the final configuration.
		plog.SetLevel(plog.LevelFromString(runConfig.LogLevel))

		runConfig.LogSummary()

		startTime := time.Now()
		backupEngine := engine.New(runConfig, version)
		err = backupEngine.ExecuteBackup(ctx)
		duration := time.Since(startTime).Round(time.Millisecond)

		if err != nil {
			plog.Info("Backup process finished with an error.", "duration", duration)
			return err // The error will be logged by main()
		}
		plog.Info("Backup process finished successfully.", "duration", duration)
		return nil
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
