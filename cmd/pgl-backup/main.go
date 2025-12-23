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

// appName is the canonical name of the application used for logging.
const appName = "PGL-Backup"

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
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s (version %s):\n", appName, version)
		fmt.Fprintf(flag.CommandLine.Output(), "A simple and powerful file backup utility with snapshot and incremental modes.\n\n")
		flag.PrintDefaults()
	}
}

// parseFlagConfig defines and parses command-line flags, and constructs a
// configuration object containing only the values provided by those flags.
func parseFlagConfig() (action, map[string]interface{}, error) {
	// --- Flag Design Philosophy ---
	// Flags are exposed for options that are useful to override for a single run
	// (e.g., -dry-run, -mode=snapshot, -log-level=debug).
	//
	// Core strategic options that define the long-term behavior of a backup set
	// (e.g., retention policies, rollover intervals) do not have corresponding flags.
	// These should be set consistently in the pgl-backup.conf file to ensure
	// predictable behavior over time.

	// Define flags with zero-value defaults. We will merge them later.
	srcFlag := flag.String("source", "", "Source directory to copy from")
	targetFlag := flag.String("target", "", "Base destination directory for backups")
	modeFlag := flag.String("mode", "incremental", "Backup mode: 'incremental' or 'snapshot'.")
	logLevelFlag := flag.String("log-level", "info", "Set the logging level: 'debug', 'notice', 'info', 'warn', 'error'.")
	failFastFlag := flag.Bool("fail-fast", false, "Stop the backup immediately on the first file sync error.")
	dryRunFlag := flag.Bool("dry-run", false, "Show what would be done without making any changes.")
	metricsFlag := flag.Bool("metrics", false, "Enable detailed performance and file-counting metrics.")
	initFlag := flag.Bool("init", false, "Generate a default pgl-backup.conf file and exit.")
	versionFlag := flag.Bool("version", false, "Print the application version and exit.")
	syncEngineFlag := flag.String("sync-engine", "native", "Sync engine to use: 'native' or 'robocopy' (Windows only).")
	syncWorkersFlag := flag.Int("sync-workers", 0, "Number of worker goroutines for file synchronization.")
	mirrorWorkersFlag := flag.Int("mirror-workers", 0, "Number of worker goroutines for file deletions in mirror mode.")
	deleteWorkersFlag := flag.Int("delete-workers", 0, "Number of worker goroutines for deleting outdated backups.")
	compressWorkersFlag := flag.Int("compress-workers", 0, "Number of worker goroutines for compressing backups.")
	retryCountFlag := flag.Int("retry-count", 0, "Number of retries for failed file copies.")
	retryWaitFlag := flag.Int("retry-wait", 0, "Seconds to wait between retries.")
	bufferSizeKBFlag := flag.Int("buffer-size-kb", 0, "Size of the I/O buffer in kilobytes for file copies and compression.")
	modTimeWindowFlag := flag.Int("mod-time-window", 1, "Time window in seconds to consider file modification times equal (0=exact).")
	userExcludeFilesFlag := flag.String("user-exclude-files", "", "Comma-separated list of case-insensitiv file names to exclude (supports glob patterns).")
	userExcludeDirsFlag := flag.String("user-exclude-dirs", "", "Comma-separated list of case-insensitiv directory names to exclude (supports glob patterns).")
	preserveSourceNameFlag := flag.Bool("preserve-source-name", true, "Preserve the source directory's name in the destination path. Set to false to sync contents directly.")
	preBackupHooksFlag := flag.String("pre-backup-hooks", "", "Comma-separated list of commands to run before the backup.")
	postBackupHooksFlag := flag.String("post-backup-hooks", "", "Comma-separated list of commands to run after the backup.")
	compressionEnabledFlag := flag.Bool("compression", false, "Enable compression for backups.")
	compressionFormatFlag := flag.String("compression-format", "", "Compression format for backups: 'zip', 'tar.gz or 'tar.zst'.")

	flag.Parse()

	// Create a map of the flags that were explicitly set by the user, along with their values.
	// This map is used to selectively override the base configuration.
	usedFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { usedFlags[f.Name] = true })

	flagMap := make(map[string]interface{})

	// Helper to add a value to the map only if the corresponding flag was set.
	addIfUsed := func(name string, value interface{}) {
		if usedFlags[name] {
			flagMap[name] = value
		}
	}

	// Helper for flags that need parsing. It only calls the parser if the flag was used.
	addParsedIfUsed := func(name string, rawValue string, parser func(string) []string) {
		if usedFlags[name] {
			flagMap[name] = parser(rawValue)
		}
	}

	// Populate the map using the helper.
	addIfUsed("source", *srcFlag)
	addIfUsed("target", *targetFlag)
	addIfUsed("log-level", *logLevelFlag)
	addIfUsed("fail-fast", *failFastFlag)
	addIfUsed("dry-run", *dryRunFlag)
	addIfUsed("metrics", *metricsFlag)
	addIfUsed("preserve-source-name", *preserveSourceNameFlag)
	addIfUsed("sync-workers", *syncWorkersFlag)
	addIfUsed("mirror-workers", *mirrorWorkersFlag)
	addIfUsed("delete-workers", *deleteWorkersFlag)
	addIfUsed("compress-workers", *compressWorkersFlag)
	addIfUsed("retry-count", *retryCountFlag)
	addIfUsed("retry-wait", *retryWaitFlag)
	addIfUsed("buffer-size-kb", *bufferSizeKBFlag)
	addIfUsed("mod-time-window", *modTimeWindowFlag)
	addIfUsed("compression", *compressionEnabledFlag)

	// Handle flags that require parsing/validation.
	addParsedIfUsed("user-exclude-files", *userExcludeFilesFlag, flagparse.ParseExcludeList)
	addParsedIfUsed("user-exclude-dirs", *userExcludeDirsFlag, flagparse.ParseExcludeList)
	addParsedIfUsed("pre-backup-hooks", *preBackupHooksFlag, flagparse.ParseCmdList)
	addParsedIfUsed("post-backup-hooks", *postBackupHooksFlag, flagparse.ParseCmdList)

	if usedFlags["mode"] {
		mode, err := config.BackupModeFromString(*modeFlag)
		if err != nil {
			return actionRunBackup, nil, err
		}
		flagMap["mode"] = mode
	}
	if usedFlags["sync-engine"] {
		engineType, err := config.SyncEngineFromString(*syncEngineFlag)
		if err != nil {
			return actionRunBackup, nil, err
		}
		flagMap["sync-engine"] = engineType
	}
	if usedFlags["compression-format"] {
		format, err := config.CompressionFormatFromString(*compressionFormatFlag)
		if err != nil {
			return actionRunBackup, nil, err
		}
		flagMap["compression-format"] = format
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

// runInit handles the logic for the 'init' action.
func runInit(ctx context.Context, flagMap map[string]interface{}, version string) error {
	// For init, source and target flags are mandatory.
	if _, ok := flagMap["target"]; !ok {
		return fmt.Errorf("the -target flag is required for the init operation")
	}
	if _, ok := flagMap["source"]; !ok {
		return fmt.Errorf("the -source flag is required for the init operation")
	}

	// Create a config from defaults merged with user flags.
	runConfig := config.MergeConfigWithFlags(config.NewDefault(), flagMap)

	startTime := time.Now()
	initEngine := engine.New(runConfig, version)
	err := initEngine.InitializeBackupTarget(ctx)
	duration := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		return err // The error will be logged with full details by main()
	}
	plog.Info(appName+" target successfully initialized.", "duration", duration)
	return nil
}

// runBackup handles the logic for the main backup action.
func runBackup(ctx context.Context, flagMap map[string]interface{}, version string) error {
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
		return err // The error will be logged with full details by main()
	}
	plog.Info(appName+" finished successfully.", "duration", duration)
	return nil
}

// run encapsulates the main application logic and returns an error if something
// goes wrong, allowing the main function to handle exit codes.
func run(ctx context.Context) error {
	plog.Info("Starting "+appName, "version", version, "pid", os.Getpid())

	action, flagMap, err := parseFlagConfig()
	if err != nil {
		return err
	}

	switch action {
	case actionShowVersion:
		fmt.Printf("%s version %s\n", appName, version)
		return nil
	case actionInitConfig:
		return runInit(ctx, flagMap, version)
	case actionRunBackup:
		return runBackup(ctx, flagMap, version)
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
		plog.Error(appName+" exited with error", "error", err)
		os.Exit(1)
	}
}
