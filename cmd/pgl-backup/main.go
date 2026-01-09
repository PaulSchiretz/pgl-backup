package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/engine"
	"github.com/paulschiretz/pgl-backup/pkg/flagparse"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// appName is the canonical name of the application used for logging.
const appName = "PGL-Backup"

// appVersion holds the application's version string.
// It's a `var` so it can be set at compile time using ldflags.
// Example: go build -ldflags="-X main.appVersion=1.0.0"
var appVersion = "dev"

// init is called before main. We use it to set up a custom, more descriptive
// help message for the command-line flags.
func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s (version %s):\n", appName, appVersion)
		fmt.Fprintf(flag.CommandLine.Output(), "A simple and powerful file backup utility with snapshot and incremental modes.\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Note: Structural options (paths, retention policies) are configured via pgl-backup.config.json.\n\n")
		flag.PrintDefaults()
	}
}

// promptForConfirmation prompts the user for a yes/no response.
func promptForConfirmation(prompt string, defaultYes bool) bool {
	suffix := "[y/N]"
	if defaultYes {
		suffix = "[Y/n]"
	}
	fmt.Printf("%s %s: ", prompt, suffix)

	var response string
	_, _ = fmt.Scanln(&response)
	response = strings.ToLower(strings.TrimSpace(response))

	if response == "" {
		return defaultYes
	}
	return response == "y" || response == "yes"
}

// runInit handles the logic for the 'init' and 'init-default' actions.
func runInit(ctx context.Context, action flagparse.ActionFlag, flagMap map[string]interface{}) error {
	// For init, the target flag is mandatory to know where to look/write.
	targetVal, ok := flagMap["target"]
	if !ok {
		return fmt.Errorf("the -target flag is required for the init operation")
	}
	targetPath := targetVal.(string)

	var baseConfig config.Config

	// Check if init-default is set
	if action == flagparse.InitDefaultAction {
		// Check for force flag to bypass confirmation
		force := false
		if f, ok := flagMap["force"]; ok {
			force = f.(bool)
		}

		if !force {
			configPath := filepath.Join(targetPath, config.ConfigFileName)
			if _, err := os.Stat(configPath); err == nil {
				fmt.Printf("WARNING: Configuration file already exists at %s.\n", configPath)
				fmt.Printf("Using -init-default will overwrite it with default values. All custom settings will be lost.\n")
				if !promptForConfirmation("Are you sure you want to continue?", false) {
					plog.Info(appName + " init-default operation canceled.")
					return nil
				}
			}
		}
		baseConfig = config.NewDefault()
	} else {
		// Try to load existing config to preserve settings.
		// If it fails (e.g. corrupt JSON or path mismatch), we fall back to defaults.
		// Note: config.Load returns NewDefault() if the file simply doesn't exist.
		var err error
		baseConfig, err = config.Load(targetPath)
		if err != nil {
			plog.Warn("Could not load existing configuration, starting with defaults.", "reason", err)
			baseConfig = config.NewDefault()
		}
	}

	// Create a config from base merged with user flags.
	runConfig := config.MergeConfigWithFlags(baseConfig, flagMap)

	// Ensure source is set (either from existing config or flags).
	if runConfig.Paths.Source == "" {
		return fmt.Errorf("the -source flag is required for the init operation (unless updating an existing config)")
	}

	startTime := time.Now()
	initEngine := engine.New(runConfig, appVersion)
	err := initEngine.InitializeBackupTarget(ctx)
	duration := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		return err // The error will be logged with full details by main()
	}
	plog.Info(appName+" target successfully initialized.", "duration", duration)
	return nil
}

// runBackup handles the logic for the main backup execFlag.
func runBackup(ctx context.Context, flagMap map[string]interface{}) error {
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
	backupEngine := engine.New(runConfig, appVersion)
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
	plog.Info("Starting "+appName, "version", appVersion, "pid", os.Getpid())

	appAction, flagMap, err := flagparse.ParseFlagConfig()
	if err != nil {
		return err
	}

	switch appAction {
	case flagparse.VersionAction:
		fmt.Printf("%s version %s\n", appName, appVersion)
		return nil
	case flagparse.InitAction, flagparse.InitDefaultAction:
		return runInit(ctx, appAction, flagMap)
	case flagparse.BackupAction:
		return runBackup(ctx, flagMap)
	default:
		return fmt.Errorf("internal error: unknown action %d", appAction)
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
