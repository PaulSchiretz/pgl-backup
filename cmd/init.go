package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/engine"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// RunInit handles the logic for the 'init' command.
func RunInit(ctx context.Context, flagMap map[string]interface{}, appName, appVersion string) error {
	// For init, the target flag is mandatory to know where to look/write.
	targetVal, ok := flagMap["target"]
	if !ok {
		return fmt.Errorf("the -target flag is required for the init operation")
	}
	targetPath := targetVal.(string)

	var baseConfig config.Config

	// Check if init-default is set
	initDefault := false
	if v, ok := flagMap["default"]; ok {
		initDefault = v.(bool)
	}

	if initDefault {
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
