package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/buildinfo"
	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/flagparse"
	"github.com/paulschiretz/pgl-backup/pkg/lockfile"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/preflight"
)

// RunInit handles the logic for the 'init' command.
func RunInit(ctx context.Context, flagMap map[string]interface{}) error {
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
				if !PromptForConfirmation("Are you sure you want to continue?", false) {
					plog.Info(buildinfo.Name + " init-default operation canceled.")
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
	runConfig := config.MergeConfigWithFlags(flagparse.Init, baseConfig, flagMap)

	// Ensure source is set (either from existing config or flags).
	if runConfig.Source == "" {
		return fmt.Errorf("the -source flag is required for the init operation (unless updating an existing config)")
	}

	// CRITICAL: Validate the config for the run
	if err := runConfig.Validate(true); err != nil {
		return err
	}

	startTime := time.Now()

	// 1. Preflight Checks
	// Ensure the target directory exists (or can be created) and is writable.
	validator := preflight.NewValidator()
	pfPlan := &preflight.Plan{
		SourceAccessible:   true,
		TargetAccessible:   true,
		TargetWriteable:    true,
		EnsureTargetExists: true,
		PathNesting:        true,
		DryRun:             runConfig.Runtime.DryRun,
	}

	if err := validator.Run(ctx, runConfig.Source, runConfig.TargetBase, pfPlan, time.Now().UTC()); err != nil {
		return fmt.Errorf("initialization preflight failed: %w", err)
	}

	if runConfig.Runtime.DryRun {
		plog.Info("[DRY RUN] Initialization complete. No changes made.")
		return nil
	}

	// 2. Acquire Lock
	// Ensure exclusive access to the target directory.
	appID := fmt.Sprintf("pgl-backup-init:%s", runConfig.TargetBase)
	lock, err := lockfile.Acquire(ctx, runConfig.TargetBase, appID)
	if err != nil {
		return fmt.Errorf("failed to acquire lock on target directory: %w", err)
	}
	defer lock.Release()

	// 3. Generate Config
	if err := config.Generate(runConfig); err != nil {
		return fmt.Errorf("failed to generate config file: %w", err)
	}

	duration := time.Since(startTime).Round(time.Millisecond)
	plog.Info(buildinfo.Name+" target successfully initialized.", "duration", duration)
	return nil
}

// PromptForConfirmation prompts the user for a yes/no response.
func PromptForConfirmation(prompt string, defaultYes bool) bool {
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
