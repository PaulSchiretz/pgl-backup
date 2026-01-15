package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/buildinfo"
	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/engine"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// RunBackup handles the logic for the main backup execution.
func RunBackup(ctx context.Context, flagMap map[string]interface{}) error {
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
	backupEngine := engine.New(runConfig)
	err = backupEngine.ExecuteBackup(ctx)
	duration := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		return err // The error will be logged with full details by main()
	}
	plog.Info(buildinfo.Name+" finished successfully.", "duration", duration)
	return nil
}
