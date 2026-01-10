package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/engine"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// RunPrune handles the logic for the prune command.
func RunPrune(ctx context.Context, flagMap map[string]interface{}, appName, appVersion string) error {
	// For prune, the target flag is mandatory.
	targetPath, ok := flagMap["target"].(string)
	if !ok || targetPath == "" {
		return fmt.Errorf("the -target flag is required to run prune")
	}

	// Load config from the target directory.
	loadedConfig, err := config.Load(targetPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration from target: %w", err)
	}

	// Merge the flag values over the loaded config.
	runConfig := config.MergeConfigWithFlags(loadedConfig, flagMap)

	// Set the global log level.
	plog.SetLevel(plog.LevelFromString(runConfig.LogLevel))

	runConfig.LogSummary()

	startTime := time.Now()
	pruneEngine := engine.New(runConfig, appVersion)
	err = pruneEngine.ExecutePrune(ctx)
	duration := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		return err
	}
	plog.Info(appName+" prune finished successfully.", "duration", duration)
	return nil
}
