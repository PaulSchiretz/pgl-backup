package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/buildinfo"
	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/engine"
	"github.com/paulschiretz/pgl-backup/pkg/flagparse"
	"github.com/paulschiretz/pgl-backup/pkg/patharchive"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/pathretention"
	"github.com/paulschiretz/pgl-backup/pkg/pathsync"
	"github.com/paulschiretz/pgl-backup/pkg/planner"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/preflight"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// RunList handles the logic for the prune command.
func RunList(ctx context.Context, flagMap map[string]interface{}) error {
	// Define mandatory flags
	base, ok := flagMap["base"].(string)
	if !ok || base == "" {
		return fmt.Errorf("the -base flag is required to run prune")
	}

	var err error
	// Validate Base
	base, err = util.ExpandPath(base)
	if err != nil {
		return fmt.Errorf("could not expand base path: %w", err)
	}
	absBasePath, err := filepath.Abs(base)
	if err != nil {
		return fmt.Errorf("could not determine absolute base path for %s: %w", base, err)
	}
	absBasePath = util.DenormalizePath(absBasePath)

	// NOTE: Base needs to exist, for a Prune run
	if _, err := os.Stat(absBasePath); os.IsNotExist(err) {
		return fmt.Errorf("base path '%s' does not exist", absBasePath)
	}

	// Load config from the base directory.
	loadedConfig, err := config.Load(absBasePath)
	if err != nil {
		return fmt.Errorf("failed to load configuration from base: %w", err)
	}

	// Merge the flag values over the loaded config.
	runConfig := config.MergeConfigWithFlags(flagparse.Prune, loadedConfig, flagMap)

	// CRITICAL: Validate the config for the run
	if err := runConfig.Validate(); err != nil {
		return err
	}

	// Set the global log level.
	plog.SetLevel(plog.LevelFromString(runConfig.LogLevel))

	// Log the Summary
	runConfig.LogSummary(flagparse.Prune, absBasePath, "", "", "")

	// Create the runner and feed it with our leaf workers
	runner := engine.NewRunner(
		preflight.NewValidator(),
		pathsync.NewPathSyncer(
			runConfig.Engine.Performance.BufferSizeKB,
			runConfig.Engine.Performance.SyncWorkers,
			runConfig.Engine.Performance.MirrorWorkers,
		),
		patharchive.NewPathArchiver(),
		pathretention.NewPathRetainer(
			runConfig.Engine.Performance.DeleteWorkers,
		),
		pathcompression.NewPathCompressor(
			runConfig.Engine.Performance.BufferSizeKB,
		),
	)

	// Get the Plan
	listPlan, err := planner.GenerateListPlan(runConfig)
	if err != nil {
		return err
	}

	// Execute the plan
	startTime := time.Now()
	err = runner.ExecuteList(ctx, absBasePath, listPlan)
	duration := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		return err
	}
	plog.Info(buildinfo.Name+" list finished successfully.", "duration", duration)
	return nil
}
