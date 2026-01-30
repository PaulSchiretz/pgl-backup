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

// RunRestore handles the logic for the restore command.
func RunRestore(ctx context.Context, flagMap map[string]interface{}) error {

	// Define mandatory flags
	base, ok := flagMap["base"].(string)
	if !ok || base == "" {
		return fmt.Errorf("the -base flag is required to run a restore")
	}
	target, ok := flagMap["target"].(string)
	if !ok || target == "" {
		return fmt.Errorf("the -target flag is required to run a restore")
	}
	backupName, ok := flagMap["backup-name"].(string)
	if !ok || backupName == "" {
		return fmt.Errorf("the -backup-name flag is required to run a restore")
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

	// NOTE: Base needs to exist, for a Restore run
	if _, err := os.Stat(absBasePath); os.IsNotExist(err) {
		return fmt.Errorf("base path '%s' does not exist", absBasePath)
	}

	// Validate Target
	target, err = util.ExpandPath(target)
	if err != nil {
		return fmt.Errorf("could not expand target path: %w", err)
	}
	absTargetPath, err := filepath.Abs(target)
	if err != nil {
		return fmt.Errorf("could not determine absolute target path: %w", err)
	}
	absTargetPath = util.DenormalizePath(absTargetPath)
	// NOTE: Target will be created if it doesn't exist, for a Restore run

	// Load config from the base directory.
	loadedConfig, err := config.Load(absBasePath)
	if err != nil {
		return fmt.Errorf("failed to load configuration from base: %w", err)
	}

	// Merge the flag values over the loaded config.
	runConfig := config.MergeConfigWithFlags(flagparse.Restore, loadedConfig, flagMap)

	// CRITICAL: Validate the config for the run
	if err := runConfig.Validate(); err != nil {
		return err
	}

	// Set the global log level.
	plog.SetLevel(plog.LevelFromString(runConfig.LogLevel))

	// Log the Summary
	runConfig.LogSummary(flagparse.Restore, absBasePath, "", absTargetPath, backupName)

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
	restorePlan, err := planner.GenerateRestorePlan(runConfig)
	if err != nil {
		return err
	}

	// Execute the plan
	startTime := time.Now()
	err = runner.ExecuteRestore(ctx, absBasePath, backupName, absTargetPath, restorePlan)
	duration := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		return err
	}
	plog.Info(buildinfo.Name+" restore finished successfully.", "duration", duration)
	return nil
}
