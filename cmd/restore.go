package cmd

import (
	"context"
	"fmt"
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
	// For restore, the base and target flags are mandatory.
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
	// Require mode explicitly to avoid ambiguity about which backup path to search.
	if _, ok := flagMap["mode"]; !ok {
		return fmt.Errorf("the -mode flag is required to run a restore ('incremental' or 'snapshot')")
	}

	// Build absolute base path
	absBasePath, err := filepath.Abs(base)
	if err != nil {
		return fmt.Errorf("could not determine absolute base path for %s: %w", base, err)
	}
	absBasePath = util.DenormalizePath(absBasePath)

	// Load config from the base directory.
	loadedConfig, err := config.Load(absBasePath)
	if err != nil {
		return fmt.Errorf("failed to load configuration from base: %w", err)
	}

	// Merge the flag values over the loaded config.
	runConfig := config.MergeConfigWithFlags(flagparse.Restore, loadedConfig, flagMap)

	// CRITICAL: Validate the config for the run
	if err := runConfig.Validate(config.ValidationOptions{
		CheckTarget:     true,
		CheckBaseExists: true,
	}); err != nil {
		return err
	}

	// Set the global log level.
	plog.SetLevel(plog.LevelFromString(runConfig.LogLevel))

	// Log the Summary
	runConfig.LogSummary(flagparse.Restore)

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
	err = runner.ExecuteRestore(ctx, runConfig.Base, runConfig.Runtime.BackupName, runConfig.Target, restorePlan)
	duration := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		return err
	}
	plog.Info(buildinfo.Name+" restore finished successfully.", "duration", duration)
	return nil
}
