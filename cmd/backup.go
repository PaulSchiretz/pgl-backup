package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/buildinfo"
	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/engine"
	"github.com/paulschiretz/pgl-backup/pkg/flagparse"
	"github.com/paulschiretz/pgl-backup/pkg/hook"
	"github.com/paulschiretz/pgl-backup/pkg/patharchive"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/pathretention"
	"github.com/paulschiretz/pgl-backup/pkg/pathsync"
	"github.com/paulschiretz/pgl-backup/pkg/planner"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/preflight"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// RunBackup handles the logic for the main backup execution.
func RunBackup(ctx context.Context, flagMap map[string]any) error {
	// Define mandatory flags
	base, ok := flagMap["base"].(string)
	if !ok || base == "" {
		return fmt.Errorf("the -base flag is required to run a backup")
	}
	source, ok := flagMap["source"].(string)
	if !ok || source == "" {
		return fmt.Errorf("the -source flag is required to run a backup")
	}

	var err error

	// Validate base path
	absBasePath, err := util.ExpandedDenormalizedAbsPath(base)
	if err != nil {
		return fmt.Errorf("base path invalid: %w", err)
	}

	// NOTE: Base path needs to exist, for a Backup run
	if _, err := os.Stat(absBasePath); os.IsNotExist(err) {
		return fmt.Errorf("base path '%s' does not exist", absBasePath)
	}

	// Validate source path
	absSourcePath, err := util.ExpandedDenormalizedAbsPath(source)
	if err != nil {
		return fmt.Errorf("source path invalid: %w", err)
	}

	// NOTE: Source path needs to exist, for a Backup run
	if _, err := os.Stat(absSourcePath); os.IsNotExist(err) {
		return fmt.Errorf("source path '%s' does not exist", absSourcePath)
	}

	// Load config from the base directory, or use defaults if not found.
	loadedConfig, err := config.Load(absBasePath)
	if err != nil {
		return fmt.Errorf("failed to load configuration from base: %w", err)
	}

	// Merge the flag values over the loaded config to get the final run config.
	runConfig := config.MergeConfigWithFlags(flagparse.Backup, loadedConfig, flagMap)

	// Default to incremental if not specified
	if runConfig.Runtime.Mode != "incremental" && runConfig.Runtime.Mode != "snapshot" {
		runConfig.Runtime.Mode = "incremental"
	}

	// CRITICAL: Validate the config for the run
	if err := runConfig.Validate(); err != nil {
		return err
	}

	// Set the global log level based on the final configuration.
	plog.SetLevel(plog.LevelFromString(runConfig.LogLevel))

	// Log the Summary
	runConfig.LogSummary(flagparse.Backup, absBasePath, absSourcePath, "", "")

	// Create the runner and feed it with our leaf workers
	runner := engine.NewRunner(
		preflight.NewValidator(),
		hook.NewHookExecutor(exec.CommandContext),
		pathsync.NewPathSyncer(
			runConfig.Engine.Performance.BufferSizeKB,
			runConfig.Engine.Performance.ReadAheadLimitKB,
			runConfig.Engine.Performance.SyncWorkers,
			runConfig.Engine.Performance.MirrorWorkers,
		),
		patharchive.NewPathArchiver(),
		pathretention.NewPathRetainer(
			runConfig.Engine.Performance.DeleteWorkers,
		),
		pathcompression.NewPathCompressor(
			runConfig.Engine.Performance.BufferSizeKB,
			runConfig.Engine.Performance.ReadAheadLimitKB,
			runConfig.Engine.Performance.CompressWorkers,
		),
	)

	// Get the Plan
	backupPlan, err := planner.GenerateBackupPlan(runConfig)
	if err != nil {
		return err
	}

	// Execute the plan
	startTime := time.Now()
	err = runner.ExecuteBackup(ctx, absBasePath, absSourcePath, backupPlan)
	duration := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		return err // The error will be logged with full details by main()
	}
	plog.Info(buildinfo.Name+" finished successfully.", "duration", duration)
	return nil
}
