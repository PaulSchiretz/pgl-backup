package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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
	backupName, _ := flagMap["backup-name"].(string)

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

	// If backup name is missing, trigger interactive selection
	if backupName == "" {
		listPlan, err := planner.GenerateListPlan(runConfig)
		if err != nil {
			return fmt.Errorf("failed to generate list plan for interactive backup selection: %w", err)
		}

		backups, err := runner.ListBackups(ctx, absBasePath, listPlan)
		if err != nil {
			return fmt.Errorf("failed to list backups: %w", err)
		}

		if len(backups) == 0 {
			plog.Info(buildinfo.Name + " no backups found that can be restored.")
			return nil
		}

		totalOptions := len(backups) + 1
		optionNumberWidth := len(strconv.Itoa(totalOptions))

		// Output the backup table

		// Use a fixed layout without timezone for the rows, moving timezone to header
		timeLayout := "Mon, 02 Jan 2006 15:04:05" // 25 characters
		timeZoneLayout := time.Now().Local().Format("MST")

		fmt.Print("Please select a backup to restore:\n\n")
		// The %-25s format for Timestamp ensures alignment for dates.
		fmt.Printf("  %*s %-25s %-5s %s\n", optionNumberWidth+1, "#)", fmt.Sprintf("Timestamp (%s)", timeZoneLayout), "Type", "Backup Name")
		for i, b := range backups {
			mode := b.Metadata.Mode
			switch mode {
			case "incremental":
				mode = "INC"
			case "snapshot":
				mode = "SNP"
			default:
				mode = "INV"
			}
			fmt.Printf("  %*d) %s [%s] %s\n", optionNumberWidth, i+1, b.Metadata.TimestampUTC.Local().Format(timeLayout), mode, filepath.Base(b.RelPathKey))
		}
		fmt.Printf("  %*d) Cancel and exit %s.\n", optionNumberWidth, totalOptions, buildinfo.Name)

		var selection int
		for {
			fmt.Printf("\nSelect a backup (1-%d) [%d]: ", totalOptions, totalOptions)
			var input string
			_, err := fmt.Scanln(&input)
			if err != nil {
				if err.Error() == "unexpected newline" {
					selection = totalOptions
					break
				}
				return fmt.Errorf("failed to read input: %w", err)
			}
			selection, err = strconv.Atoi(input)
			if err != nil || selection < 1 || selection > totalOptions {
				fmt.Printf("Invalid selection. Please enter a number between 1 and %d.\n", totalOptions)
				continue
			}
			break
		}

		if selection == totalOptions {
			plog.Info(buildinfo.Name + " restore canceled by user.")
			return nil
		}
		backupName = filepath.Base(backups[selection-1].RelPathKey)
	}

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
