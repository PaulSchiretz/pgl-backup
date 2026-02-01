package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/buildinfo"
	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/engine"
	"github.com/paulschiretz/pgl-backup/pkg/flagparse"
	"github.com/paulschiretz/pgl-backup/pkg/hints"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
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
	uuid, _ := flagMap["uuid"].(string)

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
	runConfig.LogSummary(flagparse.Restore, absBasePath, "", absTargetPath, uuid)

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

	// If backup uuid is missing or "latest", trigger selection logic
	if uuid == "" || strings.ToLower(uuid) == "latest" {
		listPlan, err := planner.GenerateListPlan(runConfig)
		if err != nil {
			return fmt.Errorf("failed to generate list plan for backup selection: %w", err)
		}

		// Ensure consistent sorting (Newest First) so that index 0 is always the latest backup.
		// This is critical for the "latest" alias and consistent interactive listing.
		listPlan.SortOrder = planner.Desc

		backups, err := runner.ListBackups(ctx, absBasePath, listPlan)
		if err != nil {
			return fmt.Errorf("failed to list backups: %w", err)
		}

		if len(backups) == 0 {
			if runConfig.Runtime.Mode != "any" {
				plog.Info(fmt.Sprintf("%s no %s backups found that can be restored.", buildinfo.Name, runConfig.Runtime.Mode))
			} else {
				plog.Info(buildinfo.Name + " no backups found that can be restored.")
			}
			return nil
		}

		if strings.ToLower(uuid) == "latest" {
			// ListBackups returns sorted by timestamp descending (newest first)
			latest := backups[0]
			uuid = latest.Metadata.UUID
			plog.Info("Resolving 'latest' alias to backup", "uuid", uuid, "timestamp", latest.Metadata.TimestampUTC)
		} else {
			uuid, err = PromptBackupSelection(backups)
			if err != nil {
				if hints.IsHint(err) {
					plog.Info(buildinfo.Name + " restore canceled by user.")
					return nil
				}
				return err
			}
			if uuid == "" {
				return nil
			}
		}
	}

	// Get the Plan
	restorePlan, err := planner.GenerateRestorePlan(runConfig)
	if err != nil {
		return err
	}

	// Execute the plan
	startTime := time.Now()
	err = runner.ExecuteRestore(ctx, absBasePath, uuid, absTargetPath, restorePlan)
	duration := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		return err
	}
	plog.Info(buildinfo.Name+" restore finished successfully.", "duration", duration)
	return nil
}

// PromptBackupSelection handles the interactive selection of a backup from the list.
func PromptBackupSelection(backups []metafile.MetafileInfo) (string, error) {

	// Output the backup selection table
	totalNumOptions := len(backups) + 1
	optionNumColWidth := len(strconv.Itoa(totalNumOptions))

	timestampLayout := "Mon, 02 Jan 2006 15:04:05"
	timestampColWidth := len(timestampLayout)
	timestampHeaderTitle := fmt.Sprintf("Timestamp (%s)", time.Now().Local().Format("MST"))
	// Ensure column is wide enough for both the data (timestampLayout) and the header (timestampHeaderLayout)
	if len(timestampHeaderTitle) > timestampColWidth {
		timestampColWidth = len(timestampHeaderTitle)
	}

	fmt.Print("Please select a backup to restore:\n\n")
	// The %-*s format for Timestamp ensures alignment for dates.
	fmt.Printf("  %*s %-*s %-5s %-36s %s\n", optionNumColWidth+1, "#)", timestampColWidth, timestampHeaderTitle, "Type", "UUID", "Rel Path")
	for i, b := range backups {
		mode := strings.ToLower(b.Metadata.Mode)
		switch mode {
		case "incremental":
			mode = "INC"
		case "snapshot":
			mode = "SNP"
		default:
			mode = "INV"
		}
		fmt.Printf("  %*d) %-*s [%s] %s %s\n", optionNumColWidth, i+1, timestampColWidth, b.Metadata.TimestampUTC.Local().Format(timestampLayout), mode, b.Metadata.UUID, b.RelPathKey)
	}
	fmt.Printf("  %*d) Cancel and exit %s (or type 'q').\n", optionNumColWidth, totalNumOptions, buildinfo.Name)

	var selection int
	for {
		fmt.Printf("\nSelect a backup (1-%d) [%d]: ", totalNumOptions, totalNumOptions)
		var input string
		_, err := fmt.Scanln(&input)
		if err != nil {
			if err.Error() == "unexpected newline" {
				selection = totalNumOptions
				break
			}
			return "", fmt.Errorf("failed to read input: %w", err)
		}

		inputLower := strings.ToLower(strings.TrimSpace(input))
		if inputLower == "q" || inputLower == "quit" {
			return "", hints.New("restore canceled by user")
		}

		selection, err = strconv.Atoi(input)
		if err != nil || selection < 1 || selection > totalNumOptions {
			fmt.Printf("Invalid selection. Please enter a number between 1 and %d, or 'q' to quit.\n", totalNumOptions)
			continue
		}
		break
	}

	if selection == totalNumOptions {
		return "", hints.New("restore canceled by user")
	}
	return backups[selection-1].Metadata.UUID, nil
}
