package patharchive

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// archiveTask holds the mutable state for a single execution of the Archive func.
type archiveTask struct {
	ctx           context.Context
	absSourcePath string
	absTargetPath string
	metrics       Metrics
	dryRun        bool
	timestampUTC  time.Time
}

func (t *archiveTask) execute() error {
	plog.Info("Archiving backup",
		"move", t.absSourcePath,
		"to", t.absTargetPath)

	t.metrics.StartProgress("Archive progress", 10*time.Second)
	defer func() {
		t.metrics.StopProgress()
		t.metrics.LogSummary("Archive finished")
	}()

	// Check for cancellation before performing the rename.
	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	default:
	}

	if t.dryRun {
		plog.Notice("[DRY RUN] ARCHIVED", "moved", t.absSourcePath, "to", t.absTargetPath)
		return nil
	}

	// Log the intent before starting the operation
	plog.Info("Starting archive operation", "move", t.absSourcePath, "to", t.absTargetPath)

	// Sanity check: ensure the destination for the archive does not already exist.
	if _, err := os.Stat(t.absTargetPath); err == nil {
		return fmt.Errorf("archive destination %s already exists", t.absTargetPath)
	} else if !os.IsNotExist(err) {
		// The error is not "file does not exist", so it might be a permissions issue
		// or the archives subdir doesn't exist. Let's try to create it.
		return fmt.Errorf("could not check archive destination %s: %w", t.absTargetPath, err)
	}

	// Strategy: Rename (Move)
	if err := os.Rename(t.absSourcePath, t.absTargetPath); err != nil {
		return fmt.Errorf("failed to archive backup (directory might be in use): %w", err)
	}
	plog.Notice("ARCHIVED", "moved", t.absSourcePath, "to", t.absTargetPath)

	t.metrics.AddBackupsMoved(1)
	return nil
}
