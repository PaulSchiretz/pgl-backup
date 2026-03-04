package pathrotation

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// unstageTask holds the mutable state for a single execution of the Unstage func.
type unstageTask struct {
	ctx            context.Context
	absUnstagePath string
	metrics        Metrics
	dryRun         bool
	timestampUTC   time.Time
}

func (t *unstageTask) execute() error {
	plog.Info("Unstaging backup", "remove", t.absUnstagePath)

	t.metrics.StartProgress("Unstage progress", 10*time.Second)
	defer func() {
		t.metrics.StopProgress()
		t.metrics.LogSummary("Unstage finished")
	}()

	// Check for cancellation before performing the rename.
	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	default:
	}

	if t.dryRun {
		plog.Notice("[DRY RUN] UNSTAGED", "removed", t.absUnstagePath)
		return nil
	}

	// Log the intent before starting the operation
	plog.Info("Starting unstage operation", "remove", t.absUnstagePath)

	// Strategy: Remove (Unstage)
	if err := os.RemoveAll(t.absUnstagePath); err != nil {
		return fmt.Errorf("failed to unstage backup (directory might be in use): %w", err)
	}
	plog.Notice("UNSTAGED", "removed", t.absUnstagePath)

	t.metrics.AddBackupsRemoved(1)
	return nil
}
