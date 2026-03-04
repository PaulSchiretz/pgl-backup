package pathrotation

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// cleanStageTask holds the mutable state for a single execution of the Unstage func.
type cleanStageTask struct {
	ctx          context.Context
	absStagePath string
	metrics      Metrics
	dryRun       bool
	timestampUTC time.Time
}

func (t *cleanStageTask) execute() error {
	plog.Info("Cleanup staging path", "remove", t.absStagePath)

	t.metrics.StartProgress("Clean stage progress", 10*time.Second)
	defer func() {
		t.metrics.StopProgress()
		t.metrics.LogSummary("Clean stage finished")
	}()

	// Check for cancellation before performing the rename.
	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	default:
	}

	if t.dryRun {
		plog.Notice("[DRY RUN] CLEANED STAGE", "removed", t.absStagePath)
		return nil
	}

	// Log the intent before starting the operation
	plog.Info("Starting clean stage operation", "remove", t.absStagePath)

	if err := os.RemoveAll(t.absStagePath); err != nil {
		return fmt.Errorf("failed to cleanup staging directory: %w", err)
	}
	plog.Notice("CLEANED STAGE", "removed", t.absStagePath)

	t.metrics.AddBackupsRemoved(1)
	return nil
}
