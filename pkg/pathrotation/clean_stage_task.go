package pathrotation

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// cleanStageTask holds the mutable state for a single execution of the CleanupStage func.
type cleanStageTask struct {
	ctx          context.Context
	absStagePath string
	metrics      Metrics
	dryRun       bool
	timestampUTC time.Time
}

func (t *cleanStageTask) execute() error {
	plog.Info("Cleaning stage", "remove", t.absStagePath)

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

	// We retry this operation to handle transient file locks (e.g., AntiVirus scanners, Windows Indexer)
	const maxRetries = 5
	var err error
	for range maxRetries {
		if err = os.RemoveAll(t.absStagePath); err == nil {
			plog.Notice("CLEANED STAGE", "removed", t.absStagePath)
			t.metrics.AddBackupsRemoved(1)
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("failed to cleanup staging directory after %d attempts: %w", maxRetries, err)
}
