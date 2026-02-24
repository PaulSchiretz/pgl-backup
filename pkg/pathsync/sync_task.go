package pathsync

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// syncTask holds the mutable state for a single sync execution.
// This makes the PathSyncer itself stateless and safe for concurrent use if needed.
type syncTask struct {
	*PathSyncer

	engine Engine

	absSourcePath string
	absTargetPath string

	ctx context.Context

	mirror          bool
	dryRun          bool
	failFast        bool
	safeCopy        bool
	sequentialWrite bool
	fileExclusions  exclusionSet
	dirExclusions   exclusionSet

	retryCount        int
	retryWait         time.Duration
	modTimeWindow     time.Duration
	overwriteBehavior OverwriteBehavior

	timestampUTC time.Time
	metrics      Metrics
}

func (t *syncTask) execute() error {
	plog.Info("Syncing files", "source", t.absSourcePath, "target", t.absTargetPath)

	// Start progress reporting
	t.metrics.StartProgress("Sync progress", 10*time.Second)
	defer func() {
		t.metrics.StopProgress()
		t.metrics.LogSummary("Sync finished")
	}()

	// Before dispatching to a specific sync engine, we prepare the destination directory.
	// This centralizes the logic, ensuring that the target directory exists with appropriate
	// permissions, regardless of which engine (native, robocopy) is used.
	srcInfo, err := os.Stat(t.absSourcePath)
	if err != nil {
		return fmt.Errorf("could not stat source directory %s: %w", t.absSourcePath, err)
	}

	// We use the source directory's permissions as a template for the destination.
	// Crucially, `withBackupWritePermission` is applied to ensure the backup user
	// can always write to the destination on subsequent runs, preventing permission lockouts.
	if !t.dryRun && t.absTargetPath != "" && srcInfo != nil {
		if err := os.MkdirAll(t.absTargetPath, util.WithUserWritePermission(srcInfo.Mode().Perm())); err != nil {
			return fmt.Errorf("failed to create target directory %s: %w", t.absTargetPath, err)
		}
	}

	return t.syncDirectories()
}

func (t *syncTask) syncDirectories() error {
	var err error
	var syn syncer
	switch t.engine {
	case Native:
		syn, err = newNativeSyncer(
			t.mirror,
			t.dryRun,
			t.failFast,
			t.safeCopy,
			t.sequentialWrite,
			t.fileExclusions,
			t.dirExclusions,
			t.retryCount,
			t.retryWait,
			t.modTimeWindow,
			t.overwriteBehavior,
			t.ioBufferPool,
			t.ioBufferSize,
			t.readAheadLimiter,
			t.readAheadLimit,
			t.readAheadPool,
			t.numSyncWorkers,
			t.numMirrorWorkers,
			t.metrics,
		)
		if err != nil {
			return err
		}
		t.lastNativeSyncer = syn.(*nativeSyncer) // Store for testing
	default:
		return fmt.Errorf("unknown sync engine configured: %v", t.engine)
	}

	if err := syn.Sync(t.ctx, t.absSourcePath, t.absTargetPath); err != nil {
		return err
	}
	return nil
}
