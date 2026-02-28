package patharchive

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// tasks holds the mutable state for a single execution of the archive.
// This makes the ArchiveEngine itself stateless and safe for concurrent use if needed.
type task struct {
	ctx              context.Context
	absBasePath      string
	relTargetPathKey string

	toArchive    metafile.MetafileInfo
	interval     time.Duration
	metrics      Metrics
	dryRun       bool
	timestampUTC time.Time
}

func (t *task) execute() (metafile.MetafileInfo, error) {
	plog.Info("Archiving backup",
		"backup_time", t.toArchive.Metadata.TimestampUTC,
		"current_time", t.timestampUTC,
		"archive_interval", t.interval)

	t.metrics.StartProgress("Archive progress", 10*time.Second)
	defer func() {
		t.metrics.StopProgress()
		t.metrics.LogSummary("Archive finished")
	}()

	// Check for cancellation before performing the rename.
	select {
	case <-t.ctx.Done():
		return metafile.MetafileInfo{}, t.ctx.Err()
	default:
	}

	if t.dryRun {
		plog.Notice("[DRY RUN] ARCHIVE", "moved", t.toArchive.RelPathKey, "to", t.relTargetPathKey)
		return metafile.MetafileInfo{RelPathKey: t.relTargetPathKey, Metadata: t.toArchive.Metadata}, nil
	}

	absSourcePath := util.DenormalizedAbsPath(t.absBasePath, t.toArchive.RelPathKey)
	absTargetPath := util.DenormalizedAbsPath(t.absBasePath, t.relTargetPathKey)

	// Log the intent before starting the operation
	plog.Info("Starting archive operation", "from", t.toArchive.RelPathKey, "to", t.relTargetPathKey)

	// Sanity check: ensure the destination for the archive does not already exist.
	if _, err := os.Stat(absTargetPath); err == nil {
		return metafile.MetafileInfo{}, fmt.Errorf("archive destination %s already exists", t.relTargetPathKey)
	} else if !os.IsNotExist(err) {
		// The error is not "file does not exist", so it might be a permissions issue
		// or the archives subdir doesn't exist. Let's try to create it.
		return metafile.MetafileInfo{}, fmt.Errorf("could not check archive destination %s: %w", t.relTargetPathKey, err)
	}

	// Strategy: Rename (Move)
	if err := os.Rename(absSourcePath, absTargetPath); err != nil {
		return metafile.MetafileInfo{}, fmt.Errorf("failed to archive current backup (directory might be in use): %w", err)
	}
	plog.Notice("ARCHIVED", "moved", t.toArchive.RelPathKey, "to", t.relTargetPathKey)

	t.metrics.AddArchivesCreated(1)
	return metafile.MetafileInfo{
		RelPathKey: util.NormalizePath(t.relTargetPathKey),
		Metadata:   t.toArchive.Metadata,
	}, nil
}
