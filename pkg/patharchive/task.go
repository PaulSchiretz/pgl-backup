package patharchive

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/patharchivemetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// tasks holds the mutable state for a single execution of the archive.
// This makes the ArchiveEngine itself stateless and safe for concurrent use if needed.
type task struct {
	ctx               context.Context
	absTargetBasePath string
	relTargetPathKey  string

	toArchive    metafile.MetafileInfo
	interval     time.Duration
	location     *time.Location
	metrics      patharchivemetrics.Metrics
	dryRun       bool
	timestampUTC time.Time
}

func (t *task) execute() (metafile.MetafileInfo, error) {

	if !t.shouldArchive() {
		return metafile.MetafileInfo{}, ErrNothingToArchive
	}

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
		action := "moved"
		plog.Notice("[DRY RUN] ARCHIVE", "action", action, "from", t.toArchive.RelPathKey, "to", t.relTargetPathKey)

		return metafile.MetafileInfo{RelPathKey: t.relTargetPathKey, Metadata: t.toArchive.Metadata}, nil
	}

	absSourcePath := util.DenormalizePath(filepath.Join(t.absTargetBasePath, t.toArchive.RelPathKey))
	absTargetPath := util.DenormalizePath(filepath.Join(t.absTargetBasePath, t.relTargetPathKey))

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
	return metafile.MetafileInfo{RelPathKey: t.relTargetPathKey, Metadata: t.toArchive.Metadata}, nil
}

// shouldArchive determines if a new backup archive should be created based on the state of the current run.
//
// DESIGN NOTE on time zones:
// For intervals of 24 hours or longer, this function intentionally calculates
// archive boundaries based on the **local system's midnight** (`time.Local`).
// This ensures that archives align with a user's calendar day ("start a new
// weekly backup on Sunday night"), even though all stored timestamps are UTC.
// The conversion handles Daylight Saving Time (DST) shifts correctly by checking
// for midnight-to-midnight boundary crossings (epoch day counting).
func (t *task) shouldArchive() bool {
	if t.interval == 0 {
		return true // Archive interval check is explicitly disabled, always create an archive.
	}

	// For intervals of 24 hours or longer, this function intentionally calculates
	// archive boundaries based on the local system's midnight to align with a
	// user's calendar day, even though all stored timestamps are UTC.
	if t.interval >= 24*time.Hour {
		lastDayNum := t.calculateEpochDays(t.toArchive.Metadata.TimestampUTC)
		currentDayNum := t.calculateEpochDays(t.timestampUTC)

		// Calculate the Bucket Size in Days
		daysInBucket := int64(t.interval / (24 * time.Hour))

		// Check if we have crossed a bucket boundary
		// Example: Interval = 7 days.
		// Day 10 / 7 = 1.  Day 12 / 7 = 1.  (No Archive)
		// Day 13 / 7 = 1.  Day 14 / 7 = 2.  (Archive!)
		return (currentDayNum / daysInBucket) != (lastDayNum / daysInBucket)
	}

	// Sub-Daily Intervals (Hourly, 6-Hourly)
	// Use standard truncation for clean UTC time buckets.
	lastBackupBoundary := t.toArchive.Metadata.TimestampUTC.Truncate(t.interval)
	currentBackupBoundary := t.timestampUTC.Truncate(t.interval)

	return !currentBackupBoundary.Equal(lastBackupBoundary)
}

// calculateEpochDays calculates the number of days since the Unix Epoch (1970-01-01)
// for a given time in a specific location. It normalizes the time to midnight
// and adds a 12-hour buffer to handle DST transitions (23h/25h days) robustly.
func (t *task) calculateEpochDays(ti time.Time) int64 {
	y, m, d := ti.In(t.location).Date()
	midnight := time.Date(y, m, d, 0, 0, 0, 0, t.location)
	anchor := time.Date(1970, 1, 1, 0, 0, 0, 0, t.location)
	// Add 12 hours to center the calculation in the day, avoiding DST jitter (23h vs 25h days).
	return int64(midnight.Sub(anchor).Hours()+12) / 24
}
