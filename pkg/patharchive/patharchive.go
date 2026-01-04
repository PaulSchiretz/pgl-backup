// --- ARCHITECTURAL OVERVIEW: Archive Time Handling ---
//
// This package employs a specific time-handling strategy for creating archives,
// designed to provide predictable creation schedules for the user.
//
// 1. Archive (Snapshot Creation) - Predictable Creation
//    - Goal: To honor the user's configured `ArchiveInterval` as literally as possible.
//    - Logic: The `shouldArchive` function calculates time-based "bucketing" based on the
//      **local system's midnight** for day-or-longer intervals. This gives the user direct,
//      predictable control over the *frequency* of new archives, anchored to their local day.
//      This is distinct from the retention logic in the `engine` package, which uses UTC
//      for consistent historical cleanup.

// package patharchive is responsible for archiving the "current" incremental backup
// into a permanent, timestamped directory when a configured time interval is crossed.
package patharchive

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/patharchivemetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// archiveRun holds the mutable state for a single execution of the archiver.
// This makes the ArchiveEngine itself stateless and safe for concurrent use if needed.
type archiveRun struct {
	ctx                       context.Context
	dirPath                   string // path of the archive
	archiveBackupPath         string
	interval                  time.Duration // interval holds the final interval for the current run.
	currentBackupPath         string
	currentBackupTimestampUTC time.Time
	currentTimestampUTC       time.Time
	dryRun                    bool
	metrics                   patharchivemetrics.Metrics
	location                  *time.Location
}

type PathArchiver struct {
	config config.Config
}

// Archiver defines the interface for a component that archives a backup, turning the
// 'current' state into a permanent historical record.
type Archiver interface {
	Archive(ctx context.Context, dirPath, currentBackupPath string, currentTimestampUTC time.Time) (string, error)
}

// Statically assert that *PathArchiver implements the Archiver interface.
var _ Archiver = (*PathArchiver)(nil)

// NewPathArchiver creates a new PathArchiver with the given configuration.
func NewPathArchiver(cfg config.Config) *PathArchiver {
	return &PathArchiver{
		config: cfg,
	}
}

// Archive checks if the time since the last backup has crossed the configured interval.
// If it has, it renames the current backup directory to a permanent, timestamped archive directory. It also
// prepares the archive interval before checking. It is now responsible for reading its own metadata.
func (a *PathArchiver) Archive(ctx context.Context, dirPath, currentBackupPath string, currentTimestampUTC time.Time) (string, error) {
	metadata, err := metafile.Read(currentBackupPath)
	if err != nil {
		if os.IsNotExist(err) {
			// This is a normal condition on the first run; no previous backup to archive.
			return "", nil
		}
		// Any other error (e.g., corrupt file, permissions) is a problem.
		return "", fmt.Errorf("could not read previous backup metadata for archive check: %w", err)
	}

	// A valid metafile was found, so we can proceed with archiving.
	currentBackupTimestampUTC := metadata.TimestampUTC

	// Determine the archiving interval
	interval := a.determineInterval()

	// The directory name must remain uniquely based on UTC time to avoid DST conflicts,
	// but we add the user's local offset to make the timezone clear to the user.
	archiveTimestamp := config.FormatTimestampWithOffset(currentBackupTimestampUTC)
	archiveDirName := a.config.Naming.Prefix + archiveTimestamp
	archiveBackupPath := filepath.Join(dirPath, archiveDirName)

	// Ensure the archives directory exists.
	if interval > 0 && !a.config.DryRun {
		if err := os.MkdirAll(dirPath, util.UserWritableDirPerms); err != nil {
			return "", fmt.Errorf("failed to create archives subdirectory %s: %w", dirPath, err)
		}
	}

	var m patharchivemetrics.Metrics
	if a.config.Metrics {
		m = &patharchivemetrics.ArchiveMetrics{}
	} else {
		m = &patharchivemetrics.NoopMetrics{}
	}

	run := &archiveRun{
		ctx:                       ctx,
		archiveBackupPath:         archiveBackupPath,
		interval:                  interval,
		currentBackupPath:         currentBackupPath,
		currentBackupTimestampUTC: currentBackupTimestampUTC,
		currentTimestampUTC:       currentTimestampUTC,
		dryRun:                    a.config.DryRun,
		metrics:                   m,
		location:                  time.Local,
	}

	return run.execute()
}

// execute runs the archive logic.
func (r *archiveRun) execute() (string, error) {

	if !r.shouldArchive() {
		return "", nil
	}

	plog.Info("Archiving backup",
		"backup_time", r.currentBackupTimestampUTC,
		"current_time", r.currentTimestampUTC,
		"archive_interval", r.interval)

	r.metrics.StartProgress("Archive progress", 10*time.Second)
	defer func() {
		r.metrics.StopProgress()
		r.metrics.LogSummary("Archive finished")
	}()

	// Check for cancellation before performing the rename.
	select {
	case <-r.ctx.Done():
		return "", r.ctx.Err()
	default:
	}

	if r.dryRun {
		plog.Notice("[DRY RUN] ARCHIVE", "from", r.currentBackupPath, "to", r.archiveBackupPath)
		return "", nil
	}

	plog.Notice("ARCHIVE", "from", r.currentBackupPath, "to", r.archiveBackupPath)

	// Sanity check: ensure the destination for the archive does not already exist.
	if _, err := os.Stat(r.archiveBackupPath); err == nil {
		return "", fmt.Errorf("archive destination %s already exists", r.archiveBackupPath)
	} else if !os.IsNotExist(err) {
		// The error is not "file does not exist", so it might be a permissions issue
		// or the archives subdir doesn't exist. Let's try to create it.
		return "", fmt.Errorf("could not check archive destination %s: %w", r.archiveBackupPath, err)
	}

	if err := os.Rename(r.currentBackupPath, r.archiveBackupPath); err != nil {
		return "", fmt.Errorf("failed to archive backup: %w", err)
	}
	r.metrics.AddArchivesCreated(1)
	plog.Notice("ARCHIVED", "from", r.currentBackupPath, "to", r.archiveBackupPath)
	return r.archiveBackupPath, nil
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
func (r *archiveRun) shouldArchive() bool {
	if r.interval == 0 {
		return false // Archive is explicitly disabled.
	}

	// For intervals of 24 hours or longer, this function intentionally calculates
	// archive boundaries based on the local system's midnight to align with a
	// user's calendar day, even though all stored timestamps are UTC.
	if r.interval >= 24*time.Hour {
		loc := r.location

		lastDayNum := calculateEpochDays(r.currentBackupTimestampUTC, loc)
		currentDayNum := calculateEpochDays(r.currentTimestampUTC, loc)

		// Calculate the Bucket Size in Days
		daysInBucket := int64(r.interval / (24 * time.Hour))

		// Check if we have crossed a bucket boundary
		// Example: Interval = 7 days.
		// Day 10 / 7 = 1.  Day 12 / 7 = 1.  (No Archive)
		// Day 13 / 7 = 1.  Day 14 / 7 = 2.  (Archive!)
		return (currentDayNum / daysInBucket) != (lastDayNum / daysInBucket)
	}

	// Sub-Daily Intervals (Hourly, 6-Hourly)
	// Use standard truncation for clean UTC time buckets.
	lastBackupBoundary := r.currentBackupTimestampUTC.Truncate(r.interval)
	currentBackupBoundary := r.currentTimestampUTC.Truncate(r.interval)

	return !currentBackupBoundary.Equal(lastBackupBoundary)
}

// calculateEpochDays calculates the number of days since the Unix Epoch (1970-01-01)
// for a given time in a specific location. It normalizes the time to midnight
// and adds a 12-hour buffer to handle DST transitions (23h/25h days) robustly.
func calculateEpochDays(t time.Time, loc *time.Location) int64 {
	y, m, d := t.In(loc).Date()
	midnight := time.Date(y, m, d, 0, 0, 0, 0, loc)
	anchor := time.Date(1970, 1, 1, 0, 0, 0, 0, loc)
	// Add 12 hours to center the calculation in the day, avoiding DST jitter (23h vs 25h days).
	return int64(midnight.Sub(anchor).Hours()+12) / 24
}

// determineInterval calculates the effective archive interval based on configuration.
// If the mode is 'auto', it calculates the optimal interval based on the retention policy.
// If the mode is 'manual', it validates the user-configured interval.
func (a *PathArchiver) determineInterval() time.Duration {
	if a.config.Archive.Incremental.IntervalMode == config.ManualInterval {
		interval := time.Duration(a.config.Archive.Incremental.IntervalSeconds) * time.Second
		a.checkInterval(interval)
		return interval
	}
	return a.adjustInterval()
}

// adjustInterval calculates the optimal archive interval based on the retention
// policy. This is only called when the archive policy mode is 'auto'.
func (a *PathArchiver) adjustInterval() time.Duration {
	policy := a.config.Retention.Incremental
	var suggestedInterval time.Duration

	// If the retention policy is explicitly disabled, auto-mode should also disable archiving.
	if !policy.Enabled {
		plog.Debug("Retention policy is disabled; auto-disabling archiving for this run.")
		return 0 // disables the interval
	}

	// Pick the shortest duration required to satisfy the configured retention slots.
	switch {
	case policy.Hours > 0:
		suggestedInterval = 1 * time.Hour
	case policy.Days > 0:
		suggestedInterval = 24 * time.Hour
	case policy.Weeks > 0:
		suggestedInterval = 7 * 24 * time.Hour
	case policy.Months > 0:
		suggestedInterval = 30 * 24 * time.Hour // Approximation
	case policy.Years > 0:
		suggestedInterval = 365 * 24 * time.Hour // Approximation
	default:
		// Fallback if retention is disabled but mode is auto.
		suggestedInterval = 24 * time.Hour
	}

	plog.Debug("Auto-determined archive interval", "interval", suggestedInterval)
	return suggestedInterval
}

// checkInterval validates the interval against the retention policy.
func (a *PathArchiver) checkInterval(interval time.Duration) {
	policy := a.config.Retention.Incremental

	if interval == 0 {
		plog.Debug("Archiving is disabled (interval = 0). Retention policy warnings for interval mismatch are suppressed.")
		return
	}

	var mismatchedPeriods []string
	if policy.Hours > 0 && interval > 1*time.Hour {
		mismatchedPeriods = append(mismatchedPeriods, "Hourly")
	}
	if policy.Days > 0 && interval > 24*time.Hour {
		mismatchedPeriods = append(mismatchedPeriods, "Daily")
	}
	if policy.Weeks > 0 && interval > 168*time.Hour {
		mismatchedPeriods = append(mismatchedPeriods, "Weekly")
	}
	avgMonth := 30 * 24 * time.Hour
	if policy.Months > 0 && interval > avgMonth {
		mismatchedPeriods = append(mismatchedPeriods, "Monthly")
	}
	avgYear := 365 * 24 * time.Hour
	if policy.Years > 0 && interval > avgYear {
		mismatchedPeriods = append(mismatchedPeriods, "Yearly")
	}

	if len(mismatchedPeriods) > 0 {
		plog.Warn("Configuration Mismatch: The 'manual' archive interval is slower than the enabled retention period(s).",
			"mismatched_periods", strings.Join(mismatchedPeriods, ", "),
			"archive_interval", interval,
			"impact", "Retention slots for these periods will fill at the rate of the archive interval, not the retention period.")
	}
}
