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

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
	"pixelgardenlabs.io/pgl-backup/pkg/util"
)

// archiveRunState holds the state specific to a single archive operation.
type archiveRunState struct {
	interval                  time.Duration // interval holds the final interval for the current run.
	currentBackupPath         string
	currentBackupTimestampUTC time.Time
	currentTimestampUTC       time.Time
}

type PathArchiver struct {
	config config.Config
}

// Archiver defines the interface for a component that archives a backup, turning the
// 'current' state into a permanent historical record.
type Archiver interface {
	Archive(ctx context.Context, currentBackupPath string, currentBackupTimestampUTC, currentTimestampUTC time.Time) error
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
// prepares the archive interval before checking.
func (a *PathArchiver) Archive(ctx context.Context, currentBackupPath string, currentBackupTimestampUTC, currentTimestampUTC time.Time) error {
	runState := &archiveRunState{
		interval:                  a.config.IncrementalArchivePolicy.Interval,
		currentBackupPath:         currentBackupPath,
		currentBackupTimestampUTC: currentBackupTimestampUTC,
		currentTimestampUTC:       currentTimestampUTC,
	}
	// Calculate and set the effective interval for this run, and log warnings.
	a.prepareInterval(runState)

	if !a.shouldArchive(runState) {
		return nil
	}

	plog.Info("Archive interval crossed, creating new archive.",
		"backup_time", runState.currentBackupTimestampUTC,
		"current_time", runState.currentTimestampUTC,
		"archive_interval", runState.interval)

	// Check for cancellation before performing the rename.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// The directory name must remain uniquely based on UTC time to avoid DST conflicts,
	// but we add the user's local offset to make the timezone clear to the user.
	archiveTimestamp := config.FormatTimestampWithOffset(runState.currentBackupTimestampUTC)
	archiveDirName := a.config.Naming.Prefix + archiveTimestamp

	// Archives are stored in a dedicated subdirectory for clarity.
	archivesSubDir := filepath.Join(a.config.Paths.TargetBase, a.config.Paths.ArchivesSubDir)
	archivePath := filepath.Join(archivesSubDir, archiveDirName)

	// Sanity check: ensure the destination for the archive does not already exist.
	if _, err := os.Stat(archivePath); err == nil {
		return fmt.Errorf("archive destination %s already exists", archivePath)
	} else if !os.IsNotExist(err) {
		// The error is not "file does not exist", so it might be a permissions issue
		// or the archives subdir doesn't exist. Let's try to create it.
		return fmt.Errorf("could not check archive destination %s: %w", archivePath, err)
	}

	plog.Info("Archiving previous day's backup", "destination", archivePath)
	if a.config.DryRun {
		plog.Info("[DRY RUN] Would archive (rename)", "from", runState.currentBackupPath, "to", archivePath)
		return nil
	}

	if err := os.MkdirAll(archivesSubDir, util.UserWritableDirPerms); err != nil {
		return fmt.Errorf("failed to create archives subdirectory %s: %w", archivesSubDir, err)
	}
	if err := os.Rename(runState.currentBackupPath, archivePath); err != nil {
		return fmt.Errorf("failed to archive backup: %w", err)
	}
	return nil
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
func (a *PathArchiver) shouldArchive(runState *archiveRunState) bool {
	if runState.interval == 0 {
		return false // Archive is explicitly disabled.
	}

	// For intervals of 24 hours or longer, this function intentionally calculates
	// archive boundaries based on the local system's midnight to align with a
	// user's calendar day, even though all stored timestamps are UTC.
	if runState.interval >= 24*time.Hour {
		loc := time.Local

		// Normalize both times to the system's local midnight.
		y1, m1, d1 := runState.currentBackupTimestampUTC.In(loc).Date()
		lastDayMidnight := time.Date(y1, m1, d1, 0, 0, 0, 0, loc)

		y2, m2, d2 := runState.currentTimestampUTC.In(loc).Date()
		currentDayMidnight := time.Date(y2, m2, d2, 0, 0, 0, 0, loc)

		// Calculate days since a fixed anchor (Unix Epoch Local).
		anchor := time.Date(1970, 1, 1, 0, 0, 0, 0, loc)
		lastDayNum := int64(lastDayMidnight.Sub(anchor).Hours() / 24)
		currentDayNum := int64(currentDayMidnight.Sub(anchor).Hours() / 24)

		// Calculate the Bucket Size in Days
		daysInBucket := int64(runState.interval / (24 * time.Hour))

		// Check if we have crossed a bucket boundary
		// Example: Interval = 7 days.
		// Day 10 / 7 = 1.  Day 12 / 7 = 1.  (No Archive)
		// Day 13 / 7 = 1.  Day 14 / 7 = 2.  (Archive!)
		return (currentDayNum / daysInBucket) != (lastDayNum / daysInBucket)
	}

	// Sub-Daily Intervals (Hourly, 6-Hourly)
	// Use standard truncation for clean UTC time buckets.
	lastBackupBoundary := runState.currentBackupTimestampUTC.Truncate(runState.interval)
	currentBackupBoundary := runState.currentTimestampUTC.Truncate(runState.interval)

	return !currentBackupBoundary.Equal(lastBackupBoundary)
}

// prepareInterval sets the archive interval for the current run.
// If the mode is 'auto', it calculates the optimal interval based on the retention policy.
// If the mode is 'manual', it validates the user-configured interval and returns it.
func (a *PathArchiver) prepareInterval(runState *archiveRunState) {
	if a.config.IncrementalArchivePolicy.Mode == config.ManualInterval {
		a.checkInterval(runState)
	} else {
		a.adjustInterval(runState)
	}
}

// adjustInterval calculates the optimal archive interval based on the retention
// policy. This is only called when the archive policy mode is 'auto'.
func (a *PathArchiver) adjustInterval(runState *archiveRunState) {
	policy := a.config.IncrementalRetentionPolicy
	var suggestedInterval time.Duration

	// If the retention policy is explicitly disabled, auto-mode should also disable archiving.
	if !policy.Enabled {
		plog.Debug("Retention policy is disabled; auto-disabling archiving for this run.")
		runState.interval = 0 //disables the interval
		return
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
	runState.interval = suggestedInterval
}

// checkInterval validates the interval against the retention policy.
func (a *PathArchiver) checkInterval(runState *archiveRunState) {
	policy := a.config.IncrementalRetentionPolicy

	if runState.interval == 0 {
		plog.Debug("Archiving is disabled (interval = 0). Retention policy warnings for interval mismatch are suppressed.")
		return
	}

	var mismatchedPeriods []string
	if policy.Hours > 0 && runState.interval > 1*time.Hour {
		mismatchedPeriods = append(mismatchedPeriods, "Hourly")
	}
	if policy.Days > 0 && runState.interval > 24*time.Hour {
		mismatchedPeriods = append(mismatchedPeriods, "Daily")
	}
	if policy.Weeks > 0 && runState.interval > 168*time.Hour {
		mismatchedPeriods = append(mismatchedPeriods, "Weekly")
	}
	avgMonth := 30 * 24 * time.Hour
	if policy.Months > 0 && runState.interval > avgMonth {
		mismatchedPeriods = append(mismatchedPeriods, "Monthly")
	}
	avgYear := 365 * 24 * time.Hour
	if policy.Years > 0 && runState.interval > avgYear {
		mismatchedPeriods = append(mismatchedPeriods, "Yearly")
	}

	if len(mismatchedPeriods) > 0 {
		plog.Warn("Configuration Mismatch: The manual archive interval is slower than the enabled retention period(s).",
			"mismatched_periods", strings.Join(mismatchedPeriods, ", "),
			"archive_interval", runState.interval,
			"impact", "Retention slots for these periods will fill at the rate of the archive interval, not the retention period.")
	}
}
