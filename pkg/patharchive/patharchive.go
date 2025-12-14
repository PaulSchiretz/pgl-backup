// package patharchive is responsible for archiving the "current" incremental backup
// into a permanent, timestamped directory when a configured time interval is crossed.
package patharchive

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
	"pixelgardenlabs.io/pgl-backup/pkg/util"
)

// archiveRunState holds the state specific to a single archive operation.
type archiveRunState struct {
	// interval holds the final interval for the current run.
	interval          time.Duration
	lastBackupUTC     time.Time
	currentBackupUTC  time.Time
	currentBackupPath string
}

// PathArchiver handles the process of archiving the current backup by renaming it on the filesystem.
type PathArchiver struct {
	config   config.Config
	runState archiveRunState
}

// Archiver defines the interface for a component that archives a backup, turning the
// 'current' state into a permanent historical record.
type Archiver interface {
	Archive(ctx context.Context, currentBackupPath string, lastBackupUTC, currentBackupUTC time.Time) error
	IsSlowFillingArchive(requiredInterval time.Duration) bool
}

// Statically assert that *PathArchiver implements the Archiver interface.
var _ Archiver = (*PathArchiver)(nil)

// NewPathArchiver creates a new PathArchiver with the given configuration.
func NewPathArchiver(cfg config.Config) *PathArchiver {
	archiver := &PathArchiver{
		config:   cfg,
		runState: archiveRunState{}, // Initialize with zero values for a clean default state.
	}
	// Pre-calculate the effective interval so it's available immediately for methods like IsSlowFillingArchive.
	archiver.runState.interval = archiver.prepareInterval()
	return archiver
}

// Archive checks if the time since the last backup has crossed the configured interval.
// If it has, it renames the current backup directory to a permanent, timestamped archive directory. It also
// prepares the archive interval before checking.
func (r *PathArchiver) Archive(ctx context.Context, currentBackupPath string, lastBackupUTC, currentBackupUTC time.Time) error {
	// Populate the runState for this specific archive operation, preserving the pre-calculated interval.
	r.runState = archiveRunState{
		interval:          r.runState.interval, // this is already calculated in the constructor
		currentBackupPath: currentBackupPath,
		lastBackupUTC:     lastBackupUTC,
		currentBackupUTC:  currentBackupUTC,
	}

	if !r.shouldArchive() {
		return nil
	}

	plog.Info("Archive interval crossed, creating new archive.",
		"last_backup_time", r.runState.lastBackupUTC,
		"current_time", r.runState.currentBackupUTC,
		"archive_interval", r.runState.interval)

	// Check for cancellation before performing the rename.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// The directory name must remain uniquely based on UTC time to avoid DST conflicts,
	// but we add the user's local offset to make the timezone clear to the user.
	archiveTimestamp := config.FormatTimestampWithOffset(r.runState.lastBackupUTC)
	archiveDirName := r.config.Naming.Prefix + archiveTimestamp

	// Archives are stored in a dedicated subdirectory for clarity.
	archivesSubDir := filepath.Join(r.config.Paths.TargetBase, r.config.Paths.ArchivesSubDir)
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
	if r.config.DryRun {
		plog.Info("[DRY RUN] Would archive (rename)", "from", r.runState.currentBackupPath, "to", archivePath)
		return nil
	}

	if err := os.MkdirAll(archivesSubDir, util.UserWritableDirPerms); err != nil {
		return fmt.Errorf("failed to create archives subdirectory %s: %w", archivesSubDir, err)
	}
	if err := os.Rename(r.runState.currentBackupPath, archivePath); err != nil {
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
func (r *PathArchiver) shouldArchive() bool {
	interval := r.runState.interval
	lastBackupUTC := r.runState.lastBackupUTC
	currentBackupUTC := r.runState.currentBackupUTC

	if interval == 0 {
		return false // Archive is explicitly disabled.
	}

	// For intervals of 24 hours or longer, this function intentionally calculates
	// archive boundaries based on the local system's midnight to align with a
	// user's calendar day, even though all stored timestamps are UTC.
	if interval >= 24*time.Hour {
		loc := time.Local

		// Normalize both times to the system's local midnight.
		y1, m1, d1 := lastBackupUTC.In(loc).Date()
		lastDayMidnight := time.Date(y1, m1, d1, 0, 0, 0, 0, loc)

		y2, m2, d2 := currentBackupUTC.In(loc).Date()
		currentDayMidnight := time.Date(y2, m2, d2, 0, 0, 0, 0, loc)

		// Calculate days since a fixed anchor (Unix Epoch Local).
		anchor := time.Date(1970, 1, 1, 0, 0, 0, 0, loc)
		lastDayNum := int64(lastDayMidnight.Sub(anchor).Hours() / 24)
		currentDayNum := int64(currentDayMidnight.Sub(anchor).Hours() / 24)

		// Calculate the Bucket Size in Days
		daysInBucket := int64(interval / (24 * time.Hour))

		// Check if we have crossed a bucket boundary
		// Example: Interval = 7 days.
		// Day 10 / 7 = 1.  Day 12 / 7 = 1.  (No Archive)
		// Day 13 / 7 = 1.  Day 14 / 7 = 2.  (Archive!)
		return (currentDayNum / daysInBucket) != (lastDayNum / daysInBucket)
	}

	// Sub-Daily Intervals (Hourly, 6-Hourly)
	// Use standard truncation for clean UTC time buckets.
	lastBackupBoundary := lastBackupUTC.Truncate(interval)
	currentBackupBoundary := currentBackupUTC.Truncate(interval)

	return !currentBackupBoundary.Equal(lastBackupBoundary)
}

// prepareInterval sets the archive interval for the current run.
// If the mode is 'auto', it calculates the optimal interval based on the retention policy.
// If the mode is 'manual', it validates the user-configured interval and returns it.
func (r *PathArchiver) prepareInterval() time.Duration {
	if r.config.IncrementalArchivePolicy.Mode == config.ManualInterval {
		r.checkManualInterval()
		return r.config.IncrementalArchivePolicy.Interval
	}
	return r.autoAdjustInterval()
}

// autoAdjustInterval calculates the optimal archive interval based on the retention
// policy. This is only called when the archive policy mode is 'auto'.
func (r *PathArchiver) autoAdjustInterval() time.Duration {
	policy := r.config.IncrementalRetentionPolicy
	var suggestedInterval time.Duration

	// If the retention policy is explicitly disabled, auto-mode should also disable archiving.
	if !policy.Enabled {
		plog.Debug("Retention policy is disabled; auto-disabling archiving for this run.")
		return 0
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

// checkManualInterval validates the user-configured interval against the retention policy.
func (r *PathArchiver) checkManualInterval() {
	policy := r.config.IncrementalRetentionPolicy
	interval := r.config.IncrementalArchivePolicy.Interval

	if interval == 0 {
		plog.Debug("Archiving is disabled (interval = 0). Retention policy warnings for interval mismatch are suppressed.")
		return
	}

	if policy.Hours > 0 && interval > 1*time.Hour {
		plog.Warn("Configuration Mismatch: Hourly retention is enabled, but archive interval is too slow.",
			"keep_hourly", policy.Hours, "archive_interval", interval,
			"impact", "Hourly slots will fill at the speed of the archive interval.")
	}

	if policy.Days > 0 && interval > 24*time.Hour {
		plog.Warn("Configuration Mismatch: Daily retention is enabled, but archive interval is too slow.",
			"keep_daily", policy.Days, "archive_interval", interval,
			"impact", "Daily slots will be filled by Weekly/Monthly archives, delaying the 'Weekly' retention rule.")
	}

	if policy.Weeks > 0 && interval > 168*time.Hour {
		plog.Warn("Configuration Mismatch: Weekly retention is enabled, but archive interval is too slow.",
			"keep_weekly", policy.Weeks, "archive_interval", interval)
	}

	avgMonth := 30 * 24 * time.Hour
	if policy.Months > 0 && interval > avgMonth {
		plog.Warn("Configuration Mismatch: Monthly retention is enabled, but archive interval is too slow.",
			"keep_monthly", policy.Months, "archive_interval", interval,
			"impact", "Archives occur less frequently than once a month; some calendar months will have no backup.")
	}

	avgYear := 365 * 24 * time.Hour
	if policy.Years > 0 && interval > avgYear {
		plog.Warn("Configuration Mismatch: Yearly retention is enabled, but archive interval is too slow.",
			"keep_yearly", policy.Years, "archive_interval", interval,
			"impact", "Archives occur less frequently than once a year; some calendar years will have no backup.")
	}
}

// IsSlowFillingArchive determines if a "slow-fill" warning should be shown.
// This is true when the configured or calculated archive interval is slower than the required retention period.
func (r *PathArchiver) IsSlowFillingArchive(requiredInterval time.Duration) bool {
	return r.runState.interval > requiredInterval
}
