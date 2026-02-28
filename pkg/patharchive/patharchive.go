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
//    - Boundary Crossing: Crucially, for intervals >= 24h, the logic checks if a *calendar boundary*
//      (e.g., midnight) has been crossed between the last backup time and now. It does NOT simply
//      check `TimeSince(LastBackup) > Interval`. This ensures that even if backups run frequently
//      (updating the "last backup" timestamp each time), an archive is still triggered exactly once
//     when the day/week changes, preventing the "sliding window" problem.
//
// 2. Archive Creation Strategy - Rename vs. Copy
//   - This package exclusively uses an `os.Rename` operation to archive the `current` backup.
//     A `Copy` strategy was considered and rejected.
//   - Rationale: A backup target (USB HDD, NAS) is almost always slower than the source (SSD).
//     A `Copy` operation on the target (reading 1TB and writing 1TB) is therefore the
//     slowest possible action. On network drives, this causes the "hairpin problem" where
//     data must be read to the client and written back, doubling network traffic.
//     In contrast, a `Rename` is an instant metadata operation.
//   - The subsequent full re-sync from a fast source to a slow target is significantly
//     faster than a slow target copying to itself. The `Rename` strategy is therefore the
//     unambiguously correct choice for performance in all typical backup scenarios.

// package patharchive is responsible for archiving the "current" incremental backup
// into a permanent, timestamped directory when a configured time interval is crossed.
package patharchive

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/hints"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

var ErrNothingToArchive = hints.New("nothing to archive")
var ErrDisabled = hints.New("archiving is disabled")

type PathArchiver struct{}

// NewPathArchiver creates a new PathArchiver with the given configuration.
func NewPathArchiver() *PathArchiver {
	return &PathArchiver{}
}

// Archive moves the current backup directory to a permanent, timestamped archive directory.
//
// The decision to archive should be made by the caller using ShouldArchive.
// This function now unconditionally performs the archive operation, but retains
// basic safety checks.
func (a *PathArchiver) Archive(ctx context.Context, absBasePath, relArchivePathKey, archiveEntryPrefix string, toArchive metafile.MetafileInfo, p *Plan, timestampUTC time.Time) (metafile.MetafileInfo, error) {

	if !p.Enabled {
		return metafile.MetafileInfo{}, ErrDisabled
	}

	if toArchive.RelPathKey == "" {
		return metafile.MetafileInfo{}, ErrNothingToArchive
	}

	select {
	case <-ctx.Done():
		return metafile.MetafileInfo{}, ctx.Err()
	default:
	}

	// Resolve the actual archiving interval
	interval := a.resolveInterval(p)

	var m Metrics
	if p.Metrics {
		m = &ArchiveMetrics{}
	} else {
		m = &NoopMetrics{}
	}

	// Ensure the archives directory exists.
	if !p.DryRun {
		absArchivePath := util.DenormalizedAbsPath(absBasePath, relArchivePathKey)
		if err := os.MkdirAll(absArchivePath, util.UserWritableDirPerms); err != nil {
			return metafile.MetafileInfo{}, fmt.Errorf("failed to create archive directory %s: %w", relArchivePathKey, err)
		}
	}

	// The directory name must remain uniquely based on UTC time to avoid DST conflicts,
	// but we add the user's local offset to make the timezone clear to the user.
	timestamp := util.FormatTimestampWithOffset(toArchive.Metadata.TimestampUTC)
	archiveEntry := archiveEntryPrefix + timestamp
	relTargetPathKey := path.Join(relArchivePathKey, archiveEntry)

	t := &task{
		ctx:              ctx,
		absBasePath:      absBasePath,
		relTargetPathKey: relTargetPathKey,
		toArchive:        toArchive,
		interval:         interval,
		metrics:          m,
		timestampUTC:     timestampUTC,
		dryRun:           p.DryRun,
	}

	result, err := t.execute()
	if err != nil {
		return metafile.MetafileInfo{}, err
	}

	return result, nil
}

// ShouldArchive determines if a new backup archive should be created based on the plan and timestamps.
//
// DESIGN NOTE on time zones:
// For intervals of 24 hours or longer, this function intentionally calculates
// archive boundaries based on the **local system's midnight** (`time.Local`).
// This ensures that archives align with a user's calendar day ("start a new
// weekly backup on Sunday night"), even though all stored timestamps are UTC.
// The conversion handles Daylight Saving Time (DST) shifts correctly by checking
// for midnight-to-midnight boundary crossings (epoch day counting).
func (a *PathArchiver) ShouldArchive(toArchive metafile.MetafileInfo, p *Plan, timestampUTC time.Time) (bool, error) {
	if !p.Enabled {
		return false, ErrDisabled
	}

	if toArchive.RelPathKey == "" {
		return false, ErrNothingToArchive
	}

	interval := a.resolveInterval(p)

	if interval == 0 {
		return true, nil // Archive interval check is explicitly disabled, always create an archive.
	}

	location := time.Local

	// For intervals of 24 hours or longer, this function intentionally calculates
	// archive boundaries based on the local system's midnight to align with a
	// user's calendar day, even though all stored timestamps are UTC.
	if interval >= 24*time.Hour {
		lastDayNum := calculateEpochDays(toArchive.Metadata.TimestampUTC, location)
		currentDayNum := calculateEpochDays(timestampUTC, location)

		daysInBucket := int64(interval / (24 * time.Hour))

		return (currentDayNum / daysInBucket) != (lastDayNum / daysInBucket), nil
	}

	// Sub-Daily Intervals (Hourly, 6-Hourly)
	lastBackupBoundary := toArchive.Metadata.TimestampUTC.Truncate(interval)
	currentBackupBoundary := timestampUTC.Truncate(interval)

	return !currentBackupBoundary.Equal(lastBackupBoundary), nil
}

// resolveInterval calculates the effective archive interval based on configuration.
// If the mode is 'auto', it calculates the optimal interval based on the retention policy.
// If the mode is 'manual', it validates the user-configured interval.
func (a *PathArchiver) resolveInterval(p *Plan) time.Duration {
	switch p.IntervalMode {
	case Manual:
		interval := time.Duration(p.IntervalSeconds) * time.Second
		a.checkInterval(interval, p)
		return interval
	default:
		return a.adjustInterval(p)
	}
}

// adjustInterval calculates the optimal archive interval based on the retention
// policy. This is only called when the archive policy mode is 'auto'.
func (a *PathArchiver) adjustInterval(p *Plan) time.Duration {
	var suggestedInterval time.Duration

	// Pick the shortest duration required to satisfy the configured retention slots.
	switch {
	case p.Constraints.Hours > 0:
		suggestedInterval = 1 * time.Hour
	case p.Constraints.Days > 0:
		suggestedInterval = 24 * time.Hour
	case p.Constraints.Weeks > 0:
		suggestedInterval = 7 * 24 * time.Hour
	case p.Constraints.Months > 0:
		suggestedInterval = 30 * 24 * time.Hour // Approximation
	case p.Constraints.Years > 0:
		suggestedInterval = 365 * 24 * time.Hour // Approximation
	default:
		// Fallback if retention is disabled but mode is auto.
		suggestedInterval = 24 * time.Hour
	}

	plog.Debug("Auto-determined archive interval", "interval", suggestedInterval)
	return suggestedInterval
}

// checkInterval validates the interval against the retention policy.
func (a *PathArchiver) checkInterval(interval time.Duration, p *Plan) {

	// Shortcut for always archive interval
	if interval == 0 {
		plog.Debug("Archiving is always enabled (interval = 0) Retention policy warnings for interval mismatch are suppressed.")
		return
	}

	var mismatchedPeriods []string
	if p.Constraints.Hours > 0 && interval > 1*time.Hour {
		mismatchedPeriods = append(mismatchedPeriods, "Hourly")
	}
	if p.Constraints.Days > 0 && interval > 24*time.Hour {
		mismatchedPeriods = append(mismatchedPeriods, "Daily")
	}
	if p.Constraints.Weeks > 0 && interval > 168*time.Hour {
		mismatchedPeriods = append(mismatchedPeriods, "Weekly")
	}
	avgMonth := 30 * 24 * time.Hour
	if p.Constraints.Months > 0 && interval > avgMonth {
		mismatchedPeriods = append(mismatchedPeriods, "Monthly")
	}
	avgYear := 365 * 24 * time.Hour
	if p.Constraints.Years > 0 && interval > avgYear {
		mismatchedPeriods = append(mismatchedPeriods, "Yearly")
	}

	if len(mismatchedPeriods) > 0 {
		plog.Warn("Configuration Mismatch: The 'manual' archive interval is slower than your retention period(s).",
			"mismatched_periods", strings.Join(mismatchedPeriods, ", "),
			"archive_interval", interval,
			"impact", "Retention slots for these periods will fill at the rate of the archive interval, not the retention period.")
	}
}

// calculateEpochDays calculates the number of days since the Unix Epoch (1970-01-01)
// for a given time in a specific location. It normalizes the time to midnight
// and adds a 12-hour buffer to handle DST transitions (23h/25h days) robustly.
func calculateEpochDays(ti time.Time, location *time.Location) int64 {
	y, m, d := ti.In(location).Date()
	midnight := time.Date(y, m, d, 0, 0, 0, 0, location)
	anchor := time.Date(1970, 1, 1, 0, 0, 0, 0, location)
	// Add 12 hours to center the calculation in the day, avoiding DST jitter (23h vs 25h days).
	return int64(midnight.Sub(anchor).Hours()+12) / 24
}
