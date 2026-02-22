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

// Archive checks if the time since the last backup has crossed the configured interval.
// If it has, it renames the current backup directory to a permanent, timestamped archive directory. It also
// prepares the archive interval before checking. It is now responsible for reading its own metadata.
func (a *PathArchiver) Archive(ctx context.Context, absBasePath, relArchivePathKey, archiveEntryPrefix string, toArchive metafile.MetafileInfo, p *Plan, timestampUTC time.Time) (metafile.MetafileInfo, error) {

	if !p.Enabled {
		plog.Debug("Archive is disabled, skipping archiving")
		return metafile.MetafileInfo{}, ErrDisabled
	}

	if toArchive.RelPathKey == "" {
		return metafile.MetafileInfo{}, ErrNothingToArchive
	}

	// Check for cancellation
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
		location:         time.Local,
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
