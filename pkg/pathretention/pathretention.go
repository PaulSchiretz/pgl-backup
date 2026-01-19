// --- ARCHITECTURAL OVERVIEW: Retention Strategy ---
//
// The retention logic implements a "Consistent History" strategy to organize backups
// into intuitive, calendar-based slots for cleanup.
//
// Goal:  To ensure that retention rules (e.g., "keep one backup from last week") always refer
//        to standard calendar periods defined in UTC, providing a clean and portable history.
//
// Logic: The `determineBackupsToKeep` function applies fixed calendar concepts (like ISO weeks
//        and YYYY-MM-DD dates) to the **UTC timestamp** stored in each backup's metadata.

// Package pathretention implements the logic for cleaning up outdated backups
// based on configurable retention policies (hourly, daily, weekly, etc.).
package pathretention

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathretentionmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// Constants for time formats used in retention bucketing
const (
	hourFormat  = "2006-01-02-15" // YYYY-MM-DD-HH
	dayFormat   = "2006-01-02"    // YYYY-MM-DD
	weekFormat  = "%d-%d"         // Sprintf format for "YYYY-WW" using year and ISO week number (weeks start on Monday). Go's time package does not have a layout code (like WW). The only way to get the ISO week is to call the time.ISOWeek() method
	monthFormat = "2006-01"       // YYYY-MM
	yearFormat  = "2006"          // YYYY
)

var ErrDisabled = errors.New("retention policy is disabled")
var ErrNothingToPrune = errors.New("nothing to prune")

type PathRetainer struct {
	numWorkers int
}

// NewPathRetainer creates a new PathRetainer with the given configuration.
func NewPathRetainer(numWorkers int) *PathRetainer {
	return &PathRetainer{
		numWorkers: numWorkers,
	}
}

// Prune scans a given backups and deletes backups
// that are no longer needed according to the passed retention policy.
func (r *PathRetainer) Prune(ctx context.Context, absTargetBasePath string, toPrune []metafile.MetafileInfo, p *Plan, timestampUTC time.Time) error {

	if !p.Enabled {
		plog.Debug("Retention policy is disabled, skipping prune")
		return ErrDisabled
	}

	// NOTE: Even if retention is 0,0,0,0,0 and enabled, the backups that we just created are filetered before a call to this func. So it is safe to simply do what we are told.
	// Check for cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(toPrune) == 0 {
		return ErrNothingToPrune
	}

	// Sort all backups from newest to oldest for consistent processing.
	sort.Slice(toPrune, func(i, j int) bool {
		return toPrune[i].Metadata.TimestampUTC.After(toPrune[j].Metadata.TimestampUTC)
	})

	var m pathretentionmetrics.Metrics
	if p.Metrics {
		m = &pathretentionmetrics.RetentionMetrics{}
	} else {
		m = &pathretentionmetrics.NoopMetrics{}
	}

	t := &task{
		PathRetainer:      r,
		ctx:               ctx,
		absTargetBasePath: absTargetBasePath,
		keepHours:         p.Hours,
		keepDays:          p.Days,
		keepWeeks:         p.Weeks,
		keepMonths:        p.Months,
		keepYears:         p.Years,
		toPrune:           toPrune,
		timestampUTC:      timestampUTC,
		metrics:           m,
		dryRun:            p.DryRun,
		deleteTasksChan:   make(chan metafile.MetafileInfo, r.numWorkers*2),
	}
	return t.execute()
}
