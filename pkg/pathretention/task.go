package pathretention

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// task holds the mutable state for a single execution of the retention manager.
// This makes the RetentionEngine itself stateless and safe for concurrent use if needed.
type task struct {
	*PathRetainer

	ctx         context.Context
	absBasePath string

	toPrune []metafile.MetafileInfo

	keepHours  int
	keepDays   int
	keepWeeks  int
	keepMonths int
	keepYears  int

	timestampUTC time.Time

	metrics         Metrics
	dryRun          bool
	deleteTasksChan chan metafile.MetafileInfo
	deleteWg        sync.WaitGroup
}

// execute runs the retention logic.
func (t *task) execute() error {

	// Filter the list of all backups to get only those that should be deleted.
	toDelete := t.filterToDelete()

	if len(toDelete) <= 0 {
		if t.dryRun {
			plog.Debug("[DRY RUN] No backups need deletion")
		} else {
			plog.Debug("No backups need deletion")
		}
		return nil
	}

	plog.Info("Deleting outdated backups", "count", len(toDelete))

	t.metrics.StartProgress("Delete progress", 10*time.Second)
	defer func() {
		t.metrics.StopProgress()
		t.metrics.LogSummary("Delete finished")
	}()

	// Start workers
	for range t.numWorkers {
		t.deleteWg.Add(1)
		go t.deleteWorker()
	}

	// Start producer
	go t.deleteTaskProducer(toDelete)

	t.deleteWg.Wait()

	return nil
}

// deleteTaskProducer feeds the eligible backups into the channel for workers.
func (t *task) deleteTaskProducer(eligibleBackups []metafile.MetafileInfo) {
	defer close(t.deleteTasksChan)
	for _, b := range eligibleBackups {
		select {
		case <-t.ctx.Done():
			plog.Debug("Cancellation received, stopping retention job feeding.")
			return // Stop feeding on cancel.
		case t.deleteTasksChan <- b:
		}
	}
}

// deleteWorker consumes tasks from the channel and deletes the backups.
func (t *task) deleteWorker() {
	defer t.deleteWg.Done()
	for b := range t.deleteTasksChan {
		// Check for cancellation before each deletion.
		select {
		case <-t.ctx.Done():
			return
		default:
		}

		if t.dryRun {
			plog.Notice("[DRY RUN] DELETE", "path", b.RelPathKey)
			continue
		}
		plog.Notice("DELETE", "path", b.RelPathKey)
		absPathToDelete := util.DenormalizePath(filepath.Join(t.absBasePath, b.RelPathKey))
		if err := os.RemoveAll(absPathToDelete); err != nil {
			t.metrics.AddBackupsFailed(1)
			plog.Warn("Failed to delete outdated backup directory", "path", b.RelPathKey, "error", err)
		} else {
			t.metrics.AddBackupsDeleted(1)
		}
		plog.Notice("DELETED", "path", b.RelPathKey)
	}
}

// filterToDelete identifies which backups should be deleted based on the retention policy.
func (t *task) filterToDelete() []metafile.MetafileInfo {
	toKeep := t.filterToKeep()

	var toDelete []metafile.MetafileInfo
	for _, b := range t.toPrune {
		if _, shouldKeep := toKeep[b.RelPathKey]; !shouldKeep {
			toDelete = append(toDelete, b)
		}
	}

	plog.Debug("Total unique backups to be deleted", "count", len(toDelete))
	return toDelete
}

// filterBackupsToKeep applies the retention policy to a sorted list of backups.
func (t *task) filterToKeep() map[string]bool {
	toKeep := make(map[string]bool)

	// Keep track of which periods we've already saved a backup for.
	savedHourly := make(map[string]bool)
	savedDaily := make(map[string]bool)
	savedWeekly := make(map[string]bool)
	savedMonthly := make(map[string]bool)
	savedYearly := make(map[string]bool)

	for _, b := range t.toPrune {
		// The rules are processed from shortest to longest duration.
		// Once a backup is kept, it's not considered for longer-duration rules.
		// This "promotes" a backup to the highest-frequency slot it qualifies for.

		// Rule: Keep N hourly backups.
		hourKey := b.Metadata.TimestampUTC.Format(hourFormat)
		if t.keepHours > 0 && len(savedHourly) < t.keepHours && !savedHourly[hourKey] {
			toKeep[b.RelPathKey] = true
			savedHourly[hourKey] = true
			continue // Promoted to hourly, skip other rules
		}

		// Rule: Keep N daily backups.
		dayKey := b.Metadata.TimestampUTC.Format(dayFormat)
		if t.keepDays > 0 && len(savedDaily) < t.keepDays && !savedDaily[dayKey] {
			toKeep[b.RelPathKey] = true
			savedDaily[dayKey] = true
			continue // Promoted to daily
		}

		// Rule: Keep N weekly backups.
		year, week := b.Metadata.TimestampUTC.ISOWeek()
		weekKey := fmt.Sprintf(weekFormat, year, week)
		if t.keepWeeks > 0 && len(savedWeekly) < t.keepWeeks && !savedWeekly[weekKey] {
			toKeep[b.RelPathKey] = true
			savedWeekly[weekKey] = true
			continue // Promoted to weekly
		}

		// Rule: Keep N monthly backups
		monthKey := b.Metadata.TimestampUTC.Format(monthFormat)
		if t.keepMonths > 0 && len(savedMonthly) < t.keepMonths && !savedMonthly[monthKey] {
			toKeep[b.RelPathKey] = true
			savedMonthly[monthKey] = true
			continue // Promoted to monthly
		}

		// Rule: Keep N yearly backups.
		yearKey := b.Metadata.TimestampUTC.Format(yearFormat)
		if t.keepYears > 0 && len(savedYearly) < t.keepYears && !savedYearly[yearKey] {
			toKeep[b.RelPathKey] = true
			savedYearly[yearKey] = true
		}
	}

	// Build a descriptive log message for the retention plan
	var planParts []string
	if t.keepHours > 0 {
		planParts = append(planParts, fmt.Sprintf("%d hourly", len(savedHourly)))
	}
	if t.keepDays > 0 {
		planParts = append(planParts, fmt.Sprintf("%d daily", len(savedDaily)))
	}
	if t.keepWeeks > 0 {
		planParts = append(planParts, fmt.Sprintf("%d weekly", len(savedWeekly)))
	}
	if t.keepMonths > 0 {
		planParts = append(planParts, fmt.Sprintf("%d monthly", len(savedMonthly)))
	}
	if t.keepYears > 0 {
		planParts = append(planParts, fmt.Sprintf("%d yearly", len(savedYearly)))
	}
	plog.Debug("Retention plan", "details", strings.Join(planParts, ", "))
	plog.Debug("Total unique backups to be kept", "count", len(toKeep))

	return toKeep
}
