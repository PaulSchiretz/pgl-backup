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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/metafile"
	"pixelgardenlabs.io/pgl-backup/pkg/pathretentionmetrics"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
	"pixelgardenlabs.io/pgl-backup/pkg/util"
)

// Constants for time formats used in retention bucketing
const (
	hourFormat  = "2006-01-02-15" // YYYY-MM-DD-HH
	dayFormat   = "2006-01-02"    // YYYY-MM-DD
	weekFormat  = "%d-%d"         // Sprintf format for "YYYY-WW" using year and ISO week number (weeks start on Monday). Go's time package does not have a layout code (like WW). The only way to get the ISO week is to call the time.ISOWeek() method
	monthFormat = "2006-01"       // YYYY-MM
	yearFormat  = "2006"          // YYYY
)

// retentionRun holds the mutable state for a single execution of the retention manager.
// This makes the RetentionEngine itself stateless and safe for concurrent use if needed.
type retentionRun struct {
	ctx                  context.Context
	dirPath              string
	retentionPolicyTitle string
	retentionPolicy      config.RetentionPolicyConfig
	backups              []metafile.MetafileInfo
	dryRun               bool
	metrics              pathretentionmetrics.Metrics
	numWorkers           int
}

type PathRetentionManager struct {
	config config.Config
}

// RetentionManager defines the interface for a component that applies a retenpolicy to backups.
type RetentionManager interface {
	Apply(ctx context.Context, policyTitle string, dirPath string, retentionPolicy config.RetentionPolicyConfig, excludeDir string) error
}

// Statically assert that *PathRetentionManager implements the RetentionManager interface.
var _ RetentionManager = (*PathRetentionManager)(nil)

// NewPathRetentionManager creates a new PathRetentionManager with the given configuration.
func NewPathRetentionManager(cfg config.Config) *PathRetentionManager {
	return &PathRetentionManager{
		config: cfg,
	}
}

// Apply scans a given directory and deletes backups
// that are no longer needed according to the passed retention policy.
func (rm *PathRetentionManager) Apply(ctx context.Context, retentionPolicyTitle string, dirPath string, retentionPolicy config.RetentionPolicyConfig, excludeDir string) error {
	if retentionPolicy.Hours <= 0 && retentionPolicy.Days <= 0 && retentionPolicy.Weeks <= 0 && retentionPolicy.Months <= 0 && retentionPolicy.Years <= 0 {
		plog.Debug(fmt.Sprintf("Retention policy for %s is disabled (all values are zero). Skipping.", retentionPolicyTitle))
		return nil
	}

	var m pathretentionmetrics.Metrics
	if rm.config.Metrics {
		m = &pathretentionmetrics.RetentionMetrics{}
	} else {
		m = &pathretentionmetrics.NoopMetrics{}
	}

	run := &retentionRun{
		ctx:                  ctx,
		dirPath:              dirPath,
		retentionPolicyTitle: retentionPolicyTitle,
		retentionPolicy:      retentionPolicy,
		dryRun:               rm.config.DryRun,
		metrics:              m,
		numWorkers:           rm.config.Engine.Performance.DeleteWorkers,
	}

	// Get a sorted list of all valid backups
	var err error
	run.backups, err = rm.fetchSortedBackups(ctx, dirPath, excludeDir, retentionPolicyTitle)
	if err != nil {
		return err
	}
	return run.execute()
}

// fetchSortedBackups scans a directory for valid backup folders, parses their
// metadata to get an accurate timestamp, and returns them sorted from newest to oldest.
// It relies exclusively on the `.pgl-backup.meta.json` file; directories without a
// readable metafile are ignored for retention purposes.
func (rm *PathRetentionManager) fetchSortedBackups(ctx context.Context, dirPath, excludeDir, policyTitle string) ([]metafile.MetafileInfo, error) {
	prefix := rm.config.Naming.Prefix

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			plog.Debug("Archives directory does not exist yet, no retention policy to apply.", "path", dirPath)
			return []metafile.MetafileInfo{}, nil // Not an error, just means no archives exist yet.
		}
		return []metafile.MetafileInfo{}, fmt.Errorf("failed to read backup directory %s: %w", dirPath, err)
	}

	var foundBackups []metafile.MetafileInfo
	for _, entry := range entries {
		// Check for cancellation during the directory scan.
		select {
		case <-ctx.Done():
			return []metafile.MetafileInfo{}, ctx.Err()
		default:
		}

		dirName := entry.Name()
		if !entry.IsDir() || !strings.HasPrefix(dirName, prefix) || dirName == excludeDir {
			continue
		}

		backupPath := filepath.Join(dirPath, dirName)
		metadata, err := metafile.Read(backupPath)
		if err != nil {
			plog.Warn("Skipping retention check for directory; cannot read metadata", "policy", policyTitle, "directory", dirName, "reason", err)
			continue
		}

		// The metafile is the sole source of truth for the backup time.
		foundBackups = append(foundBackups, metafile.MetafileInfo{RelPathKey: util.NormalizePath(dirName), Metadata: metadata})
	}

	// Sort all backups from newest to oldest for consistent processing.
	sort.Slice(foundBackups, func(i, j int) bool {
		return foundBackups[i].Metadata.TimestampUTC.After(foundBackups[j].Metadata.TimestampUTC)
	})
	return foundBackups, nil
}

// execute runs the retention logic.
func (r *retentionRun) execute() error {

	if len(r.backups) == 0 {
		plog.Debug("No backups to delete", "policy", r.retentionPolicyTitle)
		return nil
	}

	// Filter the list of all backups to get only those that should be deleted.
	eligibleBackups := r.filterBackupsToDelete()

	if len(eligibleBackups) == 0 {
		if r.dryRun {
			plog.Debug("[DRY RUN] No backups need deletion", "policy", r.retentionPolicyTitle)
		} else {
			plog.Debug("No backups need deletion", "policy", r.retentionPolicyTitle)
		}
		return nil
	}

	plog.Info("Deleting outdated backups", "policy", r.retentionPolicyTitle, "count", len(eligibleBackups))

	r.metrics.StartProgress("Delete progress", 10*time.Second)
	defer func() {
		r.metrics.StopProgress()
		r.metrics.LogSummary("Delete finished")
	}()

	// --- 4. Delete backups in parallel using a worker pool ---
	// This is especially effective for network drives where latency is a factor.
	// Buffer it to 2x the workers to keep the pipeline full without wasting memory
	deleteDirTasksChan := make(chan metafile.MetafileInfo, r.numWorkers*2)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < r.numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for b := range deleteDirTasksChan {
				// Check for cancellation before each deletion.
				select {
				case <-r.ctx.Done():
					// Don't process any more jobs if context is cancelled.
					return
				default:
				}

				dirToDelete := filepath.Join(r.dirPath, util.DenormalizePath(b.RelPathKey))

				if r.dryRun {
					plog.Notice("[DRY RUN] DELETE", "policy", r.retentionPolicyTitle, "path", dirToDelete)
					continue
				}
				plog.Notice("DELETE", "policy", r.retentionPolicyTitle, "path", dirToDelete, "worker", workerID)
				if err := os.RemoveAll(dirToDelete); err != nil {
					r.metrics.AddBackupsFailed(1)
					plog.Warn("Failed to delete outdated backup directory", "policy", r.retentionPolicyTitle, "path", dirToDelete, "error", err)
				} else {
					r.metrics.AddBackupsDeleted(1)
				}
				plog.Notice("DELETED", "policy", r.retentionPolicyTitle, "path", dirToDelete)
			}
		}(i + 1)
	}

	// Feed the jobs in a separate goroutine so the main function can simply wait for the workers to finish.
	go func() {
		defer close(deleteDirTasksChan)
		for _, b := range eligibleBackups {
			select {
			case <-r.ctx.Done():
				plog.Debug("Cancellation received, stopping retention job feeding.")
				return // Stop feeding on cancel.
			case deleteDirTasksChan <- b:
			}
		}
	}()

	wg.Wait()

	return nil
}

// filterBackupsToDelete identifies which backups should be deleted based on the retention policy.
func (r *retentionRun) filterBackupsToDelete() []metafile.MetafileInfo {
	backupsToKeep := r.determineBackupsToKeep()

	var backupsToDelete []metafile.MetafileInfo
	for _, backup := range r.backups {
		if _, shouldKeep := backupsToKeep[backup.RelPathKey]; !shouldKeep {
			backupsToDelete = append(backupsToDelete, backup)
		}
	}

	plog.Debug("Total unique backups to be deleted", "policy", r.retentionPolicyTitle, "count", len(backupsToDelete))
	return backupsToDelete
}

// determineBackupsToKeep applies the retention policy to a sorted list of backups.
func (r *retentionRun) determineBackupsToKeep() map[string]bool {
	backupsToKeep := make(map[string]bool)

	// Keep track of which periods we've already saved a backup for.
	savedHourly := make(map[string]bool)
	savedDaily := make(map[string]bool)
	savedWeekly := make(map[string]bool)
	savedMonthly := make(map[string]bool)
	savedYearly := make(map[string]bool)

	for _, b := range r.backups {
		// The rules are processed from shortest to longest duration.
		// Once a backup is kept, it's not considered for longer-duration rules.
		// This "promotes" a backup to the highest-frequency slot it qualifies for.

		// Rule: Keep N hourly backups.
		hourKey := b.Metadata.TimestampUTC.Format(hourFormat)
		if r.retentionPolicy.Hours > 0 && len(savedHourly) < r.retentionPolicy.Hours && !savedHourly[hourKey] {
			backupsToKeep[b.RelPathKey] = true
			savedHourly[hourKey] = true
			continue // Promoted to hourly, skip other rules
		}

		// Rule: Keep N daily backups.
		dayKey := b.Metadata.TimestampUTC.Format(dayFormat)
		if r.retentionPolicy.Days > 0 && len(savedDaily) < r.retentionPolicy.Days && !savedDaily[dayKey] {
			backupsToKeep[b.RelPathKey] = true
			savedDaily[dayKey] = true
			continue // Promoted to daily
		}

		// Rule: Keep N weekly backups.
		year, week := b.Metadata.TimestampUTC.ISOWeek()
		weekKey := fmt.Sprintf(weekFormat, year, week)
		if r.retentionPolicy.Weeks > 0 && len(savedWeekly) < r.retentionPolicy.Weeks && !savedWeekly[weekKey] {
			backupsToKeep[b.RelPathKey] = true
			savedWeekly[weekKey] = true
			continue // Promoted to weekly
		}

		// Rule: Keep N monthly backups
		monthKey := b.Metadata.TimestampUTC.Format(monthFormat)
		if r.retentionPolicy.Months > 0 && len(savedMonthly) < r.retentionPolicy.Months && !savedMonthly[monthKey] {
			backupsToKeep[b.RelPathKey] = true
			savedMonthly[monthKey] = true
			continue // Promoted to monthly
		}

		// Rule: Keep N yearly backups.
		yearKey := b.Metadata.TimestampUTC.Format(yearFormat)
		if r.retentionPolicy.Years > 0 && len(savedYearly) < r.retentionPolicy.Years && !savedYearly[yearKey] {
			backupsToKeep[b.RelPathKey] = true
			savedYearly[yearKey] = true
		}
	}

	// Build a descriptive log message for the retention plan
	var planParts []string
	if r.retentionPolicy.Hours > 0 {
		planParts = append(planParts, fmt.Sprintf("%d hourly", len(savedHourly)))
	}
	if r.retentionPolicy.Days > 0 {
		planParts = append(planParts, fmt.Sprintf("%d daily", len(savedDaily)))
	}
	if r.retentionPolicy.Weeks > 0 {
		planParts = append(planParts, fmt.Sprintf("%d weekly", len(savedWeekly)))
	}
	if r.retentionPolicy.Months > 0 {
		planParts = append(planParts, fmt.Sprintf("%d monthly", len(savedMonthly)))
	}
	if r.retentionPolicy.Years > 0 {
		planParts = append(planParts, fmt.Sprintf("%d yearly", len(savedYearly)))
	}
	plog.Debug("Retention plan", "policy", r.retentionPolicyTitle, "details", strings.Join(planParts, ", "))
	plog.Debug("Total unique backups to be kept", "policy", r.retentionPolicyTitle, "count", len(backupsToKeep))

	return backupsToKeep
}
