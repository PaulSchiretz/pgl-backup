package pathretention

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/metafile"
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

// retentionRunState holds the state specific to a single apply operation.
type retentionRunState struct {
	dirPath              string
	retentionPolicyTitle string
	retentionPolicy      config.RetentionPolicyConfig
	excludeDir           string
	backups              []backupInfo
}

type PathRetentionManager struct {
	config config.Config
}

// backupInfo holds the parsed metadata and rel directory path of a backup found on disk.
type backupInfo struct {
	RelPathKey string // Normalized, forward-slash and maybe otherwise modified key. NOT for direct FS access.
	Metadata   metafile.MetafileContent
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
func (r *PathRetentionManager) Apply(ctx context.Context, retentionPolicyTitle string, dirPath string, retentionPolicy config.RetentionPolicyConfig, excludeDir string) error {

	runState := &retentionRunState{
		dirPath:              dirPath,
		retentionPolicyTitle: retentionPolicyTitle,
		retentionPolicy:      retentionPolicy,
		excludeDir:           excludeDir,
	}

	// Prepare for the retention run
	if err := r.prepareRun(ctx, runState); err != nil {
		return err
	}

	if runState.retentionPolicy.Hours <= 0 && runState.retentionPolicy.Days <= 0 && runState.retentionPolicy.Weeks <= 0 && runState.retentionPolicy.Months <= 0 && runState.retentionPolicy.Years <= 0 {
		plog.Debug(fmt.Sprintf("Retention policy for %s is disabled (all values are zero). Skipping.", runState.retentionPolicyTitle))
		return nil
	}

	if len(runState.backups) == 0 {
		plog.Debug("No backups to delete", "policy", runState.retentionPolicyTitle)
		return nil
	}

	// Filter the list of all backups to get only those that should be deleted.
	eligibleBackups := r.filterBackupsToDelete(runState)

	if len(eligibleBackups) == 0 {
		if r.config.DryRun {
			plog.Debug("[DRY RUN] No backups need deletion", "policy", runState.retentionPolicyTitle)
		} else {
			plog.Debug("No backups need deletion", "policy", runState.retentionPolicyTitle)
		}
		return nil
	}

	plog.Info("Deleting outdated backups", "policy", runState.retentionPolicyTitle, "count", len(eligibleBackups))

	// --- 4. Delete backups in parallel using a worker pool ---
	// This is especially effective for network drives where latency is a factor.
	numWorkers := r.config.Engine.Performance.DeleteWorkers // Use the configured number of workers.
	var wg sync.WaitGroup
	// Buffer it to 2x the workers to keep the pipeline full without wasting memory
	deleteDirTasksChan := make(chan backupInfo, numWorkers*2)

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for b := range deleteDirTasksChan {
				// Check for cancellation before each deletion.
				select {
				case <-ctx.Done():
					// Don't process any more jobs if context is cancelled.
					return
				default:
				}

				dirToDelete := filepath.Join(runState.dirPath, util.DenormalizePath(b.RelPathKey))

				if r.config.DryRun {
					plog.Notice("[DRY RUN] DELETE", "policy", runState.retentionPolicyTitle, "path", dirToDelete)
					continue
				}
				plog.Notice("DELETE", "policy", runState.retentionPolicyTitle, "path", dirToDelete, "worker", workerID)
				if err := os.RemoveAll(dirToDelete); err != nil {
					plog.Warn("Failed to delete outdated backup directory", "policy", runState.retentionPolicyTitle, "path", dirToDelete, "error", err)
				}
				plog.Notice("DELETED", "policy", runState.retentionPolicyTitle, "path", dirToDelete)
			}
		}(i + 1)
	}

	// Feed the jobs in a separate goroutine so the main function can simply wait for the workers to finish.
	go func() {
		defer close(deleteDirTasksChan)
		for _, b := range eligibleBackups {
			select {
			case <-ctx.Done():
				plog.Debug("Cancellation received, stopping retention job feeding.")
				return // Stop feeding on cancel.
			case deleteDirTasksChan <- b:
			}
		}
	}()

	wg.Wait()

	return nil
}

// prepareRun prepares the runState for an retention operation.
func (r *PathRetentionManager) prepareRun(ctx context.Context, runState *retentionRunState) error {

	// Get a sorted list of all valid backups
	err := r.fetchSortedBackups(ctx, runState)
	if err != nil {
		return err
	}
	return nil
}

// fetchSortedBackups scans a directory for valid backup folders, parses their
// metadata to get an accurate timestamp, and returns them sorted from newest to oldest.
// It relies exclusively on the `.pgl-backup.meta.json` file; directories without a
// readable metafile are ignored for retention purposes.
func (r *PathRetentionManager) fetchSortedBackups(ctx context.Context, runState *retentionRunState) error {
	prefix := r.config.Naming.Prefix

	entries, err := os.ReadDir(runState.dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			plog.Debug("Archives directory does not exist yet, no retention policy to apply.", "path", runState.dirPath)
			return nil // Not an error, just means no archives exist yet.
		}
		return fmt.Errorf("failed to read backup directory %s: %w", runState.dirPath, err)
	}

	var foundBackups []backupInfo
	for _, entry := range entries {
		// Check for cancellation during the directory scan.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		dirName := entry.Name()
		if !entry.IsDir() || !strings.HasPrefix(dirName, prefix) || dirName == runState.excludeDir {
			continue
		}

		backupPath := filepath.Join(runState.dirPath, dirName)
		metadata, err := metafile.Read(backupPath)
		if err != nil {
			plog.Warn("Skipping retention check for directory; cannot read metadata", "policy", runState.retentionPolicyTitle, "directory", dirName, "reason", err)
			continue
		}

		// The metafile is the sole source of truth for the backup time.
		foundBackups = append(foundBackups, backupInfo{RelPathKey: util.NormalizePath(dirName), Metadata: metadata})
	}

	// Sort all backups from newest to oldest for consistent processing.
	sort.Slice(foundBackups, func(i, j int) bool {
		return foundBackups[i].Metadata.TimestampUTC.After(foundBackups[j].Metadata.TimestampUTC)
	})

	// fill the runstate
	runState.backups = foundBackups
	return nil
}

// filterBackupsToDelete identifies which backups should be deleted based on the retention policy.
func (r *PathRetentionManager) filterBackupsToDelete(runState *retentionRunState) []backupInfo {
	backupsToKeep := r.determineBackupsToKeep(runState)

	var backupsToDelete []backupInfo
	for _, backup := range runState.backups {
		if _, shouldKeep := backupsToKeep[backup.RelPathKey]; !shouldKeep {
			backupsToDelete = append(backupsToDelete, backup)
		}
	}

	plog.Debug("Total unique backups to be deleted", "policy", runState.retentionPolicyTitle, "count", len(backupsToDelete))
	return backupsToDelete
}

// determineBackupsToKeep applies the retention policy to a sorted list of backups.
func (r *PathRetentionManager) determineBackupsToKeep(runState *retentionRunState) map[string]bool {
	backupsToKeep := make(map[string]bool)

	// Keep track of which periods we've already saved a backup for.
	savedHourly := make(map[string]bool)
	savedDaily := make(map[string]bool)
	savedWeekly := make(map[string]bool)
	savedMonthly := make(map[string]bool)
	savedYearly := make(map[string]bool)

	for _, b := range runState.backups {
		// The rules are processed from shortest to longest duration.
		// Once a backup is kept, it's not considered for longer-duration rules.
		// This "promotes" a backup to the highest-frequency slot it qualifies for.

		// Rule: Keep N hourly backups.
		hourKey := b.Metadata.TimestampUTC.Format(hourFormat)
		if runState.retentionPolicy.Hours > 0 && len(savedHourly) < runState.retentionPolicy.Hours && !savedHourly[hourKey] {
			backupsToKeep[b.RelPathKey] = true
			savedHourly[hourKey] = true
			continue // Promoted to hourly, skip other rules
		}

		// Rule: Keep N daily backups.
		dayKey := b.Metadata.TimestampUTC.Format(dayFormat)
		if runState.retentionPolicy.Days > 0 && len(savedDaily) < runState.retentionPolicy.Days && !savedDaily[dayKey] {
			backupsToKeep[b.RelPathKey] = true
			savedDaily[dayKey] = true
			continue // Promoted to daily
		}

		// Rule: Keep N weekly backups.
		year, week := b.Metadata.TimestampUTC.ISOWeek()
		weekKey := fmt.Sprintf(weekFormat, year, week)
		if runState.retentionPolicy.Weeks > 0 && len(savedWeekly) < runState.retentionPolicy.Weeks && !savedWeekly[weekKey] {
			backupsToKeep[b.RelPathKey] = true
			savedWeekly[weekKey] = true
			continue // Promoted to weekly
		}

		// Rule: Keep N monthly backups
		monthKey := b.Metadata.TimestampUTC.Format(monthFormat)
		if runState.retentionPolicy.Months > 0 && len(savedMonthly) < runState.retentionPolicy.Months && !savedMonthly[monthKey] {
			backupsToKeep[b.RelPathKey] = true
			savedMonthly[monthKey] = true
			continue // Promoted to monthly
		}

		// Rule: Keep N yearly backups.
		yearKey := b.Metadata.TimestampUTC.Format(yearFormat)
		if runState.retentionPolicy.Years > 0 && len(savedYearly) < runState.retentionPolicy.Years && !savedYearly[yearKey] {
			backupsToKeep[b.RelPathKey] = true
			savedYearly[yearKey] = true
		}
	}

	// Build a descriptive log message for the retention plan
	var planParts []string
	if runState.retentionPolicy.Hours > 0 {
		planParts = append(planParts, fmt.Sprintf("%d hourly", len(savedHourly)))
	}
	if runState.retentionPolicy.Days > 0 {
		planParts = append(planParts, fmt.Sprintf("%d daily", len(savedDaily)))
	}
	if runState.retentionPolicy.Weeks > 0 {
		planParts = append(planParts, fmt.Sprintf("%d weekly", len(savedWeekly)))
	}
	if runState.retentionPolicy.Months > 0 {
		planParts = append(planParts, fmt.Sprintf("%d monthly", len(savedMonthly)))
	}
	if runState.retentionPolicy.Years > 0 {
		planParts = append(planParts, fmt.Sprintf("%d yearly", len(savedYearly)))
	}
	plog.Debug("Retention plan", "policy", runState.retentionPolicyTitle, "details", strings.Join(planParts, ", "))
	plog.Debug("Total unique backups to be kept", "policy", runState.retentionPolicyTitle, "count", len(backupsToKeep))

	return backupsToKeep
}
