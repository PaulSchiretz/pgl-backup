package pathsync

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// syncTask holds all the necessary metadata for a worker to process a file
// without re-calculating paths or re-fetching filesystem stats.
type syncTask struct {
	SrcAbsPath string
	SrcRelPath string
	SrcInfo    os.FileInfo // Cached info from the Walker
	TrgAbsPath string
	TrgRelPath string
	IsDir      bool
	Modified   bool // True if the item was actually created or updated in the destination
}

type exclusionType int

const (
	literalMatch exclusionType = iota
	prefixMatch
	suffixMatch
	globMatch
)

// preProcessedExclusion stores the pre-analyzed pattern details.
type preProcessedExclusion struct {
	pattern      string
	cleanPattern string // The pattern without the wildcard for prefix/suffix matching
	matchType    exclusionType
}

// nativeSyncRun encapsulates the state and logic for a single native sync operation.
// It coordinates the Producer (Walker), Consumers (Workers), and Collector.
type nativeSyncRun struct {
	src, trg                 string
	mirror, dryRun, quiet    bool
	numSyncWorkers           int
	preProcessedFileExcludes []preProcessedExclusion
	preProcessedDirExcludes  []preProcessedExclusion
	retryCount               int
	retryWait                time.Duration

	// syncedSourceTasks is populated by the Collector and read by the Deletion phase.
	syncedSourceTasks map[string]syncTask

	// syncWg waits for syncWorkers to finish processing tasks.
	syncWg sync.WaitGroup

	// syncTasks is the channel where the Walker sends pre-processed tasks.
	syncTasks chan syncTask

	// syncResults is the channel where syncWorkers send processed tasks to the Collector.
	syncResults chan syncTask

	// syncErrs captures the first critical error to be reported at the end of the run.
	syncErrs chan error

	// ctx is the cancellable context for the entire run.
	ctx context.Context
}

// copyFileHelper handles the low-level details of copying a single file.
// It ensures atomicity by writing to a temporary file first and then renaming it.
func copyFileHelper(src, trg string, retryCount int, retryWait time.Duration) error {
	var lastErr error
	for i := 0; i <= retryCount; i++ {
		if i > 0 {
			plog.Warn("Retrying file copy", "file", src, "attempt", fmt.Sprintf("%d/%d", i, retryCount), "after", retryWait)
			time.Sleep(retryWait)
		}

		lastErr = func() error {
			in, err := os.Open(src)
			if err != nil {
				return fmt.Errorf("failed to open source file %s: %w", src, err)
			}
			defer in.Close()

			trgDir := filepath.Dir(trg)

			// 1. Ensure the destination directory exists.
			if err := os.MkdirAll(trgDir, 0755); err != nil {
				return fmt.Errorf("failed to ensure destination directory %s exists: %w", trgDir, err)
			}

			// 2. Create a temporary file in the destination directory.
			out, err := os.CreateTemp(trgDir, "pgl-backup-*.tmp")
			if err != nil {
				return fmt.Errorf("failed to create temporary file in %s: %w", trgDir, err)
			}

			tempPath := out.Name()
			defer os.Remove(tempPath)

			// 3. Copy content
			if _, err = io.Copy(out, in); err != nil {
				out.Close() // Close before returning on error
				return fmt.Errorf("failed to copy content from %s to %s: %w", src, tempPath, err)
			}

			// 4. Copy file metadata (Permissions and Timestamps)
			info, err := os.Stat(src)
			if err != nil {
				out.Close() // Close before returning on error
				return fmt.Errorf("failed to get stat for source file %s: %w", src, err)
			}

			if err := out.Chmod(info.Mode()); err != nil {
				out.Close() // Close before returning on error
				return fmt.Errorf("failed to set permissions on temporary file %s: %w", tempPath, err)
			}

			// 5. Close the file.
			// This flushes data to disk. It MUST be done before Chtimes,
			// because closing/flushing might update the modification time.
			if err := out.Close(); err != nil {
				return fmt.Errorf("failed to close temporary file %s: %w", tempPath, err)
			}

			// 6. Copy Timestamps
			// We do this via os.Chtimes (using the path) after the file is closed.
			if err := os.Chtimes(tempPath, info.ModTime(), info.ModTime()); err != nil {
				return fmt.Errorf("failed to set timestamps on %s: %w", tempPath, err)
			}

			// 6. Atomically move the temporary file to the final destination.
			return os.Rename(tempPath, trg)
		}()

		if lastErr == nil {
			return nil // Success
		}
	}
	return fmt.Errorf("failed to copy file %s after %d retries: %w", src, retryCount, lastErr)
}

// processFileSync checks if a file needs to be copied (based on size/time)
// and triggers the copy operation if needed.
func (r *nativeSyncRun) processFileSync(task syncTask) (syncTask, error) {
	if r.dryRun {
		plog.Info("[DRY RUN] COPY", "path", task.SrcRelPath)
		return task, nil
	}

	// Check if the destination file exists and if it matches source (size and mod time).
	trgInfo, err := os.Stat(task.TrgAbsPath)
	if err == nil {
		// Destination exists.
		// We skip if Source is NOT newer AND sizes match.
		if !task.SrcInfo.ModTime().After(trgInfo.ModTime()) && task.SrcInfo.Size() == trgInfo.Size() {
			return task, nil
		}
	}

	if err := copyFileHelper(task.SrcAbsPath, task.TrgAbsPath, r.retryCount, r.retryWait); err != nil {
		return task, fmt.Errorf("failed to copy file to %s: %w", task.TrgAbsPath, err)
	}

	if !r.quiet {
		plog.Info("COPY", "path", task.SrcRelPath)
	}
	task.Modified = true // File was actually copied/updated
	return task, nil
}

// processDirectorySync ensures the destination directory exists.
// It does NOT set final metadata (permissions, timestamps) to avoid race conditions with file workers.
// permissions, timestamps are set in a separate finalize phase
func (r *nativeSyncRun) processDirectorySync(task syncTask) (syncTask, error) {
	if r.dryRun {
		plog.Info("[DRY RUN] SYNCDIR", "path", task.SrcRelPath)
		return task, nil
	}

	// 1. Ensure the directory exists. os.MkdirAll is idempotent.
	if err := os.MkdirAll(task.TrgAbsPath, task.SrcInfo.Mode()); err != nil {
		return task, fmt.Errorf("failed to create directory %s: %w", task.TrgAbsPath, err)
	}

	// 2. Get the current state of the destination directory.
	trgInfo, err := os.Stat(task.TrgAbsPath)
	if err != nil {
		// Genuine error accessing destination
		return task, fmt.Errorf("failed to stat destination %s: %w", task.TrgAbsPath, err)
	}

	// 3. Determine if a finalization pass is needed by comparing metadata.
	if trgInfo.Mode().Perm() != task.SrcInfo.Mode().Perm() || !trgInfo.ModTime().Equal(task.SrcInfo.ModTime()) {
		// Directory exists, but permissions or modification time are wrong.
		// Mark as modified so they get fixed in the finalization pass.
		task.Modified = true
	} else {
		// Directory existed and had the correct permissions and timestamp.
		task.Modified = false
	}

	if !r.quiet {
		plog.Info("SYNCDIR", "path", task.SrcRelPath)
	}
	return task, nil
}

// processPathSync acts as the dispatcher for a specific path.
// It determines if the path is a directory, file, or something to skip.
// It receives the full task struct so it doesn't need to do any discovery.
func (r *nativeSyncRun) processPathSync(task syncTask) (syncTask, error) {

	if task.SrcRelPath == "." {
		return task, nil // The root directory itself doesn't need I/O processing
	}

	// Directory
	if task.IsDir {
		return r.processDirectorySync(task)
	}

	// Regular File
	if task.SrcInfo.Mode().IsRegular() {
		return r.processFileSync(task)
	}

	// Symlinks, Named Pipes, etc.
	if !r.quiet {
		plog.Info("SKIP", "type", task.SrcInfo.Mode().String(), "path", task.SrcRelPath)
	}
	task.Modified = false // Skipped items are not modified
	return task, nil
}

// isExcluded checks if a given relative path matches any of the exclusion patterns,
// using a tiered optimization strategy to avoid expensive glob matching when possible.
func (r *nativeSyncRun) isExcluded(relPath string, isDir bool) bool {
	// On Windows, WalkDir provides paths with `\`. We need to normalize them
	// to `/` for consistent matching with patterns.
	if filepath.Separator != '/' {
		relPath = filepath.ToSlash(relPath)
	}

	var patterns []preProcessedExclusion
	if isDir {
		patterns = r.preProcessedDirExcludes
	} else {
		patterns = r.preProcessedFileExcludes
	}

	for _, p := range patterns {
		switch p.matchType {
		case literalMatch:
			if relPath == p.pattern {
				return true
			}
		case prefixMatch:
			// Check 1: Exact match for the excluded directory/folder name (e.g., relPath == "build")
			if relPath == p.cleanPattern {
				return true
			}

			// Check 2: Match any file/dir inside the excluded folder (e.g., relPath starts with "build/")
			if strings.HasPrefix(relPath, p.cleanPattern+"/") {
				return true
			}
		case suffixMatch:
			// A pattern like "*.log" is cleaned to ".log". A simple suffix check is sufficient
			// because we only care if the path ends with this string. Unlike prefix matching,
			// there is no container/directory that needs a separate literal check.
			if strings.HasSuffix(relPath, p.cleanPattern) {
				return true
			}

		case globMatch:
			match, err := filepath.Match(p.pattern, relPath)
			if err != nil {
				// Log the error for the invalid pattern but continue checking others.
				plog.Warn("Invalid exclusion pattern", "pattern", p.pattern, "error", err)
				continue
			}
			if match {
				return true
			}
		}
	}
	return false
}

// syncWalker is a dedicated goroutine that walks the source directory tree,
// sending each syntask to the syncTasks channel for processing by workers.
func (r *nativeSyncRun) syncWalker() {
	defer close(r.syncTasks) // Close syncTasks to signal syncWorkers to stop when walk is complete

	err := filepath.WalkDir(r.src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// If we can't access a path, log the error but keep walking
			plog.Warn("Error accessing path, skipping", "path", path, "error", err)
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		relPath, err := filepath.Rel(r.src, path)
		if err != nil {
			// This should not happen if path is from WalkDir on r.src
			plog.Warn("Could not get relative path, skipping", "path", path, "error", err)
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if d.IsDir() {
			if r.isExcluded(relPath, true) {
				plog.Info("SKIPDIR", "reason", "excluded by pattern", "dir", relPath)
				return filepath.SkipDir // Don't descend into this directory.
			}
		} else {
			if r.isExcluded(relPath, false) {
				if !r.quiet {
					plog.Info("SKIP", "reason", "excluded by pattern", "file", relPath)
				}
				return nil // It's a file, just skip it.
			}
		}

		// 3. Get Info for worker
		// WalkDir gives us a DirEntry. We need the FileInfo for timestamps/sizes later.
		// Doing it here saves the worker from doing an Lstat.
		info, err := d.Info()
		if err != nil {
			plog.Warn("Failed to get file info, skipping", "path", path, "error", err)
			return nil
		}

		// 4. Create Task
		task := syncTask{
			SrcAbsPath: path,
			SrcRelPath: relPath,
			TrgRelPath: relPath,
			TrgAbsPath: filepath.Join(r.trg, relPath),
			SrcInfo:    info,
			IsDir:      d.IsDir(),
			Modified:   false, // Will be set by worker if actual change occurs
		}

		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case r.syncTasks <- task:
			return nil
		}
	})

	if err != nil {
		// If the walker fails, send the error and cancel everything.
		select {
		case r.syncErrs <- fmt.Errorf("walker failed: %w", err):
		default:
		}
	}
}

// syncWorker acts as the Consumer. It reads paths from the 'syncTasks' channel,
// processes them (IO), and sends the relative path to the 'syncResults' channel.
func (r *nativeSyncRun) syncWorker() {
	defer r.syncWg.Done()

	for {
		select {
		case <-r.ctx.Done():
			return
		case task, ok := <-r.syncTasks:
			if !ok {
				// Channel closed by Walker, work is done.
				return
			}

			// Process the task, getting back the potentially modified task
			processedTask, err := r.processPathSync(task)
			if err != nil {
				// If I/O fails, report the error non-blockingly.
				select {
				case r.syncErrs <- err:
				default:
				}
				plog.Warn("Sync failed for path, preserving in destination map to prevent deletion",
					"path", task.SrcRelPath,
					"error", err,
					"note", "This file may be inconsistent or outdated in the destination")
				// Even if failed, we still send the task to the collector to mark it as "attempted"
				// for deletion phase, but its Modified flag will be false if no actual change happened.
			}

			// Send the processed task to the Collector.
			// The collector will use the 'Modified' flag to decide if metadata needs to be applied later.
			select {
			case <-r.ctx.Done():
				return
			case r.syncResults <- processedTask:
			}
		}
	}
}

// syncCollector is a dedicated goroutine that collects relative paths from syncResults
// and populates the syncedSourcePaths map.
func (r *nativeSyncRun) syncCollector(syncCollectorDone chan struct{}) {
	defer close(syncCollectorDone)
	for task := range r.syncResults {
		r.syncedSourceTasks[task.SrcRelPath] = task
	}
}

// handleDirectoryMetadataSync iterates through all synced directories and applies their final metadata.
// This is done in a separate pass after all files have been copied to prevent race conditions.
// It processes directories from the deepest level upwards to ensure correctness.
//
// Rationale for deepest-to-highest processing:
// While `os.Chmod` and `os.Chtimes` on a child directory do not directly affect its parent's metadata,
// other filesystem operations (like creating files within a directory) can update the parent's modification time.
// Processing from deepest to highest ensures that all potential child operations are complete before
// the final metadata is applied to a parent directory, preventing accidental overwrites or inconsistencies.
func (r *nativeSyncRun) handleDirectoryMetadataSync() error {
	plog.Info("Syncing directory metadata")

	// 1. Collect all directory tasks.
	var dirTasks []syncTask
	for _, task := range r.syncedSourceTasks {
		if task.IsDir && task.SrcRelPath != "." && task.Modified { // Only process directories that were actually modified
			dirTasks = append(dirTasks, task)
		}
	}

	// 2. Sort directories from deepest to shallowest.
	// This is a robust pattern ensuring that child modifications (if any were to occur)
	// would be complete before processing the parent.
	sort.Slice(dirTasks, func(i, j int) bool {
		return len(strings.Split(dirTasks[i].SrcRelPath, string(os.PathSeparator))) > len(strings.Split(dirTasks[j].SrcRelPath, string(os.PathSeparator)))
	})

	// 3. Apply metadata in sorted order.
	for _, task := range dirTasks {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
		}

		if !r.quiet {
			plog.Info("SETMETA", "path", task.SrcRelPath)
		}
		if err := os.Chmod(task.TrgAbsPath, task.SrcInfo.Mode()); err != nil {
			return fmt.Errorf("failed to chmod directory %s: %w", task.TrgAbsPath, err)
		}
		if err := os.Chtimes(task.TrgAbsPath, task.SrcInfo.ModTime(), task.SrcInfo.ModTime()); err != nil {
			return fmt.Errorf("failed to chtimes directory %s: %w", task.TrgAbsPath, err)
		}
	}
	return nil
}

// handleSync coordinates the concurrent synchronization pipeline.
// It uses a Producer-Consumer-Collector pattern.
func (r *nativeSyncRun) handleSync() error {
	// 1. Start the syncCollector (Collector).
	// This single goroutine owns the syncedSourcePaths map. It reads from 'syncResults'
	// and writes to the map. This avoids the need for a Mutex on the map.
	syncCollectorDone := make(chan struct{})
	go r.syncCollector(syncCollectorDone)

	// 2. Start syncWorkers (Consumers).
	// They read from 'syncTasks', perform I/O, and write to 'syncResults'.
	for i := 0; i < r.numSyncWorkers; i++ {
		r.syncWg.Add(1)
		go r.syncWorker()
	}

	// 3. Start the syncWalker (Producer)
	// This goroutine walks the file tree and feeds paths into 'syncTasks'.
	go r.syncWalker()

	// 4. Wait for syncWorkers to finish processing all syncTasks.
	r.syncWg.Wait()

	// 5. Close syncResults channel to signal the syncCollector to stop.
	// We can only do this after all syncWorkers have called syncWg.Done().
	close(r.syncResults)

	// 6. Wait for syncCollector to finish populating the map.
	<-syncCollectorDone

	// --- Phase 2: Directory Metadata Finalization ---
	// This must happen after all files are copied to prevent race conditions.
	if err := r.handleDirectoryMetadataSync(); err != nil {
		return fmt.Errorf("failed to sync directory metadata: %w", err)
	}

	// 7. Check for any errors captured during the process.
	select {
	case err := <-r.syncErrs:
		if err != nil {
			return fmt.Errorf("sync failed: %w", err)
		}
	default:
	}

	return nil
}

// handleDelete performs a sequential walk on the destination
// to remove files and directories that do not exist in the source.
func (r *nativeSyncRun) handleDelete() error {
	plog.Info("Starting deletion phase")

	// WalkDir is efficient and allows us to skip trees we delete.
	err := filepath.WalkDir(r.trg, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// If path is already gone (e.g. parent deleted), just continue.
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to access path for deletion check: %w", err)
		}

		// Responsive cancellation check
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
		}

		relPath, err := filepath.Rel(r.trg, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", path, err)
		}

		if relPath == "." {
			return nil
		}

		// CRITICAL: We MUST check exclusions here.
		// If we don't, files that were skipped during the source walk (and thus not in the map)
		// will be treated as deleted files and removed from the destination.
		if r.isExcluded(relPath, d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir // Don't descend into excluded directories.
			}
			return nil // It's an excluded file, leave it alone.
		}

		// FUTURE NOTE: Case Sensitivity (Windows/macOS) Go maps are case-sensitive.
		// If Source has Image.png -> Map has key "Image.png".
		// If Dest has image.png (and the OS is case-insensitive) -> filepath.Rel returns "image.png".
		// Result: The map lookup fails, and handleDelete deletes image.png.
		// Assessment: For a strict sync tool, this is often desired behavior (enforcing case match), but on Windows, it can sometimes surprise users. For now we stick with strict!

		// Check the map to see if this path existed in the source.
		// No lock is needed here because all workers have finished.
		if _, exists := r.syncedSourceTasks[relPath]; !exists {

			if r.dryRun {
				plog.Info("[DRY RUN] DELETE", "path", relPath)
				// In dry run, we can't skip dir because we didn't actually delete it,
				// so we must visit children to log their deletion too.
				return nil
			}

			if !r.quiet {
				plog.Info("DELETE", "path", relPath)
			}

			// RemoveAll handles both files and directories recursively.
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("failed to delete %s: %w", path, err)
			}

			// Optimization: Since we deleted the directory, don't bother
			// walking into it to check its children. They are gone.
			if d.IsDir() {
				return filepath.SkipDir
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("deletion phase failed: %w", err)
	}
	return nil
}

// execute coordinates the concurrent synchronization pipeline.
// It uses a Producer-Consumer-Collector pattern.
func (r *nativeSyncRun) execute() error {
	// Create a cancellable context for this run.
	// If any component fails, 'cancel()' will stop all other components.
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()
	r.ctx = ctx // This updates the struct, so all methods see the new context

	// 1. Run the main synchronization of files and directories.
	if err := r.handleSync(); err != nil {
		return err
	}

	// Check if context was cancelled externally.
	if r.ctx.Err() != nil {
		return r.ctx.Err()
	}

	// 2. Deletion Phase
	// Now that the sync is done and the map is fully populated, run the deletion phase.
	if !r.mirror {
		return nil
	}
	return r.handleDelete()
}

// preProcessExclusions analyzes and categorizes patterns to enable optimized matching later.
func preProcessExclusions(patterns []string, isDirPatterns bool) []preProcessedExclusion {
	preProcessed := make([]preProcessedExclusion, 0, len(patterns))
	for _, p := range patterns {
		// Normalize to use forward slashes for consistent matching logic.
		p = filepath.ToSlash(p)

		if strings.ContainsAny(p, "*?[]") {
			// If it's a prefix pattern like `node_modules/*`, we can optimize it.
			if strings.HasSuffix(p, "/*") {
				preProcessed = append(preProcessed, preProcessedExclusion{
					pattern:      p,
					cleanPattern: strings.TrimSuffix(p, "/*"),
					matchType:    prefixMatch,
				})
			} else if strings.HasPrefix(p, "*") && !strings.ContainsAny(p[1:], "*?[]") {
				// If it's a suffix pattern like `*.log`, we can also optimize it.
				preProcessed = append(preProcessed, preProcessedExclusion{
					pattern:      p,
					cleanPattern: p[1:], // The part after the *, e.g., ".log"
					matchType:    suffixMatch,
				})
			} else {
				// Otherwise, it's a general glob pattern.
				preProcessed = append(preProcessed, preProcessedExclusion{pattern: p, matchType: globMatch})
			}
		} else {
			// No wildcards. Check if it's a directory prefix or a literal match.
			// Refinement: If this is the directory exclusion list OR the pattern ends in a slash,
			// we treat it as a prefix match to exclude contents inside.
			if isDirPatterns || strings.HasSuffix(p, "/") {
				preProcessed = append(preProcessed, preProcessedExclusion{
					pattern:      p,
					cleanPattern: strings.TrimSuffix(p, "/"),
					matchType:    prefixMatch,
				})
			} else {
				// Pure literal file match (e.g., "README.md")
				preProcessed = append(preProcessed, preProcessedExclusion{pattern: p, matchType: literalMatch})
			}
		}
	}
	return preProcessed
}

// handleNative initializes the sync run structure and kicks off the execution.
func (s *PathSyncer) handleNative(ctx context.Context, src, trg string, mirror bool, excludeFiles, excludeDirs []string) error {
	plog.Info("Starting native sync", "from", src, "to", trg)

	run := &nativeSyncRun{
		src:                      src,
		trg:                      trg,
		mirror:                   mirror,
		dryRun:                   s.dryRun,
		quiet:                    s.quiet,
		numSyncWorkers:           s.engine.NativeEngineWorkers,
		preProcessedFileExcludes: preProcessExclusions(excludeFiles, false),
		preProcessedDirExcludes:  preProcessExclusions(excludeDirs, true),
		retryCount:               s.engine.NativeEngineRetryCount,
		retryWait:                time.Duration(s.engine.NativeEngineRetryWaitSeconds) * time.Second,
		syncedSourceTasks:        make(map[string]syncTask), // Initialize the map for the collector.
		// Buffer 'syncTasks' to handle bursts of rapid file discovery by the walker.
		syncTasks: make(chan syncTask, s.engine.NativeEngineWorkers*2),
		// Buffer 'syncResults' to ensure syncWorkers don't block waiting for the collector.
		syncResults: make(chan syncTask, s.engine.NativeEngineWorkers*4),
		syncErrs:    make(chan error, 1),
		ctx:         ctx,
	}

	return run.execute()
}
