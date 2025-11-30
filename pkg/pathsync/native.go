package pathsync

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// syncTask holds all the necessary metadata for a worker to process a file
// without re-calculating paths or re-fetching filesystem stats.
type syncTask struct {
	AbsPath string
	RelPath string
	Info    os.FileInfo // Cached info from the Walker
	IsDir   bool
}

// nativeSyncRun encapsulates the state and logic for a single native sync operation.
// It coordinates the Producer (Walker), Consumers (Workers), and Collector.
type nativeSyncRun struct {
	src, dst              string
	mirror, dryRun, quiet bool
	numSyncWorkers        int
	excludeFiles          []string
	excludeDirs           []string
	retryCount            int
	retryWait             time.Duration

	// syncedSourcePaths is populated by the Collector and read by the Deletion phase.
	syncedSourcePaths map[string]bool

	// syncWg waits for syncWorkers to finish processing tasks.
	syncWg sync.WaitGroup

	// syncTasks is the channel where the Walker sends pre-processed tasks.
	syncTasks chan syncTask

	// syncResults is the channel where syncWorkers send processed paths to the Collector.
	syncResults chan string

	// syncErrs captures the first critical error to be reported at the end of the run.
	syncErrs chan error

	// ctx is the cancellable context for the entire run.
	ctx context.Context
}

// copyFileHelper handles the low-level details of copying a single file.
// It ensures atomicity by writing to a temporary file first and then renaming it.
func copyFileHelper(src, dst string, retryCount int, retryWait time.Duration) error {
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

			// 1. Create a temporary file in the destination directory.
			dstDir := filepath.Dir(dst)
			out, err := os.CreateTemp(dstDir, "pgl-backup-*.tmp")
			if err != nil {
				return fmt.Errorf("failed to create temporary file in %s: %w", dstDir, err)
			}

			tempPath := out.Name()
			defer os.Remove(tempPath)

			// 2. Copy content
			if _, err = io.Copy(out, in); err != nil {
				out.Close() // Close before returning on error
				return fmt.Errorf("failed to copy content from %s to %s: %w", src, tempPath, err)
			}

			// 3. Copy file metadata (Permissions and Timestamps)
			info, err := os.Stat(src)
			if err != nil {
				out.Close() // Close before returning on error
				return fmt.Errorf("failed to get stat for source file %s: %w", src, err)
			}

			if err := out.Chmod(info.Mode()); err != nil {
				out.Close() // Close before returning on error
				return fmt.Errorf("failed to set permissions on temporary file %s: %w", tempPath, err)
			}

			// 4. Close the file.
			// This flushes data to disk. It MUST be done before Chtimes,
			// because closing/flushing might update the modification time.
			if err := out.Close(); err != nil {
				return fmt.Errorf("failed to close temporary file %s: %w", tempPath, err)
			}

			// 5. Copy Timestamps
			// We do this via os.Chtimes (using the path) after the file is closed.
			if err := os.Chtimes(tempPath, info.ModTime(), info.ModTime()); err != nil {
				return fmt.Errorf("failed to set timestamps on %s: %w", tempPath, err)
			}

			// 6. Atomically move the temporary file to the final destination.
			return os.Rename(tempPath, dst)
		}()

		if lastErr == nil {
			return nil // Success
		}
	}
	return fmt.Errorf("failed to copy file %s after %d retries: %w", src, retryCount, lastErr)
}

// processDirectorySync ensures the destination directory exists and matches source metadata.
func (r *nativeSyncRun) processDirectorySync(dstPath, relPath string, srcInfo os.FileInfo) error {
	if r.dryRun {
		plog.Info("[DRY RUN] SYNCDIR", "path", relPath)
		return nil
	}

	// 1. Check if destination exists
	dstInfo, err := os.Stat(dstPath)
	if err != nil {
		if os.IsNotExist(err) {
			// It doesn't exist, create it.
			if err := os.MkdirAll(dstPath, srcInfo.Mode()); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", dstPath, err)
			}
			// Refresh dstInfo after creation to check if umask altered permissions
			dstInfo, err = os.Stat(dstPath)
			if err != nil {
				return fmt.Errorf("failed to stat newly created directory %s: %w", dstPath, err)
			}
		} else {
			// Genuine error accessing destination
			return fmt.Errorf("failed to stat destination %s: %w", dstPath, err)
		}
	} else {
		// It exists. Verify it is actually a directory.
		if !dstInfo.IsDir() {
			return fmt.Errorf("destination path %s exists but is not a directory", dstPath)
		}
	}

	// 2. Sync Permissions
	// We mask with os.ModePerm to compare only permission bits (rwxrwxrwx), ignoring file type bits.
	if dstInfo.Mode().Perm() != srcInfo.Mode().Perm() {
		if err := os.Chmod(dstPath, srcInfo.Mode()); err != nil {
			return fmt.Errorf("failed to chmod directory %s: %w", dstPath, err)
		}
	}

	// 3. Sync Timestamps
	// We strictly sync modTime. We don't check if they differ first because directory
	// times change frequently (e.g., when a file inside is created), so we force it
	// to match source at the moment of sync.
	if !dstInfo.ModTime().Equal(srcInfo.ModTime()) {
		if err := os.Chtimes(dstPath, srcInfo.ModTime(), srcInfo.ModTime()); err != nil {
			return fmt.Errorf("failed to chtimes directory %s: %w", dstPath, err)
		}
	}

	if !r.quiet {
		plog.Info("SYNCDIR", "path", relPath)
	}
	return nil
}

// processRegularFileSync checks if a file needs to be copied (based on size/time)
// and triggers the copy operation if needed.
func (r *nativeSyncRun) processRegularFileSync(path, dstPath, relPath string, srcInfo os.FileInfo) error {
	if r.dryRun {
		plog.Info("[DRY RUN] COPY", "path", relPath)
		return nil
	}

	// Check if the destination file exists and if it matches.
	dstInfo, err := os.Stat(dstPath)
	if err == nil {
		// Destination exists.
		// We skip if Source is NOT newer AND sizes match.
		if !srcInfo.ModTime().After(dstInfo.ModTime()) && srcInfo.Size() == dstInfo.Size() {
			return nil
		}
	}

	if err := copyFileHelper(path, dstPath, r.retryCount, r.retryWait); err != nil {
		return fmt.Errorf("failed to copy file to %s: %w", dstPath, err)
	}

	if !r.quiet {
		plog.Info("COPY", "path", relPath)
	}
	return nil
}

// processPathSync acts as the dispatcher for a specific path.
// It determines if the path is a directory, file, or something to skip.
// It receives the full task struct so it doesn't need to do any discovery.
func (r *nativeSyncRun) processPathSync(task syncTask) error {
	dstPath := filepath.Join(r.dst, task.RelPath)

	if task.RelPath == "." {
		return nil // The root directory itself doesn't need I/O processing
	}

	// Drectory
	if task.IsDir {
		return r.processDirectorySync(dstPath, task.RelPath, task.Info)
	}

	// Regular File
	if task.Info.Mode().IsRegular() {
		return r.processRegularFileSync(task.AbsPath, dstPath, task.RelPath, task.Info)
	}

	// Symlinks, Named Pipes, etc.
	if !r.quiet {
		plog.Info("SKIP", "type", task.Info.Mode().String(), "path", task.RelPath)
	}
	return nil
}

// isExcluded checks if a given relative path matches any of the exclusion patterns.
// It logs a warning if a pattern is invalid but continues checking other patterns.
func (r *nativeSyncRun) isExcluded(relPath string, isDir bool) bool {
	patterns := r.excludeFiles
	if isDir {
		patterns = r.excludeDirs
	}

	for _, pattern := range patterns {
		match, err := filepath.Match(pattern, relPath)
		if err != nil {
			// Log the error for the invalid pattern but continue checking others.
			plog.Warn("Invalid exclusion pattern", "pattern", pattern, "error", err)
			continue
		}
		if match {
			return true
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
			AbsPath: path,
			RelPath: relPath,
			Info:    info,
			IsDir:   d.IsDir(),
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

			// 1. Do the heavy I/O work
			err := r.processPathSync(task)
			if err != nil {
				// If I/O fails, report the error non-blockingly.
				select {
				case r.syncErrs <- err:
				default:
				}

				plog.Warn("Sync failed for path, preserving in destination map to prevent deletion",
					"path", task.RelPath,
					"error", err,
					"note", "This file may be inconsistent or outdated in the destination")
				// We purposefully fall through to step 3
			}

			// 2. Send to the Collector.
			// Even if processPathSync failed (e.g. permission error), we record that the
			// path "exists" in the source. This prevents the Deletion Phase from
			// deleting the file at the destination just because we failed to update it.
			select { //nolint:gosimple // This select is intentional for context cancellation.
			case <-r.ctx.Done():
				return
			case r.syncResults <- task.RelPath:
			}
		}
	}
}

// syncCollector is a dedicated goroutine that collects relative paths from syncResults
// and populates the syncedSourcePaths map.
func (r *nativeSyncRun) syncCollector(syncCollectorDone chan struct{}) {
	defer close(syncCollectorDone)
	for relPath := range r.syncResults {
		r.syncedSourcePaths[relPath] = true
	}
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
	err := filepath.WalkDir(r.dst, func(path string, d os.DirEntry, err error) error {
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

		relPath, err := filepath.Rel(r.dst, path)
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
		if _, exists := r.syncedSourcePaths[relPath]; !exists {

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

// handleNative initializes the sync run structure and kicks off the execution.
func (s *PathSyncer) handleNative(ctx context.Context, src, dst string, mirror bool, excludeFiles, excludeDirs []string) error {
	plog.Info("Starting native sync", "from", src, "to", dst)

	run := &nativeSyncRun{
		src:               src,
		dst:               dst,
		mirror:            mirror,
		dryRun:            s.dryRun,
		quiet:             s.quiet,
		numSyncWorkers:    s.engine.NativeEngineWorkers,
		excludeFiles:      excludeFiles,
		excludeDirs:       excludeDirs,
		retryCount:        s.engine.NativeEngineRetryCount,
		retryWait:         time.Duration(s.engine.NativeEngineRetryWaitSeconds) * time.Second,
		syncedSourcePaths: make(map[string]bool), // Initialize the map for the collector.
		// Buffer 'syncTasks' to handle bursts of rapid file discovery by the walker.
		syncTasks: make(chan syncTask, s.engine.NativeEngineWorkers*2),
		// Buffer 'syncResults' to ensure syncWorkers don't block waiting for the collector.
		syncResults: make(chan string, s.engine.NativeEngineWorkers*4),
		syncErrs:    make(chan error, 1),
		ctx:         ctx,
	}

	return run.execute()
}
