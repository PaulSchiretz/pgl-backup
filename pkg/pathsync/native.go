package pathsync

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// nativeSyncRun encapsulates the state and logic for a single native sync operation.
// It coordinates the Producer (Walker), Consumers (Workers), and Collector.
type nativeSyncRun struct {
	src, dst              string
	mirror, dryRun, quiet bool
	numSyncWorkers        int
	filesToIgnore         []string

	// syncedSourcePaths is populated by the Collector and read by the Deletion phase.
	syncedSourcePaths map[string]bool

	// syncWg waits for syncWorkers to finish processing tasks.
	syncWg sync.WaitGroup

	// syncTasks is the channel where the Walker sends paths to be processed.
	syncTasks chan string

	// syncResults is the channel where syncWorkers send processed paths to the Collector.
	syncResults chan string

	// syncErrs captures the first error encountered to trigger cancellation.
	syncErrs chan error

	// ctx is the cancellable context for the entire run.
	ctx context.Context
}

// copyFileHelper handles the low-level details of copying a single file.
// It ensures atomicity by writing to a temporary file first and then renaming it.
func copyFileHelper(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", src, err)
	}
	defer in.Close()

	// 1. Create a temporary file in the destination directory.
	// This ensures that we don't overwrite the destination until the copy is complete
	// and that the rename operation will be on the same filesystem (making it atomic).
	dstDir := filepath.Dir(dst)
	out, err := os.CreateTemp(dstDir, "pgl-backup-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary file in %s: %w", dstDir, err)
	}

	// Clean up the temporary file if any step fails before the final rename.
	// If Rename succeeds, this Remove call will simply fail (harmlessly) or do nothing.
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

	// Explicitly close the file handle before any filesystem operations that require it (like Chtimes or Rename).
	if err := out.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file %s: %w", tempPath, err)
	}

	// We must sync timestamps so future runs can skip this file if it hasn't changed.
	if err := os.Chtimes(tempPath, info.ModTime(), info.ModTime()); err != nil {
		return fmt.Errorf("failed to set timestamps on %s: %w", tempPath, err)
	}

	// 4. Atomically move the temporary file to the final destination.
	return os.Rename(tempPath, dst)
}

// processDirectorySync ensures the destination directory exists with the correct permissions.
// Note: This function only handles the directory itself; children are handled by the Walker.
func (r *nativeSyncRun) processDirectorySync(path, dstPath, relPath string, srcInfo os.FileInfo) error {
	if r.dryRun {
		plog.Info("[DRY RUN] SYNCDIR", "path", relPath)
		return nil
	}

	// MkdirAll returns nil if the directory already exists, which is fine.
	// It creates any necessary parent directories.
	if err := os.MkdirAll(dstPath, srcInfo.Mode()); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dstPath, err)
	}

	if !r.quiet {
		plog.Info("SYNCDIR", "path", relPath)
	}
	return nil
}

// processRegularFileSync checks if a file needs to be copied (based on size/time)
// and triggers the copy operation if needed.
func (r *nativeSyncRun) processRegularFileSync(path, dstPath, relPath string, srcInfo os.FileInfo) error {
	// Check if the destination file exists and if it matches.
	dstInfo, err := os.Stat(dstPath)
	if err == nil {
		// Destination exists.
		// We skip if Source is NOT newer AND sizes match.
		if !srcInfo.ModTime().After(dstInfo.ModTime()) && srcInfo.Size() == dstInfo.Size() {
			return nil
		}
	}

	if r.dryRun {
		plog.Info("[DRY RUN] COPY", "path", relPath)
		return nil
	}

	if err := copyFileHelper(path, dstPath); err != nil {
		return fmt.Errorf("failed to copy file to %s: %w", dstPath, err)
	}

	if !r.quiet {
		plog.Info("COPY", "path", relPath)
	}
	return nil
}

// processPathSync acts as the dispatcher for a specific path.
// It determines if the path is a directory, file, or something to skip.
func (r *nativeSyncRun) processPathSync(currentPath, relPath string) error {
	srcInfo, err := os.Lstat(currentPath) // Lstat prevents following symlinks implicitly
	if err != nil {
		return fmt.Errorf("failed to stat source path %s: %w", currentPath, err)
	}

	dstPath := filepath.Join(r.dst, relPath)

	switch mode := srcInfo.Mode(); {
	case relPath == ".":
		// The root directory itself doesn't need I/O processing,
		// though its existence is recorded by the caller.
		return nil
	case mode.IsDir():
		return r.processDirectorySync(currentPath, dstPath, relPath, srcInfo)
	case mode.IsRegular():
		return r.processRegularFileSync(currentPath, dstPath, relPath, srcInfo)
	default:
		// Symlinks, Named Pipes, Sockets, etc. are currently skipped.
		if !r.quiet {
			plog.Info("SKIP", "type", mode.String(), "path", relPath)
		}
		return nil
	}
}

// syncWalker is a dedicated goroutine that walks the source directory tree,
// sending each path to the syncTasks channel for processing by workers.
func (r *nativeSyncRun) syncWalker() {
	defer close(r.syncTasks) // Close syncTasks to signal syncWorkers to stop when walk is complete

	err := filepath.WalkDir(r.src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// If we can't access a path, stop the walk.
			return err
		}

		// Check if the file or directory should be ignored.
		for _, fileToIgnore := range r.filesToIgnore {
			if d.Name() == fileToIgnore {
				if d.IsDir() {
					return filepath.SkipDir // Don't descend into this directory.
				}
				return nil // It's a file, just skip it.
			}
		}

		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case r.syncTasks <- path:
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
		case path, ok := <-r.syncTasks:
			if !ok {
				// Channel closed by Walker, work is done.
				return
			}

			// 1. Derive relative path for this task.
			// We need this for the destination path calculation and for the Collector.
			relPath, err := filepath.Rel(r.src, path)
			if err != nil {
				select {
				case r.syncErrs <- fmt.Errorf("worker failed to get relative path for %s: %w", path, err):
				default:
				}
				return // Return to stop the sync safely. To avoid accidential deletion from destination in handleDelete we need to treat this as a critical error and stop the worker.
			}

			// 2. Do the heavy I/O work (Copy or Mkdir)
			err = r.processPathSync(path, relPath)
			if err != nil {
				// If I/O fails, report the error non-blockingly.
				select {
				case r.syncErrs <- err:
				default:
				}
				// We purposefully fall through to step 3.
			}

			// 3. Send to the Collector.
			// Even if processPathSync failed (e.g. permission error), we record that the
			// path "exists" in the source. This prevents the Deletion Phase from
			// deleting the file at the destination just because we failed to update it.
			select { //nolint:gosimple // This select is intentional for context cancellation.
			case <-r.ctx.Done():
				return
			case r.syncResults <- relPath:
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

// handleDelete performs a sequential Breadth-First Search (BFS) on the destination
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

		// Never delete the metadata file. It's essential for the engine's retention logic
		// and is intentionally not present in the source directory.
		for _, fileToIgnore := range r.filesToIgnore {
			if d.Name() == fileToIgnore {
				return nil
			}
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
func (s *PathSyncer) handleNative(ctx context.Context, src, dst string, mirror bool, filesToIgnore []string) error {
	plog.Info("Starting native sync", "from", src, "to", dst)

	run := &nativeSyncRun{
		src:               src,
		dst:               dst,
		mirror:            mirror,
		dryRun:            s.dryRun,
		quiet:             s.quiet,
		numSyncWorkers:    s.engine.NativeEngineWorkers,
		filesToIgnore:     filesToIgnore,
		syncedSourcePaths: make(map[string]bool), // Initialize the map for the collector.
		// Buffer 'syncTasks' to handle bursts of rapid file discovery by the walker.
		syncTasks: make(chan string, s.engine.NativeEngineWorkers*2),
		// Buffer 'syncResults' to ensure syncWorkers don't block waiting for the collector.
		syncResults: make(chan string, s.engine.NativeEngineWorkers*4),
		syncErrs:    make(chan error, 1),
		ctx:         ctx,
	}

	return run.execute()
}
