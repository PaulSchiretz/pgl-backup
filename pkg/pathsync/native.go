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
	numWorkers            int

	// sourcePaths is populated by the Collector and read by the Deletion phase.
	sourcePaths map[string]bool

	// wg waits for Workers to finish processing tasks.
	wg sync.WaitGroup

	// tasks is the channel where the Walker sends paths to be processed.
	tasks chan string

	// results is the channel where Workers send processed paths to the Collector.
	results chan string

	// errs captures the first error encountered to trigger cancellation.
	errs chan error

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
	defer os.Remove(out.Name())
	defer out.Close()

	// 2. Copy content
	if _, err = io.Copy(out, in); err != nil {
		return fmt.Errorf("failed to copy content from %s to %s: %w", src, out.Name(), err)
	}

	// 3. Copy file metadata (Permissions and Timestamps)
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to get stat for source file %s: %w", src, err)
	}

	if err := out.Chmod(info.Mode()); err != nil {
		return fmt.Errorf("failed to set permissions on temporary file %s: %w", out.Name(), err)
	}

	// We must sync timestamps so future runs can skip this file if it hasn't changed.
	if err := os.Chtimes(out.Name(), info.ModTime(), info.ModTime()); err != nil {
		return fmt.Errorf("failed to set timestamps on %s: %w", out.Name(), err)
	}

	// 4. Atomically move the temporary file to the final destination.
	return os.Rename(out.Name(), dst)
}

// handleDirectory ensures the destination directory exists with the correct permissions.
// Note: This function only handles the directory itself; children are handled by the Walker.
func (r *nativeSyncRun) handleDirectory(path, dstPath, relPath string, srcInfo os.FileInfo) error {
	if r.dryRun {
		plog.Info("[DRY RUN] MKDIR", "path", relPath)
		return nil
	}

	// MkdirAll returns nil if the directory already exists, which is fine.
	// It creates any necessary parent directories.
	if err := os.MkdirAll(dstPath, srcInfo.Mode()); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dstPath, err)
	}

	if !r.quiet {
		plog.Info("MKDIR", "path", relPath)
	}
	return nil
}

// handleRegularFile checks if a file needs to be copied (based on size/time)
// and triggers the copy operation if needed.
func (r *nativeSyncRun) handleRegularFile(path, dstPath, relPath string, srcInfo os.FileInfo) error {
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

// handlePath acts as the dispatcher for a specific path.
// It determines if the path is a directory, file, or something to skip.
func (r *nativeSyncRun) handlePath(currentPath, relPath string) error {
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
		return r.handleDirectory(currentPath, dstPath, relPath, srcInfo)
	case mode.IsRegular():
		return r.handleRegularFile(currentPath, dstPath, relPath, srcInfo)
	default:
		// Symlinks, Named Pipes, Sockets, etc. are currently skipped.
		if !r.quiet {
			plog.Info("SKIP", "type", mode.String(), "path", relPath)
		}
		return nil
	}
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

		// Check the map to see if this path existed in the source.
		// No lock is needed here because all workers have finished.
		if _, exists := r.sourcePaths[relPath]; !exists {

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

// worker acts as the Consumer. It reads paths from the 'tasks' channel,
// processes them (IO), and sends the relative path to the 'results' channel.
func (r *nativeSyncRun) worker() {
	defer r.wg.Done()

	for {
		select {
		case <-r.ctx.Done():
			return
		case path, ok := <-r.tasks:
			if !ok {
				// Channel closed by Walker, work is done.
				return
			}

			// 1. Derive relative path for this task.
			// We need this for the destination path calculation and for the Collector.
			relPath, err := filepath.Rel(r.src, path)
			if err != nil {
				select {
				case r.errs <- fmt.Errorf("worker failed to get relative path for %s: %w", path, err):
				default:
				}
				continue // Skip this file, try next task
			}

			// 2. Do the heavy I/O work (Copy or Mkdir)
			err = r.handlePath(path, relPath)
			if err != nil {
				// If I/O fails, report the error non-blockingly.
				select {
				case r.errs <- err:
				default:
				}
				// We purposefully fall through to step 3.
			}

			// 3. Send to the Collector.
			// Even if handlePath failed (e.g. permission error), we record that the
			// path "exists" in the source. This prevents the Deletion Phase from
			// deleting the file at the destination just because we failed to update it.
			select {
			case <-r.ctx.Done():
				return
			case r.results <- relPath:
			}
		}
	}
}

// execute coordinates the concurrent synchronization pipeline.
// It uses a Producer-Consumer-Collector pattern.
func (r *nativeSyncRun) execute() error {
	// Create a cancellable context for this run.
	// If any component fails, 'cancel()' will stop all other components.
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()
	r.ctx = ctx

	plog.Info("Spawning worker goroutines for sync phase", "count", r.numWorkers)

	// 1. Start the Collector
	// This single goroutine owns the sourcePaths map. It reads from 'results'
	// and writes to the map. This avoids the need for a Mutex on the map.
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for relPath := range r.results {
			r.sourcePaths[relPath] = true
		}
	}()

	// 2. Start Workers (Consumers)
	// They read from 'tasks', perform I/O, and write to 'results'.
	for i := 0; i < r.numWorkers; i++ {
		r.wg.Add(1)
		go r.worker()
	}

	// 3. Start the Walker (Producer)
	// This goroutine walks the file tree and feeds paths into 'tasks'.
	go func() {
		defer close(r.tasks) // Close tasks to signal workers to stop when walk is complete
		err := filepath.WalkDir(r.src, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				// If we can't access a path, stop the walk.
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case r.tasks <- path:
				return nil
			}
		})
		if err != nil {
			// If the walker fails, send the error and cancel everything.
			select {
			case r.errs <- fmt.Errorf("walker failed: %w", err):
			default:
			}
			cancel()
		}
	}()

	// 4. Wait for workers to finish processing all tasks.
	r.wg.Wait()

	// 5. Close results channel to signal Collector to stop.
	// We can only do this after all workers have called wg.Done().
	close(r.results)

	// 6. Wait for Collector to finish populating the map.
	<-collectorDone

	// 7. Check for any errors captured during the process.
	select {
	case err := <-r.errs:
		if err != nil {
			return fmt.Errorf("sync failed: %w", err)
		}
	default:
	}

	// Check if context was cancelled externally.
	if r.ctx.Err() != nil {
		return r.ctx.Err()
	}

	// 8. Deletion Phase
	// Now that the sync is done and the map is fully populated, run the deletion phase.
	if !r.mirror {
		return nil
	}
	return r.handleDelete()
}

// handleNative initializes the sync run structure and kicks off the execution.
func (s *PathSyncer) handleNative(ctx context.Context, src, dst string, mirror bool) error {
	plog.Info("Starting native sync", "from", src, "to", dst)

	run := &nativeSyncRun{
		src:         src,
		dst:         dst,
		mirror:      mirror,
		dryRun:      s.dryRun,
		quiet:       s.quiet,
		numWorkers:  s.engine.NativeEngineWorkers,
		sourcePaths: make(map[string]bool),
		// Buffer 'tasks' to handle bursts of rapid file discovery by the walker.
		tasks: make(chan string, s.engine.NativeEngineWorkers*2),
		// Buffer 'results' to ensure workers don't block waiting for the collector.
		results: make(chan string, s.engine.NativeEngineWorkers*4),
		errs:    make(chan error, 1),
		ctx:     ctx,
	}

	return run.execute()
}
