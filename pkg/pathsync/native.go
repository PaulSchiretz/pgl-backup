package pathsync

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// nativeSyncRun encapsulates the state and logic for a single native sync operation.
type nativeSyncRun struct {
	src, dst              string
	mirror, dryRun, quiet bool
	numWorkers            int

	sourcePaths map[string]bool
	mu          sync.Mutex
	wg          sync.WaitGroup
	tasks       chan string
	errs        chan error
	ctx         context.Context
	cancel      context.CancelFunc
}

// copyFileHelper handles copying a regular file from src to dst and preserves permissions.
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
	defer os.Remove(out.Name())
	defer out.Close()

	// 2. Copy content
	if _, err = io.Copy(out, in); err != nil {
		return fmt.Errorf("failed to copy content from %s to %s: %w", src, out.Name(), err)
	}

	// 3. Copy file mode (permissions)
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to get stat for source file %s: %w", src, err)
	}
	if err := out.Chmod(info.Mode()); err != nil {
		return fmt.Errorf("failed to set permissions on temporary file %s: %w", out.Name(), err)
	}

	// 4. Atomically move the temporary file to the final destination.
	return os.Rename(out.Name(), dst)
}

// handleDirectoryAsyncHelper processes a directory path. It creates the corresponding
// destination directory and adds its children to the task queue.
func (r *nativeSyncRun) handleDirectory(path, dstPath, relPath string, srcInfo os.FileInfo) error {
	if r.dryRun {
		log.Printf("[DRY RUN] MKDIR: %s", relPath)
	} else {
		if err := os.MkdirAll(dstPath, srcInfo.Mode()); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dstPath, err)
		}
		if !r.quiet {
			log.Printf("MKDIR: %s", relPath)
		}
	}

	// Enqueue children for the next level of traversal
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %w", path, err)
	}
	for _, entry := range entries {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err() // Stop enqueuing if context is cancelled.
		// This send is blocking. If the tasks channel is full, it will wait.
		// This naturally throttles directory reading to the speed of file processing.
		case r.tasks <- filepath.Join(path, entry.Name()):
		}
	}
	return nil
}

// handleRegularFileAsyncHelper processes a regular file path, copying it if necessary.
func (r *nativeSyncRun) handleRegularFile(path, dstPath, relPath string, srcInfo os.FileInfo) error {
	// Check if the destination file exists and if it's older.
	dstInfo, err := os.Stat(dstPath)
	if err == nil {
		// Destination exists, compare modification times and size.
		if !srcInfo.ModTime().After(dstInfo.ModTime()) && srcInfo.Size() == dstInfo.Size() {
			return nil // Skip if source is not newer.
		}
	}

	// If we reach here, we need to copy the file.
	if r.dryRun {
		log.Printf("[DRY RUN] COPY: %s", relPath)
		return nil
	}

	if err := copyFileHelper(path, dstPath); err != nil {
		return fmt.Errorf("failed to copy file to %s: %w", dstPath, err)
	}
	if !r.quiet {
		log.Printf("COPY: %s", relPath)
	}
	return nil
}

// handlePathAsyncHelper is the core logic executed by each worker goroutine.
// It processes a single path, and if it's a directory, it sends its children
// back to the tasks channel for other workers to process.
func (r *nativeSyncRun) handlePath(currentPath string) error {
	// Get relative path for logging and destination path calculation
	relPath, err := filepath.Rel(r.src, currentPath)
	if err != nil { // This should ideally not fail as currentPath is a child of r.src
		return fmt.Errorf("failed to get relative path for %s: %w", currentPath, err)
	}

	// Safely update the shared map
	r.mu.Lock()
	r.sourcePaths[relPath] = true
	r.mu.Unlock()

	srcInfo, err := os.Lstat(currentPath) // Use Lstat to not follow symlinks
	if err != nil {
		return fmt.Errorf("failed to stat source path %s: %w", currentPath, err)
	}

	dstPath := filepath.Join(r.dst, relPath)

	switch mode := srcInfo.Mode(); {
	case mode.IsDir():
		return r.handleDirectory(currentPath, dstPath, relPath, srcInfo)
	case mode.IsRegular():
		return r.handleRegularFile(currentPath, dstPath, relPath, srcInfo)
	default:
		if !r.quiet {
			log.Printf("SKIP (unsupported file type: %s): %s", mode.String(), relPath)
		}
		return nil
	}
}

// handleDeleteSyncHelper performs a sequential BFS traversal to delete files and directories
// from the destination that are not present in the source.
func (r *nativeSyncRun) handleDelete() error {
	log.Println("Starting sequential deletion phase for mirror sync...")

	// Check for cancellation before starting the delete phase.
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	default:
	}

	deleteQueue := []string{r.dst}
	for len(deleteQueue) > 0 {
		currentDstPath := deleteQueue[0]
		deleteQueue = deleteQueue[1:]

		entries, err := os.ReadDir(currentDstPath)
		if err != nil {
			// If the directory was already deleted (as part of a parent), just skip.
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("failed to read destination directory for deletion check: %w", err)
		}

		for _, entry := range entries {
			// Check for cancellation inside the loop to be responsive.
			select {
			case <-r.ctx.Done():
				return r.ctx.Err()
			default:
			}

			fullPath := filepath.Join(currentDstPath, entry.Name())
			relPath, err := filepath.Rel(r.dst, fullPath)
			if err != nil {
				return fmt.Errorf("failed to get relative path for deletion check on %s: %w", fullPath, err)
			}

			if _, existsInSource := r.sourcePaths[relPath]; !existsInSource {
				// This path does not exist in the source, so delete it.
				if r.dryRun {
					log.Printf("[DRY RUN] DELETE (not in source): %s", relPath)
				} else {
					if !r.quiet {
						log.Printf("DELETE (not in source): %s", relPath)
					}
					if err := os.RemoveAll(fullPath); err != nil {
						return fmt.Errorf("failed to delete %s: %w", fullPath, err)
					}
				}
			} else if entry.IsDir() {
				// If it exists in the source and is a directory, check its children.
				deleteQueue = append(deleteQueue, fullPath)
			}
		}
	}
	return nil
}

// worker is the main goroutine function for processing tasks.
func (r *nativeSyncRun) worker() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done(): // If context is cancelled, exit.
			return
		case path, ok := <-r.tasks:
			if !ok { // If channel is closed, exit.
				return
			}

			// Process the path
			if err := r.handlePath(path); err != nil {
				select {
				case r.errs <- err: // Send error non-blockingly
				default:
				}
				r.cancel() // Signal all other goroutines to stop.
				return
			}
		}
	}
}

// execute performs the main synchronization logic.
func (r *nativeSyncRun) execute() error {
	log.Printf("Spawning %d worker goroutines for sync phase.", r.numWorkers)
	for i := 0; i < r.numWorkers; i++ {
		r.wg.Add(1)
		go r.worker()
	}

	r.tasks <- r.src
	r.wg.Wait()
	close(r.tasks)
	close(r.errs)

	if err := <-r.errs; err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}

	if !r.mirror {
		return nil
	}

	return r.handleDelete()
}

// handleNative is the entry point for the native file synchronization.
func (s *PathSyncer) handleNative(ctx context.Context, src, dst string, mirror bool) error {
	log.Printf("Starting native sync from %s to %s...", src, dst)

	run := &nativeSyncRun{
		src:         src,
		dst:         dst,
		mirror:      mirror,
		dryRun:      s.dryRun,
		quiet:       s.quiet,
		numWorkers:  s.engine.NativeEngineWorkers,
		sourcePaths: make(map[string]bool),
		tasks:       make(chan string, s.engine.NativeEngineWorkers*2),
		errs:        make(chan error, 1),
		ctx:         ctx,
	}
	// The cancel function is derived from the context passed into the worker.
	// It's used to signal an internal error to other workers.
	run.ctx, run.cancel = context.WithCancel(ctx)
	defer run.cancel()

	return run.execute()
}
