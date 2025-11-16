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

// nativeSyncJob encapsulates the state and logic for a single native sync operation.
type nativeSyncJob struct {
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
	out, err := os.CreateTemp(dstDir, "ppbackup-*.tmp")
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
func (j *nativeSyncJob) handleDirectory(path, dstPath, relPath string, srcInfo os.FileInfo) error {
	if j.dryRun {
		log.Printf("[DRY RUN] MKDIR: %s", relPath)
	} else {
		if err := os.MkdirAll(dstPath, srcInfo.Mode()); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dstPath, err)
		}
		if !j.quiet {
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
		case <-j.ctx.Done():
			return j.ctx.Err() // Stop enqueuing if context is cancelled.
		// This send is blocking. If the tasks channel is full, it will wait.
		// This naturally throttles directory reading to the speed of file processing.
		case j.tasks <- filepath.Join(path, entry.Name()):
		}
	}
	return nil
}

// handleRegularFileAsyncHelper processes a regular file path, copying it if necessary.
func (j *nativeSyncJob) handleRegularFile(path, dstPath, relPath string, srcInfo os.FileInfo) error {
	// Check if the destination file exists and if it's older.
	dstInfo, err := os.Stat(dstPath)
	if err == nil {
		// Destination exists, compare modification times and size.
		if !srcInfo.ModTime().After(dstInfo.ModTime()) && srcInfo.Size() == dstInfo.Size() {
			return nil // Skip if source is not newer.
		}
	}

	// If we reach here, we need to copy the file.
	if j.dryRun {
		log.Printf("[DRY RUN] COPY: %s", relPath)
		return nil
	}

	if err := copyFileHelper(path, dstPath); err != nil {
		return fmt.Errorf("failed to copy file to %s: %w", dstPath, err)
	}
	if !j.quiet {
		log.Printf("COPY: %s", relPath)
	}
	return nil
}

// handlePathAsyncHelper is the core logic executed by each worker goroutine.
// It processes a single path, and if it's a directory, it sends its children
// back to the tasks channel for other workers to process.
func (j *nativeSyncJob) handlePath(currentPath string) error {
	// Get relative path for logging and destination path calculation
	relPath, err := filepath.Rel(j.src, currentPath)
	if err != nil { // This should ideally not fail as currentPath is a child of j.src
		return fmt.Errorf("failed to get relative path for %s: %w", currentPath, err)
	}

	// Safely update the shared map
	j.mu.Lock()
	j.sourcePaths[relPath] = true
	j.mu.Unlock()

	srcInfo, err := os.Lstat(currentPath) // Use Lstat to not follow symlinks
	if err != nil {
		return fmt.Errorf("failed to stat source path %s: %w", currentPath, err)
	}

	dstPath := filepath.Join(j.dst, relPath)

	switch mode := srcInfo.Mode(); {
	case mode.IsDir():
		return j.handleDirectory(currentPath, dstPath, relPath, srcInfo)
	case mode.IsRegular():
		return j.handleRegularFile(currentPath, dstPath, relPath, srcInfo)
	default:
		// Skip other file types like symlinks, etc.
		return nil
	}
}

// handleDeleteSyncHelper performs a sequential BFS traversal to delete files and directories
// from the destination that are not present in the source.
func (j *nativeSyncJob) handleDelete() error {
	log.Println("Starting sequential deletion phase for mirror sync...")
	deleteQueue := []string{j.dst}
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
			fullPath := filepath.Join(currentDstPath, entry.Name())
			relPath, err := filepath.Rel(j.dst, fullPath)
			if err != nil {
				return fmt.Errorf("failed to get relative path for deletion check on %s: %w", fullPath, err)
			}

			if _, existsInSource := j.sourcePaths[relPath]; !existsInSource {
				// This path does not exist in the source, so delete it.
				if j.dryRun {
					log.Printf("[DRY RUN] DELETE (not in source): %s", relPath)
				} else {
					if !j.quiet {
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
func (j *nativeSyncJob) worker() {
	defer j.wg.Done()
	for {
		select {
		case <-j.ctx.Done(): // If context is cancelled, exit.
			return
		case path, ok := <-j.tasks:
			if !ok { // If channel is closed, exit.
				return
			}

			// Process the path
			if err := j.handlePath(path); err != nil {
				select {
				case j.errs <- err: // Send error non-blockingly
				default:
				}
				j.cancel() // Signal all other goroutines to stop.
				return
			}
		}
	}
}

// run performs the main synchronization logic.
func (j *nativeSyncJob) run() error {
	log.Printf("Spawning %d worker goroutines for sync phase.", j.numWorkers)
	for i := 0; i < j.numWorkers; i++ {
		j.wg.Add(1)
		go j.worker()
	}

	j.tasks <- j.src
	j.wg.Wait()
	close(j.tasks)
	close(j.errs)

	if err := <-j.errs; err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}

	if !j.mirror {
		return nil
	}

	return j.handleDelete()
}

// handleNative is the entry point for the native file synchronization.
func (s *PathSyncer) handleNative(src, dst string, mirror bool) error {
	log.Printf("Starting native sync from %s to %s...", src, dst)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job := &nativeSyncJob{
		src:         src,
		dst:         dst,
		mirror:      mirror,
		dryRun:      s.config.DryRun,
		quiet:       s.config.Quiet,
		numWorkers:  s.config.Engine.NativeEngineWorkers,
		sourcePaths: make(map[string]bool),
		tasks:       make(chan string, s.config.Engine.NativeEngineWorkers*2),
		errs:        make(chan error, 1),
		ctx:         ctx,
		cancel:      cancel,
	}
	return job.run()
}
