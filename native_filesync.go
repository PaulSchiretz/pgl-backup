package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

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
func handleDirectoryAsyncHelper(ctx context.Context, path, dstPath, relPath string, srcInfo os.FileInfo, tasks chan<- string, dryRun, quiet bool) error {
	if dryRun {
		log.Printf("[DRY RUN] MKDIR: %s", relPath)
	} else {
		if err := os.MkdirAll(dstPath, srcInfo.Mode()); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dstPath, err)
		}
		if !quiet {
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
		case <-ctx.Done():
			return ctx.Err() // Stop enqueuing if context is cancelled.
		// This send is blocking. If the tasks channel is full, it will wait.
		// This naturally throttles directory reading to the speed of file processing.
		case tasks <- filepath.Join(path, entry.Name()):
		}
	}
	return nil
}

// handleRegularFileAsyncHelper processes a regular file path, copying it if necessary.
func handleRegularFileAsyncHelper(path, dstPath, relPath string, srcInfo os.FileInfo, dryRun, quiet bool) error {
	// Check if the destination file exists and if it's older.
	dstInfo, err := os.Stat(dstPath)
	if err == nil {
		// Destination exists, compare modification times and size.
		if !srcInfo.ModTime().After(dstInfo.ModTime()) && srcInfo.Size() == dstInfo.Size() {
			return nil // Skip if source is not newer.
		}
	}

	// If we reach here, we need to copy the file.
	if dryRun {
		log.Printf("[DRY RUN] COPY: %s", relPath)
		return nil
	}

	if err := copyFileHelper(path, dstPath); err != nil {
		return fmt.Errorf("failed to copy file to %s: %w", dstPath, err)
	}
	if !quiet {
		log.Printf("COPY: %s", relPath)
	}
	return nil
}

// handlePathAsyncHelper is the core logic executed by each worker goroutine.
// It processes a single path, and if it's a directory, it sends its children
// back to the tasks channel for other workers to process.
func handlePathAsyncHelper(ctx context.Context, currentPath, src, dst string, sourcePaths *map[string]bool, mu *sync.Mutex, tasks chan<- string, dryRun, quiet bool) error {
	// Get relative path for logging and destination path calculation
	relPath, err := filepath.Rel(src, currentPath)
	if err != nil { // This should ideally not fail as currentPath is a child of src
		return fmt.Errorf("failed to get relative path for %s: %w", currentPath, err)
	}

	// Safely update the shared map
	mu.Lock()
	(*sourcePaths)[relPath] = true
	mu.Unlock()

	srcInfo, err := os.Lstat(currentPath) // Use Lstat to not follow symlinks
	if err != nil {
		return fmt.Errorf("failed to stat source path %s: %w", currentPath, err)
	}

	dstPath := filepath.Join(dst, relPath)

	switch mode := srcInfo.Mode(); {
	case mode.IsDir():
		return handleDirectoryAsyncHelper(ctx, currentPath, dstPath, relPath, srcInfo, tasks, dryRun, quiet)
	case mode.IsRegular():
		return handleRegularFileAsyncHelper(currentPath, dstPath, relPath, srcInfo, dryRun, quiet)
	default:
		// Skip other file types like symlinks, etc.
		return nil
	}
}

// handleDeleteSyncHelper performs a sequential BFS traversal to delete files and directories
// from the destination that are not present in the source.
func handleDeleteSyncHelper(dst string, sourcePaths map[string]bool, dryRun, quiet bool) error {
	log.Println("Starting sequential deletion phase for mirror sync...")
	deleteQueue := []string{dst}
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
			relPath, err := filepath.Rel(dst, fullPath)
			if err != nil {
				return fmt.Errorf("failed to get relative path for deletion check on %s: %w", fullPath, err)
			}

			if _, existsInSource := sourcePaths[relPath]; !existsInSource {
				// This path does not exist in the source, so delete it.
				if dryRun {
					log.Printf("[DRY RUN] DELETE (not in source): %s", relPath)
				} else {
					if !quiet {
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

// handleSyncNative incrementally copies files from src to dst, only if the source file
// is newer than the destination file. It also logs the copied files.
// It uses a breadth-first search (BFS) to traverse the source directory, ensuring
// parent directories are processed before their contents. This implementation uses a
// worker pool of goroutines to process files in parallel.
func handleSyncNative(src, dst string, mirror, dryRun, quiet bool, numWorkers int) error {
	log.Printf("Starting native sync from %s to %s...", src, dst)

	// Create a map to store all relative paths found in the source.
	// This makes the deletion phase safer, as it doesn't re-stat the source.
	sourcePaths := make(map[string]bool)
	var sourcePathsMutex sync.Mutex

	// Use a context to signal cancellation to all workers if an error occurs.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancel is called on exit.

	var wg sync.WaitGroup
	tasks := make(chan string, runtime.NumCPU()*2) // Buffered channel
	errs := make(chan error, 1)                    // Buffered channel to hold the first error.

	// --- Phase 1: Concurrently copy/update files from source to destination ---
	log.Printf("Spawning %d worker goroutines for sync phase.", numWorkers)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done(): // If context is cancelled, exit.
					return
				case path, ok := <-tasks:
					if !ok { // If channel is closed, exit.
						return
					}

					// Process the path
					if err := handlePathAsyncHelper(ctx, path, src, dst, &sourcePaths, &sourcePathsMutex, tasks, dryRun, quiet); err != nil {
						select {
						case errs <- err: // Send error non-blockingly
						default:
						}
						cancel() // Signal all other goroutines to stop.
						return
					}
				}
			}
		}()
	}

	// Start the process with the root source directory.
	tasks <- src
	wg.Wait()
	close(tasks) // Close channel to signal workers that no more tasks are coming.
	close(errs)  // Close the error channel after all workers are done.

	// Check if any worker sent an error.
	if err := <-errs; err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}

	// --- Phase 2: Delete files/dirs from destination that are not in source (if mirroring) ---
	if !mirror {
		return nil
	}

	// The deletion phase remains sequential as it's generally faster and safer.
	return handleDeleteSyncHelper(dst, sourcePaths, dryRun, quiet)
}
