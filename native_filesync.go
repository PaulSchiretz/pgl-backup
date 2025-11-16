package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

// copyFile handles copying a regular file from src to dst and preserves permissions.
func copyFile(src, dst string) error {

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

// handleSyncNative incrementally copies files from src to dst, only if the source file
// is newer than the destination file. It also logs the copied files.
// It uses a breadth-first search (BFS) to traverse the source directory, ensuring
// parent directories are processed before their contents.
func handleSyncNative(src, dst string, mirror, dryRun, quiet bool) error {
	log.Printf("Starting native sync from %s to %s...", src, dst)

	// Create a map to store all relative paths found in the source.
	// This makes the deletion phase safer, as it doesn't re-stat the source.
	sourcePaths := make(map[string]bool)

	// --- Phase 1: Copy/update files from source to destination using BFS ---
	// We use a queue for a breadth-first (level-order) traversal of the source directory.
	queue := []string{src}
	for len(queue) > 0 {
		// Dequeue the current path
		currentPath := queue[0]
		queue = queue[1:]

		// Get relative path for logging and destination path calculation
		relPath, err := filepath.Rel(src, currentPath)
		if err != nil { // This should ideally not fail as currentPath is a child of src
			return fmt.Errorf("failed to get relative path for %s: %w", currentPath, err)
		}
		sourcePaths[relPath] = true // Record every valid path from the source

		srcInfo, err := os.Lstat(currentPath) // Use Lstat to not follow symlinks
		if err != nil {
			return fmt.Errorf("failed to stat source path %s: %w", currentPath, err)
		}

		dstPath := filepath.Join(dst, relPath)

		switch mode := srcInfo.Mode(); {
		case mode.IsDir():
			// HANDLE DIRECTORY
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
			entries, err := os.ReadDir(currentPath)
			if err != nil {
				return fmt.Errorf("failed to read directory %s: %w", currentPath, err)
			}
			for _, entry := range entries {
				queue = append(queue, filepath.Join(currentPath, entry.Name()))
			}

		case mode.IsRegular():
			// HANDLE REGULAR FILE
			// Check if the destination file exists and if it's older.
			dstInfo, err := os.Stat(dstPath)
			if err == nil {
				// Destination exists, compare modification times and size.
				if !srcInfo.ModTime().After(dstInfo.ModTime()) && srcInfo.Size() == dstInfo.Size() {
					continue // Skip if source is not newer.
				}
			}

			// If we reach here, we need to copy the file.
			if dryRun {
				log.Printf("[DRY RUN] COPY: %s", relPath)
				continue
			}

			if err := copyFile(currentPath, dstPath); err != nil {
				return fmt.Errorf("failed to copy file to %s: %w", dstPath, err)
			}
			if !quiet {
				log.Printf("COPY: %s", relPath)
			}

		default:
			// Skip other file types like symlinks, etc.
			// log.Printf("Skipping non-regular file or directory: %s", relPath)
		}
	}

	// --- Phase 2: Delete files/dirs from destination that are not in source (if mirroring) ---
	if !mirror {
		return nil
	}

	log.Println("Starting deletion phase for mirror sync...")

	// We use a separate queue for a BFS traversal of the destination for deletion.
	// This allows us to prune entire directories efficiently.
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
