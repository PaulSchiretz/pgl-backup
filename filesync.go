package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
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

// validateSyncPaths checks that the source path exists and is a directory,
// and that the destination path can be created and is writable.
func validateSyncPaths(src, dst string) error {
	// 1. Check if source exists and is a directory.
	if srcInfo, err := os.Stat(src); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("source directory %s does not exist", src)
		}
		return fmt.Errorf("cannot stat source directory %s: %w", src, err)
	} else if !srcInfo.IsDir() {
		return fmt.Errorf("source path %s is not a directory", src)
	}

	// 2. Ensure the destination directory can be created.
	if err := os.MkdirAll(dst, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory %s: %w", dst, err)
	}

	// 3. Optionally, perform a more thorough write check.
	tempFile := filepath.Join(dst, "test_write.tmp")
	if f, err := os.Create(tempFile); err != nil {
		return fmt.Errorf("destination directory %s is not writable: %w", dst, err)
	} else {
		f.Close()
		os.Remove(tempFile)
	}

	return nil
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

// handleSyncRobocopy uses the Windows `robocopy` utility to perform a highly
// efficient and robust directory mirror. It is much faster for incremental
// backups than a manual walk. It returns a list of copied files.
func handleSyncRobocopy(src, dst string, mirror, dryRun, quiet bool) error {
	// Robocopy command arguments:
	// /MIR :: MIRror a directory tree (equivalent to /E plus /PURGE).
	// /E :: copy subdirectories, including Empty ones.
	// /V :: Verbose output, showing skipped files.
	// /TEE :: output to console window as well as the log file.
	// /R:3 :: Retry 3 times on failed copies.
	// /W:5 :: Wait 5 seconds between retries.
	// /NP :: No Progress - don't display % copied.
	// /NJH :: No Job Header.
	// /NJS :: No Job Summary. (We keep the summary for a good overview)
	args := []string{src, dst, "/V", "/TEE", "/R:3", "/W:5", "/NP", "/NJH"}
	if mirror {
		args = append(args, "/MIR")
	} else {
		args = append(args, "/E")
	}

	if quiet {
		args = append(args, "/NFL") // No File List - don't log individual files.
		args = append(args, "/NDL") // No Directory List - don't log individual directories.
	}

	if dryRun {
		args = append(args, "/L") // /L :: List only - don't copy, delete, or timestamp files.
	}

	log.Println("Starting sync with robocopy...")
	cmd := exec.Command("robocopy", args...)

	// Pipe robocopy's stdout and stderr directly to our program's stdout/stderr
	// This provides real-time logging.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	// Robocopy returns non-zero exit codes for success cases (e.g., files were copied).
	// We check if the error is a "successful" one and return nil if so.
	if err != nil && !isRobocopySuccess(err) {
		return err // It's a real error
	}
	return nil // It was a success code or no error
}

// isRobocopySuccess checks if a robocopy error is actually a success code.
// Robocopy returns exit codes < 8 for successful operations that involved copying/deleting files.
func isRobocopySuccess(err error) bool {
	if exitErr, ok := err.(*exec.ExitError); ok {
		// Exit codes 0-7 are considered success by robocopy.
		if exitErr.ExitCode() < 8 {
			return true
		}
	}
	return false
}
