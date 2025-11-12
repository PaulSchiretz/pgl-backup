package main

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

// syncDirTree incrementally copies files from src to dst, only if the source file
// is newer than the destination file. It also logs the copied files.
func syncDirTree(src, dst string, logFunc func(string)) error {
	// --- 1. PRE-CHECK: Check if the destination directory exists and is writable ---
	// We check if we can create a temporary file in the destination.
	// We use MkdirAll first in case the destination directory does not exist yet.
	if err := os.MkdirAll(dst, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory %s: %w", dst, err)
	}

	tempFile := filepath.Join(dst, "test_write.tmp")
	if f, err := os.Create(tempFile); err != nil {
		return fmt.Errorf("destination directory %s is not writable: %w", dst, err)
	} else {
		f.Close()
		// Clean up the temporary file after successful check
		os.Remove(tempFile)
	}
	// -------------------------------------------------------------------------------

	log.Printf("Starting incremental sync from %s to %s. Destination is writable.", src, dst)

	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err // Stop if there's an error accessing a path
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dst, relPath)
		fileType := d.Type()

		if fileType.IsDir() {
			// HANDLE DIRECTORIES
			info, err := d.Info()
			if err != nil {
				return err
			}
			return os.MkdirAll(dstPath, info.Mode())

		} else if fileType.IsRegular() {
			// HANDLE REGULAR FILES
			srcInfo, err := d.Info()
			if err != nil {
				return err
			}

			// Check if the destination file exists and if it's older.
			dstInfo, err := os.Stat(dstPath)
			if err == nil {
				// Destination exists, compare modification times and size.
				if !srcInfo.ModTime().After(dstInfo.ModTime()) && srcInfo.Size() == dstInfo.Size() {
					return nil // Skip if source is not newer.
				}
			}

			// If we reach here, we need to copy the file.
			if err := copyFile(path, dstPath); err != nil {
				return err
			}

			// Log the copied file
			logFunc(relPath)
			return nil

		} else {
			// SKIP ALL OTHER TYPES (including Symbolic Links)
			// log.Printf("Skipping non-regular file or directory: %s (Type: %s)", path, fileType.String())
			return nil
		}
	})
}

// syncDirTreeRobocopy uses the Windows `robocopy` utility to perform a highly
// efficient and robust directory mirror. It is much faster for incremental
// backups than a manual walk. It returns a list of copied files.
func syncDirTreeRobocopy(src, dst string, mirror bool) ([]string, error) {
	// Ensure the destination directory exists. Robocopy can create it, but
	// it's good practice to ensure the parent exists and is writable.
	if err := os.MkdirAll(dst, 0755); err != nil {
		return nil, fmt.Errorf("failed to create destination directory %s: %w", dst, err)
	}

	// Robocopy command arguments:
	// /MIR :: MIRror a directory tree (equivalent to /E plus /PURGE).
	// /E :: copy subdirectories, including Empty ones.
	// /PURGE :: delete destination files/dirs that no longer exist in source.
	// /R:3 :: Retry 3 times on failed copies.
	// /W:5 :: Wait 5 seconds between retries.
	// /NP :: No Progress - don't display % copied.
	// /NJH :: No Job Header.
	// /NJS :: No Job Summary.
	// /NDL :: No Directory List - don't log directory names.
	// /L :: List only - don't copy, delete, or timestamp any files. We use this for a dry run.
	args := []string{src, dst, "/R:3", "/W:5", "/NP", "/NJH", "/NJS", "/NDL", "/L"}
	if mirror {
		args = append(args, "/MIR")
	} else {
		args = append(args, "/E")
	}
	cmd := exec.Command("robocopy", args...)

	log.Println("Performing dry run with robocopy to find changed files...")
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Robocopy exit codes < 8 indicate success (with files copied, etc.)
		// We check the exit code to see if it's a real error.
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() >= 8 {
				return nil, fmt.Errorf("robocopy dry run failed with exit code %d: %s", exitError.ExitCode(), string(output))
			}
		} else {
			return nil, fmt.Errorf("failed to execute robocopy dry run: %w", err)
		}
	}

	copiedFiles := strings.Fields(strings.TrimSpace(string(output)))

	log.Println("Performing actual sync with robocopy...")
	// Remove the /L (List only) flag for the actual copy operation.
	actualArgs := []string{src, dst, "/R:3", "/W:5"}
	if mirror {
		actualArgs = append(actualArgs, "/MIR")
	} else {
		actualArgs = append(actualArgs, "/E")
	}
	cmd = exec.Command("robocopy", actualArgs...)

	return copiedFiles, cmd.Run()
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
