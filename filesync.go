package main

import (
	"fmt"
	"io"
	"io/fs"
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
// This is the pure Go, cross-platform implementation.
func handleSyncNative(src, dst string) error {
	// Note: This implementation does not delete files from dst that are not in src.
	log.Printf("Starting native sync from %s to %s...", src, dst)

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
			err = os.MkdirAll(dstPath, info.Mode())
			if err == nil {
				fmt.Printf("MKDIR: %s\n", relPath)
			}
			return err

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
			fmt.Printf("COPY: %s\n", relPath)
			return nil

		} else {
			// SKIP ALL OTHER TYPES (including Symbolic Links)
			// log.Printf("Skipping non-regular file or directory: %s (Type: %s)", path, fileType.String())
			return nil
		}
	})
}

// handleSyncRobocopy uses the Windows `robocopy` utility to perform a highly
// efficient and robust directory mirror. It is much faster for incremental
// backups than a manual walk. It returns a list of copied files.
func handleSyncRobocopy(src, dst string, mirror bool) error {
	// Robocopy command arguments:
	// /MIR :: MIRror a directory tree (equivalent to /E plus /PURGE).
	// /E :: copy subdirectories, including Empty ones.
	// /V :: Verbose output, showing skipped files.
	// /TEE :: output to console window as well as the log file.
	// /R:3 :: Retry 3 times on failed copies.
	// /W:5 :: Wait 5 seconds between retries.
	// /NP :: No Progress - don't display % copied.
	// /NJH :: No Job Header.
	// /NJS :: No Job Summary.
	args := []string{src, dst, "/V", "/TEE", "/R:3", "/W:5", "/NP", "/NJH", "/NJS"}
	if mirror {
		args = append(args, "/MIR")
	} else {
		args = append(args, "/E")
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
