package main

import (
	"fmt"
	"os"
	"path/filepath"
)

// validateSyncPaths checks that the source path exists and is a directory,
// and that the destination path can be created and is writable.
func validateSyncPaths(src, dst string, dryRun bool) error {
	// 1. Check if source exists and is a directory.
	if srcInfo, err := os.Stat(src); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("source directory %s does not exist", src)
		}
		return fmt.Errorf("cannot stat source directory %s: %w", src, err)
	} else if !srcInfo.IsDir() {
		return fmt.Errorf("source path %s is not a directory", src)
	}

	if dryRun {
		return nil // Skip filesystem modifications in dry run mode.
	}

	// 2. Ensure the destination directory can be created.
	if err := os.MkdirAll(dst, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory %s: %w", dst, err)
	}

	// 3. Optionally, perform a more thorough write check.
	tempFile := filepath.Join(dst, ".ppBackup_write_test.tmp")
	if f, err := os.Create(tempFile); err != nil {
		return fmt.Errorf("destination directory %s is not writable: %w", dst, err)
	} else {
		f.Close()
		_ = os.Remove(tempFile) // We don't need to handle the error on this cleanup.
	}

	return nil
}
