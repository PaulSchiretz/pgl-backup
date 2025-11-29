// Package preflight provides functions for validation and checks that run before
// a main operation begins. These checks are designed to be stateless and
// idempotent, ensuring the system is in a suitable state for an operation to
// proceed without changing the system's state itself.
package preflight

import (
	"fmt"
	"os"
	"path/filepath"

	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// CheckBackupTargetAccessible performs pre-flight checks to ensure the backup target is usable.
// It provides more user-friendly errors than letting os.MkdirAll fail.
//
// The checks include:
//  1. On Windows, verifies that the drive or network share (e.g., "Z:", "\\Server\Share") exists.
//  2. If the target path exists, confirms it is a directory.
//  3. If the target path does not exist, it confirms its immediate parent directory is accessible.
//  4. On Unix, it verifies that the target path is not on the system disk when it's expected to be
//     on a separate mounted drive. This prevents writing to a "ghost" directory if a drive is not
//     mounted. This check is performed on the target path if it exists, or its deepest existing
//     ancestor if it does not.
func CheckBackupTargetAccessible(targetPath string) error {
	// --- 1. Platform-specific: Check if the Volume/Drive exists (Windows) ---
	info, err := os.Stat(targetPath)
	if os.IsNotExist(err) {
		// Target doesn't exist. We must check its ancestors.

		// Find the Deepest Existing Ancestor
		ancestor := targetPath
		for {
			parent := filepath.Dir(ancestor)
			if parent == ancestor {
				break // Hit root
			}
			if _, err := os.Stat(parent); err == nil {
				ancestor = parent
				break // Found the deepest directory that actually exists
			}
			ancestor = parent
		}

		// Platform-specific: Validate the ancestor (e.g., Unix mount point check)
		if err := platformValidateMountPoint(ancestor); err != nil {
			return err
		}

		// The target path doesn't exist. Check accessibility of the deepest existing ancestor
		// to ensure os.MkdirAll can create the required subdirectories. This provides a
		// more specific error message on permission failure letting any subsequent os.MkdirAll fail alone.
		if _, err := os.Stat(ancestor); err != nil {
			return fmt.Errorf("cannot access ancestor directory %s: %w", ancestor, err)
		}

		return nil
	} else if err != nil {
		return fmt.Errorf("cannot access target path: %w", err)
	}

	// --- 2. The Target Path Exists ---
	if !info.IsDir() {
		return fmt.Errorf("target path exists but is not a directory: %s", targetPath)
	}

	// Platform-specific: If the folder exists, we check it directly (e.g., Unix mount point check).
	if err := platformValidateMountPoint(targetPath); err != nil {
		return err
	}

	return nil
}

// CheckBackupSourceAccessible validates that the source path exists and is a directory.
func CheckBackupSourceAccessible(srcPath string) error {
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("source directory %s does not exist", srcPath)
		}
		return fmt.Errorf("cannot stat source directory %s: %w", srcPath, err)
	}

	if !srcInfo.IsDir() {
		return fmt.Errorf("source path %s is not a directory", srcPath)
	}

	return nil
}

// CheckBackupTargetWritable ensures the target directory can be created and is writable
// by performing filesystem modifications.
func CheckBackupTargetWritable(targetPath string) error {
	// This function assumes the directory has been created by the caller.
	// It first verifies the path exists and is a directory.
	info, err := os.Stat(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("target directory does not exist: %s", targetPath)
		}
		return fmt.Errorf("cannot stat target directory %s: %w", targetPath, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("target path exists but is not a directory: %s", targetPath)
	}

	// Perform a thorough write check by creating and deleting a temporary file.
	tempFile := filepath.Join(targetPath, ".pgl-backup-writetest.tmp")
	if f, err := os.Create(tempFile); err != nil {
		return fmt.Errorf("target directory %s is not writable: %w", targetPath, err)
	} else {
		f.Close()
	}

	if err := os.Remove(tempFile); err != nil {
		// This is not a critical failure, but worth logging.
		plog.Warn("Failed to remove temporary write-test file", "path", tempFile, "error", err)
	}
	return nil
}
