// Package preflight provides functions for validation and checks that run before
// a main operation begins. These checks are usually designed to be stateless and
// idempotent, on exception are dir exists checks, ensuring the system is in a suitable state for an operation to
// proceed without changing the system's state itself.
package preflight

import (
	"fmt"
	"os"
	"path/filepath"
)

// CheckBackupTargetAccessible performs pre-flight checks to ensure the backup target is usable.
// It provides more user-friendly errors than letting os.MkdirAll fail.
//
// The checks include:
//  1. On Windows, verifies that the drive or network share (e.g., "Z:", "\\Server\Share") exists.
//  2. If the target path exists, confirms it is a directory.
//  3. If the target path does not exist, confirms its parent directory is accessible.
//  4. On Unix, if the path looks like a mount point, it verifies the device is actually mounted
//     to prevent writing to a "ghost" directory on the root filesystem. This is done by walking
//     up from the target path and checking the highest-level existing directory.
func CheckBackupTargetAccessible(targetPath string) error {
	// --- 1. Check if the Volume/Drive exists, windows only ---
	if err := checkVolumeExists(targetPath); err != nil {
		return err
	}

	// --- 2. Check existence and type ---
	info, err := os.Stat(targetPath)
	if os.IsNotExist(err) {
		// Target doesn't exist. We must check the potential parent.
		// If /mnt/backup/my-backup doesn't exist, is /mnt/backup mounted?

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

		// Validate the ancestor
		if err := validateMountPoint(ancestor); err != nil {
			return err
		}

		// If we got here, the ancestor exists and (if required) is a mount.
		// However, we still need to ensure the immediate parent of the target is accessible
		// so MkdirAll won't fail due to permissions on the parent.
		parentDir := filepath.Dir(targetPath)
		if _, err := os.Stat(parentDir); os.IsNotExist(err) {
			// The immediate parent does not exist. This is an error condition for this check,
			// as it implies a deeper non-existent path than expected.
			return fmt.Errorf("target path and its parent directory do not exist: %s", parentDir)
		} else if err != nil {
			// A different error occurred (e.g., permissions).
			return fmt.Errorf("cannot access parent directory %s: %w", parentDir, err)
		}

		return nil
	} else if err != nil {
		return fmt.Errorf("cannot access target path: %w", err)
	}

	// --- 3. The Target Path Exists ---
	if !info.IsDir() {
		return fmt.Errorf("target path exists but is not a directory: %s", targetPath)
	}

	// If the folder exists, we check it specifically.
	if err := validateMountPoint(targetPath); err != nil {
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
	// Ensure the destination directory can be created.
	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return fmt.Errorf("failed to create target directory %s: %w", targetPath, err)
	}

	// Perform a thorough write check by creating and deleting a temporary file.
	tempFile := filepath.Join(targetPath, ".pgl-backup-writetest.tmp")
	f, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("target directory %s is not writable: %w", targetPath, err)
	}
	f.Close()
	_ = os.Remove(tempFile)
	return nil
}
