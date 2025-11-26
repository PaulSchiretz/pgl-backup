// Package preflight provides functions for validation and checks that run before
// a main operation begins. These checks are designed to be stateless and
// idempotent, ensuring the system is in a suitable state for an operation to
// proceed without changing the system's state itself.
package preflight

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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
	// --- 1. Windows: Check if the Volume/Drive exists ---
	// This specifically catches "Device not ready" or missing mapped drives (e.g., Z:)
	volume := filepath.VolumeName(targetPath)
	if volume != "" {
		// On Windows, this is "C:", on Unix it's ""
		if _, err := os.Stat(volume); os.IsNotExist(err) {
			return fmt.Errorf("target path root does not exist: %s. Ensure the drive is connected and accessible", volume)
		}
	}

	// --- 2. Check existence and type ---
	info, err := os.Stat(targetPath)
	if os.IsNotExist(err) {
		// --- 3. If it doesn't exist, check if the parent directory is accessible
		parentDir := filepath.Dir(targetPath)
		if _, err := os.Stat(parentDir); os.IsNotExist(err) {
			return fmt.Errorf("target path and parent directory do not exist: %s", targetPath)
		}
		// The parent directory exists, so it's okay to create the target directory later
		return nil
	} else if err != nil {
		// Some other error occurred during stat (e.g., permission denied)
		return fmt.Errorf("cannot access target path: %w", err)
	} else {
		// The target path exists. Make sure it's a directory.
		if !info.IsDir() {
			return fmt.Errorf("target path exists but is not a directory: %s", targetPath)
		}

		// --- 4. Unix: Check for unmounted "ghost" directories ---
		// This prevents writing to the root filesystem if a drive is not mounted.
		// We walk up from the target path to find the highest-level directory that
		// actually exists and check if it's a mount point.
		if runtime.GOOS != "windows" {
			// Heuristic: only apply this check for paths outside /home.
			// Backups to a user's home directory are less likely to be on a separate mount.
			homeDir, _ := os.UserHomeDir()
			if !strings.HasPrefix(targetPath, homeDir) {
				// Walk up the path to find the highest existing directory.
				// e.g., for "/mnt/backup/data", if "/mnt/backup" exists, we check that.
				pathToCheck := targetPath
				for {
					parent := filepath.Dir(pathToCheck)
					if _, err := os.Stat(parent); os.IsNotExist(err) || parent == pathToCheck {
						break // Stop if parent doesn't exist or we've hit the root.
					}
					pathToCheck = parent
				}

				isMounted, err := IsMountPoint(pathToCheck)
				if err == nil && !isMounted && pathToCheck != "/" {
					return fmt.Errorf("path '%s' appears to be on the system disk but is expected to be a mount point. Ensure drive is connected before proceeding", pathToCheck)
				}
			}
		}

		return nil
	}
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
	if f, err := os.Create(tempFile); err != nil {
		return fmt.Errorf("target directory %s is not writable: %w", targetPath, err)
	} else {
		f.Close()
		_ = os.Remove(tempFile) // We don't need to handle the error on this cleanup.
	}

	return nil
}
