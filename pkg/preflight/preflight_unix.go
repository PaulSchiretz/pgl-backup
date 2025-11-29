//go:build !windows

package preflight

import (
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// platformValidateMountPoint checks if the path resides on the root filesystem.
// If it does, it assumes the drive is NOT mounted (Ghost detection).
func platformValidateMountPoint(path string) error {
	// 1. Allow Home Directory (backups to local user folders are usually intentional)
	homeDir, _ := os.UserHomeDir()
	if strings.HasPrefix(path, homeDir) {
		return nil
	}

	// 2. Get the Device ID of the Root partition
	rootInfo, err := os.Stat("/")
	if err != nil {
		return fmt.Errorf("failed to stat root: %w", err)
	}
	rootStat, ok := rootInfo.Sys().(*unix.Stat_t)
	if !ok {
		return fmt.Errorf("unsupported platform for unix.Stat_t")
	}

	// 3. Get the Device ID of the Target path
	pathInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat target path: %w", err)
	}
	pathStat, ok := pathInfo.Sys().(*unix.Stat_t)
	if !ok {
		return fmt.Errorf("unsupported platform for unix.Stat_t")
	}

	// 4. Compare Device IDs
	// If pathDev == rootDev, we are writing to the system partition (Ghost).
	// Exception: The user specifically targeted "/" (unlikely, but valid).
	if pathStat.Dev == rootStat.Dev && path != "/" {
		return fmt.Errorf("path '%s' is on the root filesystem (system disk). "+
			"Ensure your external drive is mounted", path)
	}

	return nil
}
