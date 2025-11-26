//go:build !windows

package preflight

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// IsMountPoint checks if the given path is a mount point on Unix-like systems.
// It returns true if path is a mount point, false otherwise.
func IsMountPoint(path string) (bool, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false, err
	}

	// Get the parent directory
	parent := filepath.Dir(path)
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return false, err
	}

	// Extract underlying system stats to compare Device IDs
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return false, fmt.Errorf("unsupported platform for syscall.Stat_t")
	}

	parentStat, ok := parentInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return false, fmt.Errorf("unsupported platform for syscall.Stat_t")
	}

	// If the directory and its parent have different Device IDs, it's a mount point.
	// Also handle the edge case of the root path "/" where path == parent.
	return stat.Dev != parentStat.Dev || path == parent, nil
}
