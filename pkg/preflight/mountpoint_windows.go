//go:build windows

package preflight

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// validateMountPoint encapsulates the Unix logic to keep the main function clean, nothing to do on windows
func validateMountPoint(path string) error {
	return nil
}

// checkVolumeExists verifies that the drive or network share root for a given path exists.
// For example, for "Z:\backup", it checks if "Z:\" exists.
func checkVolumeExists(targetPath string) error {
	volume := filepath.VolumeName(targetPath)
	if volume == "" {
		return nil // Not a path with a volume name (e.g., relative path), so nothing to check.
	}

	// 1. Start with the volume name (e.g., "C:" or "\\Server\Share")
	checkVol := volume

	// 2. Append the separator if it's missing (converts "C:" to "C:\")
	if !strings.HasSuffix(checkVol, string(filepath.Separator)) {
		checkVol += string(filepath.Separator)
	}

	// 3. Clean the resulting path for normalization.
	checkVol = filepath.Clean(checkVol)

	if _, err := os.Stat(checkVol); os.IsNotExist(err) {
		return fmt.Errorf("volume root does not exist: %s. Ensure the drive is connected", checkVol)
	}
	return nil
}
