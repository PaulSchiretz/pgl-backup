//go:build windows

package preflight

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// platformValidateMountPoint on Windows verifies that the drive or network share root for a given path exists.
// For example, for "Z:\backup", it checks if "Z:\" exists. This is analogous to the Unix "ghost directory"
// check, ensuring the target volume is actually available.
func platformValidateMountPoint(path string) error {
	volume := filepath.VolumeName(path)
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

// isUnsafeRoot checks if the given path is the current directory or a bare drive letter (e.g., "C:").
func isUnsafeRoot(path string) bool {
	// On Windows, it's unsafe to target the current directory "." or the root of the current drive "\".
	if path == "." || path == string(filepath.Separator) {
		return true
	}

	// A bare drive letter like "C:" is also unsafe because it's ambiguous.
	// filepath.Clean("C:") produces "C:.", so we must also check for that pattern.
	// A UNC path like `\\server\share` is safe because its volume name contains a separator.
	vol := filepath.VolumeName(path)
	isBareDrive := vol != "" && path == vol && !strings.Contains(vol, string(filepath.Separator))
	isCleanedBareDrive := vol != "" && path == vol+"."
	return isBareDrive || isCleanedBareDrive
}
