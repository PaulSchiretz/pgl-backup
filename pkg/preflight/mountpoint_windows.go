//go:build windows

package preflight

import "path/filepath"

// IsMountPoint checks if the given path is the root of a volume (e.g., "C:\").
// This implementation uses only the standard library and does not detect
// volumes mounted to folders, but it covers the most common case for backups
// to a separate drive.
func IsMountPoint(path string) (bool, error) {
	// A path is the root of a volume if its VolumeName plus a separator equals the path itself.
	// For example, for "C:\", VolumeName is "C:" and "C:" + "\" == "C:\".
	// For "C:\Users", VolumeName is "C:" and "C:" + "\" != "C:\Users".
	return filepath.VolumeName(path)+string(filepath.Separator) == path, nil
}
