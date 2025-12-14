package util

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Permission constants for file and directory modes.
const (
	// PermUserRead is the user-read permission bit (0400).
	PermUserRead os.FileMode = 0400
	// PermUserWrite is the user-write permission bit (0200).
	PermUserWrite os.FileMode = 0200
	// PermUserExecute is the user-execute permission bit (0100).
	PermUserExecute os.FileMode = 0100

	// UserWritableDirPerms represents the standard permissions for newly created directories (rwxr-xr-x).
	UserWritableDirPerms os.FileMode = 0755
	// UserWritableFilePerms represents the standard permissions for newly created files (rw-r--r--).
	UserWritableFilePerms os.FileMode = 0644
	// UserGroupWritableFilePerms represents permissions for files that should be writable by the user and group (rw-rw-r--).
	UserGroupWritableFilePerms os.FileMode = 0664
)

// WithUserReadPermission ensures that any directory/file permission has the owner-read
// bit (0400) set. This is necessary for inspecting the contents of the file or directory.
func WithUserReadPermission(basePerm os.FileMode) os.FileMode {
	return basePerm | PermUserRead
}

// WithUserWritePermission ensures that any directory/file permission has the owner-write
// bit (0200) set. This prevents the backup user from being locked out on subsequent runs.
func WithUserWritePermission(basePerm os.FileMode) os.FileMode {
	return basePerm | PermUserWrite
}

// WithUserExecutePermission ensures that any directory/file permission has the owner-execute
// bit (0100) set. This is crucial for directories (allowing access/traversal)
// and necessary for running scripts/programs.
func WithUserExecutePermission(basePerm os.FileMode) os.FileMode {
	return basePerm | PermUserExecute
}

// IsHostCaseInsensitiveFS checks if the current operating system (the "host") has a case-insensitive filesystem by default.
func IsHostCaseInsensitiveFS() bool {
	return runtime.GOOS == "windows" || runtime.GOOS == "darwin"
}

// ExpandPath expands the tilde (~) prefix in a path to the user's home directory.
func ExpandPath(path string) (string, error) {
	if !strings.HasPrefix(path, "~") {
		return path, nil // No tilde, return as-is.
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("could not get user home directory: %w", err)
	}

	// Replace the tilde with the home directory.
	return filepath.Join(home, path[1:]), nil
}

// InvertMap takes a map[K]V and returns a map[V]K.
// It's a generic helper for creating reverse lookup maps for enums.
func InvertMap[K comparable, V comparable](m map[K]V) map[V]K {
	inv := make(map[V]K, len(m))
	for k, v := range m {
		inv[v] = k
	}
	return inv
}

// MergeAndDeduplicate combines multiple string slices into a single slice,
// removing any duplicate entries.
func MergeAndDeduplicate(slices ...[]string) []string {
	// Use a map to automatically handle duplicates.
	combined := make(map[string]struct{})
	for _, s := range slices {
		for _, item := range s {
			combined[item] = struct{}{}
		}
	}

	// Convert map keys back to a slice.
	result := make([]string, 0, len(combined))
	for item := range combined {
		result = append(result, item)
	}
	return result
}
