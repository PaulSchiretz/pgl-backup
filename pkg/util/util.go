package util

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// WithWritePermission ensures that any directory/file permission has the owner-write
// bit (0200) set. This prevents the backup user from being locked out on subsequent runs.
func WithWritePermission(basePerm os.FileMode) os.FileMode {
	return basePerm | 0200
}

// IsCaseInsensitiveFS checks if the current operating system has a case-insensitive filesystem by default.
func IsCaseInsensitiveFS() bool {
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
