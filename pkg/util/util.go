package util

import (
	"cmp"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

// timestampFormat defines the standard, non-configurable time format for backup directory names.
const timestampFormat = "2006-01-02-15-04-05"

// FormatTimestampWithOffset formats a UTC timestamp into a string that includes
// the local timezone offset for user-friendliness, while keeping the base time in UTC.
// Example: 2023-10-27-14-00-00-123456789-0400
func FormatTimestampWithOffset(timestampUTC time.Time) string {
	// We format the UTC time for the timestamp, then format it again in the local
	// timezone just to get the offset string, and combine them.
	mainPartUTC := timestampUTC.Format(timestampFormat)
	nanoPartUTC := fmt.Sprintf("%09d", timestampUTC.Nanosecond())
	offsetPartLocal := timestampUTC.In(time.Local).Format("Z0700")

	return fmt.Sprintf("%s-%s%s", mainPartUTC, nanoPartUTC, offsetPartLocal)
}

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

// NormalizePath converts a path into a standardized key format (forward slashes, maybe otherwise modified key).
// This key is intended for internal logic (like map keys) and not for direct filesystem access.
func NormalizePath(path string) string {
	return filepath.ToSlash(path)
}

// DenormalizePath converts a standardized (forward-slash, maybe otherwise modified key) path key back into a native OS path.
func DenormalizePath(pathKey string) string {
	return filepath.FromSlash(pathKey)
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

// MergeAndDeduplicate combines multiple slices into a single slice,
// removing any duplicate entries.
func MergeAndDeduplicate[T cmp.Ordered](inputs ...[]T) []T {
	// Flatten, sort, and compact to remove duplicates.
	flat := slices.Concat(inputs...)
	if len(flat) == 0 {
		return []T{}
	}
	slices.Sort(flat)
	return slices.Compact(flat)
}

// IsPathCaseSensitive checks if the filesystem at the given path is case-sensitive.
// It does this by creating a temporary file and then attempting to stat a case-variant of it.
func IsPathCaseSensitive(path string) (bool, error) {
	// We need a directory to perform the check in. If the given path is a file, use its parent dir.
	info, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("cannot stat path '%s' to check case sensitivity: %w", path, err)
	}
	checkDir := path
	if !info.IsDir() {
		checkDir = filepath.Dir(path)
	}

	// Create a temporary file with a unique, lowercase name.
	// We use a simple pattern; os.CreateTemp is not ideal here as we need to predict the case-variant name.
	baseName := "pgl-backup-case-test-" + fmt.Sprintf("%d", os.Getpid())
	lowerPath := filepath.Join(checkDir, baseName)
	upperPath := filepath.Join(checkDir, strings.ToUpper(baseName))

	// Clean up any potential leftovers from a previous crashed run.
	_ = os.Remove(lowerPath)
	_ = os.Remove(upperPath)

	// Create the lowercase file.
	if err := os.WriteFile(lowerPath, []byte("test"), 0600); err != nil {
		return false, fmt.Errorf("failed to create temp file for case-sensitivity check in '%s': %w", checkDir, err)
	}
	defer func() {
		_ = os.Remove(lowerPath)
		_ = os.Remove(upperPath) // On case-insensitive FS, this is the same file. On sensitive, it cleans up a potential stray.
	}()

	// Now, try to stat the uppercase version.
	_, err = os.Stat(upperPath)
	if os.IsNotExist(err) {
		// If the uppercase path does not exist, the filesystem is case-sensitive.
		return true, nil
	}
	// If we can stat it (err is nil) or any other error occurs, assume case-insensitive.
	return false, nil
}

// GenerateUUID returns a cryptographically secure Version 4 UUID compliant with RFC 4122.
// It uses crypto/rand for entropy and manually sets the required version and variant bits.
// This implementation is optimized using a byte buffer and hex.Encode to avoid the
// performance overhead of fmt.Sprintf.
func GenerateUUID() (string, error) {
	uuid := make([]byte, 16)
	if _, err := rand.Read(uuid); err != nil {
		return "", err
	}

	// Set version 4 (bits 4-7 of byte 6 to 0100)
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	// Set variant RFC 4122 (bits 6-7 of byte 8 to 10)
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	// A UUID string is 36 characters: 32 hex chars + 4 hyphens
	buf := make([]byte, 36)
	hex.Encode(buf[0:8], uuid[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], uuid[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], uuid[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], uuid[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], uuid[10:])

	return string(buf), nil
}

// ByteCountIEC converts bytes to human-readable string using IEC standard (KiB, MiB, GiB).
func ByteCountIEC(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
