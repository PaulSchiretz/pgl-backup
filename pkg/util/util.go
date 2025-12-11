package util

import (
	"os"
	"runtime"
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
