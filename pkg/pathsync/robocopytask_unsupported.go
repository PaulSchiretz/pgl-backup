//go:build !windows

package pathsync

import (
	"fmt"
	"runtime"
)

// handleRobocopy is a stub for non-Windows platforms. It returns an error
// as robocopy is not available.
func (t *robocopyTask) execute() error {
	return fmt.Errorf("robocopy is not supported on this platform: %s", runtime.GOOS)
}
