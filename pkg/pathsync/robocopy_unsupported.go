//go:build !windows

package pathsync

import (
	"context"
	"fmt"
	"runtime"
)

// handleRobocopy is a stub for non-Windows platforms. It returns an error
// as robocopy is not available.
func (s *PathSyncer) handleRobocopy(ctx context.Context, src, dst string, mirror bool, excludeFiles, excludeDirs []string, enableMetrics bool) error {
	return fmt.Errorf("robocopy is not supported on this platform: %s", runtime.GOOS)
}
