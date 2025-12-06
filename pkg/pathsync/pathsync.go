package pathsync

import (
	"context"
	"fmt"
	"os"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
)

// Syncer defines the interface for a file synchronization implementation.
type Syncer interface {
	Sync(ctx context.Context, src, dst string, mirror bool, excludeFiles, excludeDirs []string) error
}

// PathSyncer orchestrates the file synchronization process.
type PathSyncer struct {
	engine   config.BackupEngineConfig
	dryRun   bool
	quiet    bool
	failFast bool
}

// NewPathSyncer creates a new PathSyncer with the given configuration.
func NewPathSyncer(cfg config.Config) *PathSyncer {
	return &PathSyncer{
		engine:   cfg.Engine,
		dryRun:   cfg.DryRun,
		quiet:    cfg.Quiet,
		failFast: cfg.FailFast,
	}
}

// Sync is the main entry point for synchronization. It dispatches to the configured sync engine.
func (s *PathSyncer) Sync(ctx context.Context, src, dst string, mirror bool, excludeFiles, excludeDirs []string) error {
	// Check for cancellation after validation but before starting the heavy work.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Before dispatching to a specific sync engine, we prepare the destination directory.
	// This centralizes the logic, ensuring that the target directory exists with appropriate
	// permissions, regardless of which engine (native, robocopy) is used.
	srcInfo, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("could not stat source directory %s: %w", src, err)
	}

	// We use the source directory's permissions as a template for the destination.
	// Crucially, `withBackupWritePermission` is applied to ensure the backup user
	// can always write to the destination on subsequent runs, preventing permission lockouts.
	if !s.dryRun {
		if err := os.MkdirAll(dst, withBackupWritePermission(srcInfo.Mode().Perm())); err != nil {
			return fmt.Errorf("failed to create destination directory %s: %w", dst, err)
		}
	}

	switch s.engine.Type {
	case config.RobocopyEngine:
		return s.handleRobocopy(ctx, src, dst, mirror, excludeFiles, excludeDirs)
	case config.NativeEngine:
		return s.handleNative(ctx, src, dst, mirror, excludeFiles, excludeDirs)
	default:
		return fmt.Errorf("unknown sync engine configured: %v", s.engine.Type)
	}
}

// withBackupWritePermission ensures that any directory/file permission has the owner-write
// bit (0200) set. This prevents the backup user from being locked out on subsequent runs.
func withBackupWritePermission(basePerm os.FileMode) os.FileMode {
	// Ensure the backup user always retains write permission.
	return basePerm | 0200
}
