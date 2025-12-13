package pathsync

import (
	"context"
	"fmt"
	"os"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/util"
)

// Syncer defines the interface for a file synchronization implementation.
type Syncer interface {
	Sync(ctx context.Context, src, dst string, mirror bool, excludeFiles, excludeDirs []string, enableMetrics bool) error
}

// PathSyncer orchestrates the file synchronization process.
type PathSyncer struct {
	engine   config.BackupEngineConfig
	dryRun   bool
	failFast bool

	// lastRun is for testing purposes only, to inspect the state of the last native sync.
	lastRun *syncRun
}

// Statically assert that *PathSyncer implements the Syncer interface.
var _ Syncer = (*PathSyncer)(nil)

// NewPathSyncer creates a new PathSyncer with the given configuration.
func NewPathSyncer(cfg config.Config) *PathSyncer {
	return &PathSyncer{
		engine:   cfg.Engine,
		dryRun:   cfg.DryRun,
		failFast: cfg.FailFast,
	}
}

// Sync is the main entry point for synchronization. It dispatches to the configured sync engine.
func (s *PathSyncer) Sync(ctx context.Context, src, dst string, mirror bool, excludeFiles, excludeDirs []string, enableMetrics bool) error {
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
	if !s.dryRun && dst != "" {
		if err := os.MkdirAll(dst, util.WithUserWritePermission(srcInfo.Mode().Perm())); err != nil {
			return fmt.Errorf("failed to create destination directory %s: %w", dst, err)
		}
	}

	switch s.engine.Type {
	case config.RobocopyEngine:
		return s.handleRobocopy(ctx, src, dst, mirror, excludeFiles, excludeDirs, enableMetrics)
	case config.NativeEngine:
		return s.handleNative(ctx, src, dst, mirror, excludeFiles, excludeDirs, enableMetrics)
	default:
		return fmt.Errorf("unknown sync engine configured: %v", s.engine.Type)
	}
}
