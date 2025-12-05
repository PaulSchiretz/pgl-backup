package pathsync

import (
	"context"
	"fmt"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
)

// Syncer defines the interface for a file synchronization implementation.
type Syncer interface {
	Sync(ctx context.Context, src, dst string, preserveSourceDirName, mirror bool, excludeFiles, excludeDirs []string) error
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
func (s *PathSyncer) Sync(ctx context.Context, src, dst string, preserveSourceDirName, mirror bool, excludeFiles, excludeDirs []string) error {
	// Check for cancellation after validation but before starting the heavy work.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	switch s.engine.Type {
	case config.RobocopyEngine:
		return s.handleRobocopy(ctx, src, dst, preserveSourceDirName, mirror, excludeFiles, excludeDirs)
	case config.NativeEngine:
		return s.handleNative(ctx, src, dst, preserveSourceDirName, mirror, excludeFiles, excludeDirs)
	default:
		return fmt.Errorf("unknown sync engine configured: %v", s.engine.Type)
	}
}
