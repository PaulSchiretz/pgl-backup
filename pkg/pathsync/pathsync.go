package pathsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// PathSyncer orchestrates the file synchronization process.
type PathSyncer struct {
	engine config.BackupEngineConfig
	dryRun bool
	quiet  bool
}

// NewPathSyncer creates a new PathSyncer with the given configuration.
func NewPathSyncer(cfg config.Config) *PathSyncer {
	return &PathSyncer{
		engine: cfg.Engine,
		dryRun: cfg.DryRun,
		quiet:  cfg.Quiet,
	}
}

// Sync is the main entry point for synchronization. It dispatches to the configured sync engine.
func (s *PathSyncer) Sync(ctx context.Context, src, dst string, mirror bool) error {
	if err := s.validateSyncPaths(src, dst); err != nil {
		return err
	}

	// Check for cancellation after validation but before starting the heavy work.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	switch s.engine.Type {
	case config.RobocopyEngine:
		return s.handleRobocopy(ctx, src, dst, mirror)
	case config.NativeEngine:
		plog.Info("Using native Go implementation for synchronization")
		return s.handleNative(ctx, src, dst, mirror)
	default:
		return fmt.Errorf("unknown sync engine configured: %v", s.engine.Type)
	}
}

// validateSyncPaths checks that the source path exists and is a directory,
// and that the destination path can be created and is writable.
func (s *PathSyncer) validateSyncPaths(src, dst string) error {
	// 1. Check if source exists and is a directory.
	if srcInfo, err := os.Stat(src); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("source directory %s does not exist", src)
		}
		return fmt.Errorf("cannot stat source directory %s: %w", src, err)
	} else if !srcInfo.IsDir() {
		return fmt.Errorf("source path %s is not a directory", src)
	}

	if s.dryRun {
		return nil // Skip filesystem modifications in dry run mode.
	}

	// 2. Ensure the destination directory can be created.
	if err := os.MkdirAll(dst, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory %s: %w", dst, err)
	}

	// 3. Optionally, perform a more thorough write check.
	tempFile := filepath.Join(dst, ".pgl-backup-writetest.tmp")
	if f, err := os.Create(tempFile); err != nil {
		return fmt.Errorf("destination directory %s is not writable: %w", dst, err)
	} else {
		f.Close()
		_ = os.Remove(tempFile) // We don't need to handle the error on this cleanup.
	}

	return nil
}
