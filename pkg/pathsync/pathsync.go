package pathsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// Syncer defines the interface for a file synchronization implementation.
type Syncer interface {
	Sync(ctx context.Context, source, target string, preserveSourceDirName, mirror bool, excludeFiles, excludeDirs []string, enableMetrics bool) error
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
func (s *PathSyncer) Sync(ctx context.Context, source, target string, preserveSourceDirName, mirror bool, excludeFiles, excludeDirs []string, enableMetrics bool) error {

	target = resolveTargetDirectory(source, target, preserveSourceDirName)

	// Check for cancellation after validation but before starting the heavy work.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	plog.Info("Syncing files", "source", source, "target", target)

	// Before dispatching to a specific sync engine, we prepare the destination directory.
	// This centralizes the logic, ensuring that the target directory exists with appropriate
	// permissions, regardless of which engine (native, robocopy) is used.
	srcInfo, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("could not stat source directory %s: %w", source, err)
	}

	// We use the source directory's permissions as a template for the destination.
	// Crucially, `withBackupWritePermission` is applied to ensure the backup user
	// can always write to the destination on subsequent runs, preventing permission lockouts.
	if !s.dryRun && target != "" {
		if err := os.MkdirAll(target, util.WithUserWritePermission(srcInfo.Mode().Perm())); err != nil {
			return fmt.Errorf("failed to create target directory %s: %w", target, err)
		}
	}

	switch s.engine.Type {
	case config.RobocopyEngine:
		err := s.handleRobocopy(ctx, source, target, mirror, excludeFiles, excludeDirs, enableMetrics)
		if err != nil {
			return err
		}
		plog.Notice("SYNCED", "source", source, "target", target)
		return nil
	case config.NativeEngine:
		err := s.handleNative(ctx, source, target, mirror, excludeFiles, excludeDirs, enableMetrics)
		if err != nil {
			return err
		}
		plog.Notice("SYNCED", "source", source, "target", target)
		return nil
	default:
		return fmt.Errorf("unknown sync engine configured: %v", s.engine.Type)
	}
}

// resolveTargetDirectory determines the final target directory path.
// If preserveSourceDirName is true, it appends the source directory's name to the target base.
func resolveTargetDirectory(source, target string, preserveSourceDirName bool) string {
	if !preserveSourceDirName {
		return target
	}

	// Append the source's base directory name to the target path.
	var nameToAppend string
	// Check if the path is a root path (e.g., "/" or "C:\")
	if filepath.Dir(source) == source {
		// Handle Windows Drive Roots (e.g., "D:\") -> "D"
		vol := filepath.VolumeName(source)
		// On Windows, for "C:\", VolumeName is "C:", we trim the colon.
		// On Unix, for "/", VolumeName is "", so nameToAppend remains empty.
		if vol != "" && strings.HasSuffix(vol, ":") {
			nameToAppend = strings.TrimSuffix(vol, ":")
		}
	} else {
		// Standard folder
		nameToAppend = filepath.Base(source)
	}
	// Append if valid
	if nameToAppend != "" && nameToAppend != "." && nameToAppend != string(filepath.Separator) {
		return filepath.Join(target, nameToAppend)
	}
	return target
}
