package pathsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/buildinfo"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathsyncmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/sharded"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// PathSyncer orchestrates the file synchronization process.
type PathSyncer struct {
	ioBufferPool     *sync.Pool // pointer to avoid copying the noCopy field if the struct is ever passed by value
	syncTaskPool     *sync.Pool
	mirrorTaskPool   *sync.Pool
	numSyncWorkers   int
	numMirrorWorkers int

	// lastNativeTask is for testing purposes only, to inspect the state of the last native sync.
	lastNativeTask *nativeTask
}

// NewPathSyncer creates a new PathSyncer with the given configuration.
func NewPathSyncer(bufferSizeKB int, numSyncWorkers int, numMirrorWorkers int) *PathSyncer {
	bufferSize := bufferSizeKB * 1024 // Buffer size is configured in KB, so multiply by 1024.
	return &PathSyncer{
		numSyncWorkers:   numSyncWorkers,
		numMirrorWorkers: numMirrorWorkers,
		ioBufferPool: &sync.Pool{
			New: func() interface{} {
				b := make([]byte, bufferSize)
				return &b
			},
		},
		syncTaskPool: &sync.Pool{
			New: func() interface{} {
				return new(syncTask)
			},
		},
		mirrorTaskPool: &sync.Pool{
			New: func() interface{} {
				return new(mirrorTask)
			},
		},
	}
}

// Sync is the main entry point for synchronization. It dispatches to the configured sync engine.
func (s *PathSyncer) Sync(ctx context.Context, absSourcePath, absTargetBasePath string, relCurrentPathKey, relContentPathKey string, p *Plan, timestampUTC time.Time) error {

	// Check for cancellation after validation but before starting the heavy work.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	absTargetCurrentContentPath := util.DenormalizePath(filepath.Join(absTargetBasePath, relCurrentPathKey, relContentPathKey))
	absSyncTargetPath := s.resolveTargetDirectory(absSourcePath, absTargetCurrentContentPath, p.PreserveSourceDirName)

	plog.Info("Syncing files", "source", absSourcePath, "target", absSyncTargetPath)

	// Before dispatching to a specific sync engine, we prepare the destination directory.
	// This centralizes the logic, ensuring that the target directory exists with appropriate
	// permissions, regardless of which engine (native, robocopy) is used.
	srcInfo, err := os.Stat(absSourcePath)
	if err != nil {
		return fmt.Errorf("could not stat source directory %s: %w", absSourcePath, err)
	}

	// We use the source directory's permissions as a template for the destination.
	// Crucially, `withBackupWritePermission` is applied to ensure the backup user
	// can always write to the destination on subsequent runs, preventing permission lockouts.
	if !p.DryRun && absSyncTargetPath != "" {
		if err := os.MkdirAll(absSyncTargetPath, util.WithUserWritePermission(srcInfo.Mode().Perm())); err != nil {
			return fmt.Errorf("failed to create target directory %s: %w", absSyncTargetPath, err)
		}
	}

	switch p.Engine {
	case Robocopy:
		err := s.runRobocopyTask(ctx, absSourcePath, absSyncTargetPath, p)
		if err != nil {
			return err
		}
	case Native:
		err := s.runNativeTask(ctx, absSourcePath, absSyncTargetPath, p)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown sync engine configured: %v", p.Engine)
	}

	// If the sync was successful, write the metafile
	absTargetCurrentPath := util.DenormalizePath(filepath.Join(absTargetBasePath, relCurrentPathKey))
	metadata := metafile.MetafileContent{
		Version:      buildinfo.Version,
		TimestampUTC: timestampUTC,
		Mode:         p.ModeIdenifier,
		Source:       absSourcePath,
	}
	if p.DryRun {
		plog.Info("[DRY RUN] Would write metafile", "directory", absTargetCurrentPath)
	} else {
		err := metafile.Write(absTargetCurrentPath, metadata)
		if err != nil {
			return err
		}
	}
	plog.Notice("SYNCED", "source", absSourcePath, "target", absSyncTargetPath)

	// Store the Result
	p.ResultInfo = metafile.MetafileInfo{
		RelPathKey: util.NormalizePath(relCurrentPathKey),
		Metadata:   metadata,
	}
	return nil
}

// runNativeTask initializes the native sync task structure and kicks off the execution.
func (s *PathSyncer) runNativeTask(ctx context.Context, absSourcePath, absSyncTargetPath string, p *Plan) error {
	var m pathsyncmetrics.Metrics
	if p.Metrics {
		m = &pathsyncmetrics.SyncMetrics{}
	} else {
		// Use the No-op implementation if metrics are disabled.
		m = &pathsyncmetrics.NoopMetrics{}
	}

	t := &nativeTask{
		PathSyncer:   s, // Just pass the compressor pointer
		src:          absSourcePath,
		trg:          absSyncTargetPath,
		dryRun:       p.DryRun,
		failFast:     p.FailFast,
		fileExcludes: preProcessExclusions(p.ExcludeFiles),
		dirExcludes:  preProcessExclusions(p.ExcludeDirs),

		retryCount:    p.RetryCount,
		retryWait:     p.RetryWait,
		modTimeWindow: p.ModTimeWindow,

		discoveredPaths:   sharded.NewShardedSet(),
		discoveredDirInfo: sharded.NewShardedMap(),
		syncedDirCache:    sharded.NewShardedSet(),
		// Buffer 'syncTasksChan' to absorb bursts of small files discovered by the walker.
		syncTasksChan:          make(chan *syncTask, s.numSyncWorkers*100),
		mirrorTasksChan:        make(chan *mirrorTask, s.numMirrorWorkers*100),
		criticalSyncErrsChan:   make(chan error, 1),
		syncErrs:               sharded.NewShardedMap(),
		criticalMirrorErrsChan: make(chan error, 1),
		mirrorErrs:             sharded.NewShardedMap(),
		mirrorDirsToDelete:     sharded.NewShardedSet(),
		ctx:                    ctx,
		metrics:                m, // Use the selected metrics implementation.
	}
	s.lastNativeTask = t // Store the run instance for testing.
	return t.execute()
}

// runRobocopyTask initializes the robocopy task structure and kicks off the execution.
func (s *PathSyncer) runRobocopyTask(ctx context.Context, absSourcePath, absSyncTargetPath string, p *Plan) error {
	t := &robocopyTask{
		src:          absSourcePath,
		trg:          absSyncTargetPath,
		retryCount:   p.RetryCount,
		retryWait:    p.RetryWait,
		mirror:       true,
		dryRun:       p.DryRun,
		failFast:     p.FailFast,
		fileExcludes: p.ExcludeFiles,
		dirExcludes:  p.ExcludeDirs,
		ctx:          ctx,
		metrics:      p.Metrics,
	}
	return t.execute()
}

// resolveTargetDirectory determines the final target directory path.
// If preserveSourceDirName is true, it appends the source directory's name to the target base.
func (s *PathSyncer) resolveTargetDirectory(absSourcePath, absTargetPath string, preserveSourceDirName bool) string {
	if !preserveSourceDirName {
		return absTargetPath
	}

	// Append the source's base directory name to the target path.
	var nameToAppend string
	// Check if the path is a root path (e.g., "/" or "C:\")
	if filepath.Dir(absSourcePath) == absSourcePath {
		// Handle Windows Drive Roots (e.g., "D:\") -> "D"
		vol := filepath.VolumeName(absSourcePath)
		// On Windows, for "C:\", VolumeName is "C:", we trim the colon.
		// On Unix, for "/", VolumeName is "", so nameToAppend remains empty.
		if vol != "" && strings.HasSuffix(vol, ":") {
			nameToAppend = strings.TrimSuffix(vol, ":")
		}
	} else {
		// Standard folder
		nameToAppend = filepath.Base(absSourcePath)
	}
	// Append if valid
	if nameToAppend != "" && nameToAppend != "." && nameToAppend != string(filepath.Separator) {
		return filepath.Join(absTargetPath, nameToAppend)
	}
	return absTargetPath
}
