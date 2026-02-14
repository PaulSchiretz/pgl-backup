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
	"github.com/paulschiretz/pgl-backup/pkg/hints"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathsyncmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/sharded"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

var ErrDisabled = hints.New("sync is disabled")

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
			New: func() any {
				return new(make([]byte, bufferSize))
			},
		},
		syncTaskPool: &sync.Pool{
			New: func() any {
				return new(syncTask)
			},
		},
		mirrorTaskPool: &sync.Pool{
			New: func() any {
				return new(mirrorTask)
			},
		},
	}
}

// Sync is the main entry point for synchronization. It dispatches to the configured sync engine.
func (s *PathSyncer) Sync(ctx context.Context, absBasePath, absSourcePath string, relCurrentPathKey, relContentPathKey string, p *Plan, timestampUTC time.Time) (metafile.MetafileInfo, error) {

	if !p.Enabled {
		plog.Debug("Sync is disabled, skipping Sync")
		return metafile.MetafileInfo{}, ErrDisabled
	}

	// Check for cancellation after validation but before starting the heavy work.
	select {
	case <-ctx.Done():
		return metafile.MetafileInfo{}, ctx.Err()
	default:
	}

	absTargetCurrentContentPath := util.DenormalizePath(filepath.Join(absBasePath, relCurrentPathKey, relContentPathKey))
	absSyncTargetPath := s.resolveTargetDirectory(absSourcePath, absTargetCurrentContentPath, p.PreserveSourceDirName)

	plog.Info("Syncing files", "source", absSourcePath, "target", absSyncTargetPath)

	// Before dispatching to a specific sync engine, we prepare the destination directory.
	// This centralizes the logic, ensuring that the target directory exists with appropriate
	// permissions, regardless of which engine (native, robocopy) is used.
	srcInfo, err := os.Stat(absSourcePath)
	if err != nil {
		return metafile.MetafileInfo{}, fmt.Errorf("could not stat source directory %s: %w", absSourcePath, err)
	}

	// We use the source directory's permissions as a template for the destination.
	// Crucially, `withBackupWritePermission` is applied to ensure the backup user
	// can always write to the destination on subsequent runs, preventing permission lockouts.
	if !p.DryRun && absSyncTargetPath != "" {
		if err := os.MkdirAll(absSyncTargetPath, util.WithUserWritePermission(srcInfo.Mode().Perm())); err != nil {
			return metafile.MetafileInfo{}, fmt.Errorf("failed to create target directory %s: %w", absSyncTargetPath, err)
		}
	}

	switch p.Engine {
	case Robocopy:
		err := s.runRobocopyTask(ctx, absSourcePath, absSyncTargetPath, p)
		if err != nil {
			return metafile.MetafileInfo{}, err
		}
	case Native:
		err := s.runNativeTask(ctx, absSourcePath, absSyncTargetPath, p)
		if err != nil {
			return metafile.MetafileInfo{}, err
		}
	default:
		return metafile.MetafileInfo{}, fmt.Errorf("unknown sync engine configured: %v", p.Engine)
	}

	// CRITICAL: Here we generate the uuid for the synced backup and write our metafile to the disk
	uuid, err := util.GenerateUUID()
	if err != nil {
		return metafile.MetafileInfo{}, fmt.Errorf("failed to generate UUID: %w", err)
	}

	metadata := metafile.MetafileContent{
		Version:      buildinfo.Version,
		TimestampUTC: timestampUTC,
		Mode:         p.ModeIdentifier,
		UUID:         uuid,
	}
	plog.Notice("SYNCED", "source", absSourcePath, "target", absSyncTargetPath)

	return metafile.MetafileInfo{
		RelPathKey: util.NormalizePath(relCurrentPathKey),
		Metadata:   metadata,
	}, nil
}

// Restore restores a backup to a target directory.
// Unlike Sync, it does not write metadata files to the destination.
func (s *PathSyncer) Restore(ctx context.Context, absBasePath string, relContentPathKey string, toRestore metafile.MetafileInfo, absRestoreTargetPath string, p *Plan) error {

	if !p.Enabled {
		plog.Debug("Sync is disabled, skipping Restore")
		return ErrDisabled
	}

	// Check for cancellation after validation but before starting the heavy work.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Construct the absolute path to the content directory within the backup
	absSourcePath := util.DenormalizePath(filepath.Join(absBasePath, toRestore.RelPathKey, relContentPathKey))
	// For restore, we sync directly into the target path. PreserveSourceDirName is always false.
	absSyncTargetPath := absRestoreTargetPath

	plog.Info("Restoring files", "source", absSourcePath, "target", absSyncTargetPath)

	// Before dispatching to a specific sync engine, we prepare the destination directory.
	// This centralizes the logic, ensuring that the target directory exists with appropriate
	// permissions, regardless of which engine (native, robocopy) is used.
	srcInfo, err := os.Stat(absSourcePath)
	if err != nil {
		return fmt.Errorf("could not stat backup content directory %s: %w", absSourcePath, err)
	}

	// We use the source directory's permissions as a template for the destination.
	// Crucially, `withBackupWritePermission` is applied to ensure the backup user
	// can always write to the destination on subsequent runs, preventing permission lockouts.
	if !p.DryRun && absSyncTargetPath != "" {
		if err := os.MkdirAll(absSyncTargetPath, util.WithUserWritePermission(srcInfo.Mode().Perm())); err != nil {
			return fmt.Errorf("failed to create restore target directory %s: %w", absSyncTargetPath, err)
		}
	}

	switch p.Engine {
	case Robocopy:
		return s.runRobocopyTask(ctx, absSourcePath, absSyncTargetPath, p)
	case Native:
		return s.runNativeTask(ctx, absSourcePath, absSyncTargetPath, p)
	default:
		return fmt.Errorf("unknown sync engine configured: %v", p.Engine)
	}
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

	discoveredPaths, err := sharded.NewShardedSet()
	if err != nil {
		return err
	}
	discoveredDirInfo, err := sharded.NewShardedMap()
	if err != nil {
		return err
	}
	syncedDirCache, err := sharded.NewShardedSet()
	if err != nil {
		return err
	}
	syncErrs, err := sharded.NewShardedMap()
	if err != nil {
		return err
	}
	mirrorErrs, err := sharded.NewShardedMap()
	if err != nil {
		return err
	}
	mirrorDirsToDelete, err := sharded.NewShardedSet()
	if err != nil {
		return err
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

		mirror: p.Mirror,

		discoveredPaths:   discoveredPaths,
		discoveredDirInfo: discoveredDirInfo,
		syncedDirCache:    syncedDirCache,
		// Buffer 'syncTasksChan' and 'mirrorTasksChan' to absorb bursts of small files discovered by the walker.
		// Our syncTask struct is very small (a string and a few int64/bool fields), roughly ~48-64 bytes.
		// Our default config uses 4 workers using a buffer of 4096 items increases memory usage by roughly (4096 * 64 bytes) ≈ 263 KB.
		syncTasksChan:   make(chan *syncTask, s.numSyncWorkers*1024),
		mirrorTasksChan: make(chan *mirrorTask, s.numMirrorWorkers*1024),

		criticalSyncErrsChan:   make(chan error, 1),
		syncErrs:               syncErrs,
		criticalMirrorErrsChan: make(chan error, 1),
		mirrorErrs:             mirrorErrs,
		mirrorDirsToDelete:     mirrorDirsToDelete,
		ctx:                    ctx,
		metrics:                m, // Use the selected metrics implementation.
	}

	s.lastNativeTask = t // Store the run instance for testing.
	return t.execute()
}

// runRobocopyTask initializes the robocopy task structure and kicks off the execution.
func (s *PathSyncer) runRobocopyTask(ctx context.Context, absSourcePath, absSyncTargetPath string, p *Plan) error {
	t := &robocopyTask{
		src:               absSourcePath,
		trg:               absSyncTargetPath,
		retryCount:        p.RetryCount,
		retryWait:         p.RetryWait,
		mirror:            p.Mirror,
		dryRun:            p.DryRun,
		failFast:          p.FailFast,
		fileExcludes:      p.ExcludeFiles,
		dirExcludes:       p.ExcludeDirs,
		ctx:               ctx,
		metrics:           p.Metrics,
		overwriteBehavior: p.OverwriteBehavior,
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
