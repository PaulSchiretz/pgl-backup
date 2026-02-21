package pathsync

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/buildinfo"
	"github.com/paulschiretz/pgl-backup/pkg/hints"
	"github.com/paulschiretz/pgl-backup/pkg/limiter"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

var ErrDisabled = hints.New("sync is disabled")

// PathSyncer orchestrates the file synchronization process.
type PathSyncer struct {
	ioBufferPool *sync.Pool // pointer to avoid copying the noCopy field if the struct is ever passed by value
	ioBufferSize int64

	readAheadLimiter   *limiter.Memory
	readAheadLimitSize int64

	numSyncWorkers   int
	numMirrorWorkers int

	// lastNativeSyncer is for testing purposes only, to inspect the state of the last native sync.
	lastNativeSyncer *nativeSyncer
}

// NewPathSyncer creates a new PathSyncer with the given configuration.
func NewPathSyncer(bufferSizeKB, readAheadLimitKB int64, numSyncWorkers int, numMirrorWorkers int) *PathSyncer {
	ioBufferSize := bufferSizeKB * 1024 // Buffer size is configured in KB, so multiply by 1024.
	readAheadLimitSize := readAheadLimitKB * 1024
	return &PathSyncer{
		ioBufferPool: &sync.Pool{
			New: func() any {
				// Allocate a slice with a specific capacity
				b := make([]byte, ioBufferSize)
				return &b // We store a pointer to the slice header
			},
		},
		ioBufferSize:       ioBufferSize,
		readAheadLimiter:   limiter.NewMemory(readAheadLimitSize, ioBufferSize),
		readAheadLimitSize: readAheadLimitSize,
		numSyncWorkers:     numSyncWorkers,
		numMirrorWorkers:   numMirrorWorkers,
	}
}

// Sync is the main entry point for synchronization. It dispatches to the configured sync engine.
func (s *PathSyncer) Sync(ctx context.Context, absBasePath, absSourcePath string, relCurrentPathKey, relContentPathKey string, p *Plan, timestampUTC time.Time) (metafile.MetafileInfo, error) {

	if !p.Enabled {
		plog.Debug("Sync is disabled, skipping Sync")
		return metafile.MetafileInfo{}, ErrDisabled
	}

	// Check for cancellation
	select {
	case <-ctx.Done():
		return metafile.MetafileInfo{}, ctx.Err()
	default:
	}

	absTargetCurrentContentPath := util.DenormalizePath(filepath.Join(absBasePath, relCurrentPathKey, relContentPathKey))
	absSyncTargetPath := s.resolveTargetDirectory(absSourcePath, absTargetCurrentContentPath, p.PreserveSourceDirName)

	var m Metrics
	if p.Metrics {
		m = &SyncMetrics{}
	} else {
		m = &NoopMetrics{}
	}

	t := &syncTask{
		PathSyncer:        s,
		ctx:               ctx,
		engine:            p.Engine,
		absSourcePath:     absSourcePath,
		absTargetPath:     absSyncTargetPath,
		mirror:            p.Mirror,
		dryRun:            p.DryRun,
		failFast:          p.FailFast,
		safeCopy:          p.SafeCopy,
		sequentialWrite:   p.SequentialWrite,
		fileExclusions:    newExclusionSet(p.ExcludeFiles),
		dirExclusions:     newExclusionSet(p.ExcludeDirs),
		retryCount:        p.RetryCount,
		retryWait:         p.RetryWait,
		modTimeWindow:     p.ModTimeWindow,
		overwriteBehavior: p.OverwriteBehavior,
		timestampUTC:      timestampUTC,
		metrics:           m,
	}

	err := t.execute()
	if err != nil {
		return metafile.MetafileInfo{}, err
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
func (s *PathSyncer) Restore(ctx context.Context, absBasePath string, relContentPathKey string, toRestore metafile.MetafileInfo, absRestoreTargetPath string, p *Plan, timestampUTC time.Time) error {

	if !p.Enabled {
		plog.Debug("Sync is disabled, skipping Restore")
		return ErrDisabled
	}

	// Check for cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Construct the absolute path to the content directory within the backup
	absSourcePath := util.DenormalizePath(filepath.Join(absBasePath, toRestore.RelPathKey, relContentPathKey))

	var m Metrics
	if p.Metrics {
		m = &SyncMetrics{}
	} else {
		m = &NoopMetrics{}
	}

	t := &syncTask{
		PathSyncer:        s,
		ctx:               ctx,
		engine:            p.Engine,
		absSourcePath:     absSourcePath,
		absTargetPath:     absRestoreTargetPath,
		mirror:            p.Mirror,
		dryRun:            p.DryRun,
		failFast:          p.FailFast,
		safeCopy:          p.SafeCopy,
		sequentialWrite:   p.SequentialWrite,
		fileExclusions:    newExclusionSet(p.ExcludeFiles),
		dirExclusions:     newExclusionSet(p.ExcludeDirs),
		retryCount:        p.RetryCount,
		retryWait:         p.RetryWait,
		modTimeWindow:     p.ModTimeWindow,
		overwriteBehavior: p.OverwriteBehavior,
		timestampUTC:      timestampUTC,
		metrics:           m,
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
