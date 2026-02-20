package pathsync

// --- ARCHITECTURAL OVERVIEW ---
// The native sync engine uses a two-phase approach to ensure both speed and correctness.
//
// --- Phase 1: Concurrent Sync (Producer-Consumer) ---
//
// The core of the engine uses a Producer-Consumer pattern to perform file
// operations concurrently, maximizing I/O throughput. This pipeline is
// orchestrated by the `handleSync` function.
//
// 1. The Producer (`syncItemProducer`):
//    - A single goroutine that walks the source directory tree (`filepath.WalkDir`).
//    - It records the presence of every file and directory in the `existingRelSourcePaths` set.
//    - It creates a `syncItem` for every file and directory and sends it to the `syncItems` channel for workers.
//
// 2. The Consumers (`syncWorker` pool):
//    - A pool of worker goroutines that read `syncItem` items from the `syncItems` channel.
//    - Each worker performs the I/O for a single item (creating directory, checking/copying file).
//    - The `syncItemProducer` has already recorded the item's presence in the `existingRelSourcePaths` set.
//
// --- Phase 2: Mirroring (Deletions) ---
//
// 3. The Mirror Phase (`handleMirror`):
//    - If mirroring is enabled, this final pass walks the *destination* directory.
//    - For each item, it checks for its presence in the `existingRelSourcePaths` set.
//    - Any destination item not found in the set (and not otherwise excluded) is deleted.
//
// A key design principle is ensuring the backup process does not lock itself out.
// To achieve this, all directories created in the destination will have
// the owner-write permission bit (0200) set, guaranteeing that the user running
// the backup can always write to them in subsequent runs. This prevents failures
// when backing up source directories with read-only permissions.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/sharded"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// syncItem holds all the necessary metadata for a worker to process a file
// without re-calculating paths or re-fetching filesystem stats.
type syncItem struct {
	RelPathKey string // Normalized, forward-slash and maybe otherwise modified key. NOT for direct FS access.
	PathInfo   lstatInfo
}

// mirrorItem holds the necessary metadata for a worker to process a deletion.
type mirrorItem struct {
	// Normalized, forward-slash and maybe otherwise modified key. NOT for direct FS access.
	RelPathKey string
}

type nativeSyncer struct {

	// Read buffer pool
	ioBufferPool *sync.Pool
	ioBufferSize int64

	// ctx is the cancellable context for the entire run.
	ctx context.Context

	src, trg         string
	numSyncWorkers   int
	numMirrorWorkers int

	mirror         bool
	dryRun         bool
	failFast       bool
	safeCopy       bool
	fileExclusions exclusionSet
	dirExclusions  exclusionSet

	retryCount        int
	retryWait         time.Duration
	modTimeWindow     time.Duration
	overwriteBehavior OverwriteBehavior

	// metrics holds the counters for the sync operation.
	metrics Metrics

	// existingRelSourcePaths is a concurrent set populated by the syncItemProducer. It holds every
	// non-excluded relative path found in the source directory. During the mirror phase, it is
	// read to determine which paths in the destination are no longer present in the source
	// and should be deleted.
	existingRelSourcePaths *sharded.ShardedSet

	// syncedDirCache tracks directories that have ALREADY been created in the destination
	// by any syncWorker. This prevents duplicate MkdirAll calls across the concurrent pool.
	syncedDirCache *sharded.ShardedSet

	// Synchronizes directory creation/conversion
	// syncedDirSFGroup is used to prevent a "thundering herd" problem when multiple
	// workers concurrently try to create the same parent directory. It ensures that
	// for any given directory path, only the first worker performs the I/O (MkdirAll),
	// while other workers for the same path wait for the result. This deduplicates
	// work and prevents race conditions.
	syncedDirSFGroup singleflight.Group

	// Optimization: syncSourceDirInfoCache is a concurrent map populated by the syncItemProducer. It stores the
	// PathInfo for every directory found in the source. This serves as a cache to avoid
	// redundant Lstat calls by workers needing to create parent directories.
	syncSourceDirInfoCache *sharded.ShardedMap

	// Optimization: syncTargetLStatCache is a lazy-loaded cache of destination directory lstat entries.
	// It is populated on-demand by the first worker to enter a directory via os.ReadDir.
	// This allows subsequent workers to verify file existence, size, and modtime entirely in RAM,
	// eliminating thousands of redundant Lstat system calls on the target filesystem.
	syncTargetLStatCache *LStatSnapshotStore

	// syncWg waits for the syncItemProducer and syncWorkers to finish processing all sync items.
	syncWg sync.WaitGroup

	// syncItemsChan is the channel where the Walker sends pre-processed items.
	syncItemsChan chan *syncItem

	// criticalSyncErrsChan captures the first critical, unrecoverable error (e.g., walker failure)
	// to enable a "fail-fast" exit from the sync phase.
	criticalSyncErrsChan chan error

	// syncErrs is a concurrent map that captures non-fatal I/O errors from any worker,
	// keyed by the relative path of the file that failed.
	syncErrs *sharded.ShardedMap

	// mirrorWg waits for the mirrorItemProducer and mirrorWorkers to finish processing all deletion items.
	mirrorWg sync.WaitGroup

	// mirrorItemsChan is the channel where the mirrorItemProducer sends paths to be deleted.
	mirrorItemsChan chan *mirrorItem

	// criticalMirrorErrsChan captures the first critical error from the mirror phase.
	criticalMirrorErrsChan chan error

	// mirrorErrs captures non-fatal I/O errors from mirror workers.
	mirrorErrs *sharded.ShardedMap

	// mirrorDirsToDelete tracks directories in the destination that need to be deleted.
	// Populated by mirrorItemProducer, consumed by handleMirror.
	mirrorDirsToDelete *sharded.ShardedSet

	// Pool for syncItem structs to reduce GC pressure
	syncItemPool *sync.Pool

	// Pool for mirrorItem structs to reduce GC pressure
	mirrorItemPool *sync.Pool
}

func newNativeSyncer(
	mirror, dryRun, failFast, safeCopy bool,
	fileExclusions, dirExclusions exclusionSet,
	retryCount int,
	retryWait, modTimeWindow time.Duration,
	overwriteBehavior OverwriteBehavior,
	ioBufferPool *sync.Pool, ioBufferSize int64,
	numSyncWorkers, numMirrorWorkers int,
	metrics Metrics,
) (*nativeSyncer, error) {
	existingRelSourcePaths, err := sharded.NewShardedSet()
	if err != nil {
		return nil, err
	}
	syncSourceDirInfoCache, err := sharded.NewShardedMap()
	if err != nil {
		return nil, err
	}
	syncedDirCache, err := sharded.NewShardedSet()
	if err != nil {
		return nil, err
	}
	syncErrs, err := sharded.NewShardedMap()
	if err != nil {
		return nil, err
	}
	mirrorErrs, err := sharded.NewShardedMap()
	if err != nil {
		return nil, err
	}
	mirrorDirsToDelete, err := sharded.NewShardedSet()
	if err != nil {
		return nil, err
	}

	return &nativeSyncer{
		numSyncWorkers:         numSyncWorkers,
		numMirrorWorkers:       numMirrorWorkers,
		metrics:                metrics,
		dryRun:                 dryRun,
		failFast:               failFast,
		safeCopy:               safeCopy,
		fileExclusions:         fileExclusions,
		dirExclusions:          dirExclusions,
		retryCount:             retryCount,
		retryWait:              retryWait,
		modTimeWindow:          modTimeWindow,
		overwriteBehavior:      overwriteBehavior,
		mirror:                 mirror,
		existingRelSourcePaths: existingRelSourcePaths,
		syncedDirCache:         syncedDirCache,
		syncSourceDirInfoCache: syncSourceDirInfoCache,
		syncTargetLStatCache:   newLStatSnapshotStore(),
		syncItemsChan:          make(chan *syncItem, numSyncWorkers*1024),
		criticalSyncErrsChan:   make(chan error, 1),
		syncErrs:               syncErrs,
		mirrorItemsChan:        make(chan *mirrorItem, numMirrorWorkers*1024),
		criticalMirrorErrsChan: make(chan error, 1),
		mirrorErrs:             mirrorErrs,
		mirrorDirsToDelete:     mirrorDirsToDelete,
		ioBufferPool:           ioBufferPool,
		ioBufferSize:           ioBufferSize,
		syncItemPool: &sync.Pool{
			New: func() any {
				return new(syncItem)
			},
		},
		mirrorItemPool: &sync.Pool{
			New: func() any {
				return new(mirrorItem)
			},
		},
	}, nil
}

// --- Helpers ---

// isExcluded checks if a given relative path key matches any of the exclusion patterns,
// using a tiered optimization strategy to avoid expensive glob matching when possible.
// It handles normalization (case-folding and path separators) internally.
func (s *nativeSyncer) isExcluded(relPathKey, relPathBasename string, isDir bool) bool {
	if isDir {
		return s.dirExclusions.matches(relPathKey, relPathBasename)
	}
	return s.fileExclusions.matches(relPathKey, relPathBasename)
}

// copyFileSafe handles the low-level details of copying a single file.
// It ensures atomicity by writing to a temporary file first and then renaming it.
func (s *nativeSyncer) copyFileSafe(absSrcPath, absTrgPath string, item *syncItem, retryCount int, retryWait time.Duration) error {
	var lastErr error
	for i := range retryCount + 1 {
		if i > 0 {
			plog.Warn("Retrying file copy", "file", absSrcPath, "attempt", fmt.Sprintf("%d/%d", i, retryCount), "after", retryWait)
			time.Sleep(retryWait)
		}

		lastErr = func() (err error) {
			// 1. Open source file.
			in, err := os.Open(absSrcPath)
			if err != nil {
				return fmt.Errorf("failed to open source file %s: %w", absSrcPath, err)
			}
			defer in.Close()

			absTrgDir := filepath.Dir(absTrgPath)

			// 2. Create a temporary file in the destination directory.
			out, err := os.CreateTemp(absTrgDir, "pgl-backup-*.tmp")
			if err != nil {
				return fmt.Errorf("failed to create temporary file in %s: %w", absTrgDir, err)
			}
			defer out.Close() // Ensure closed on error.

			absTempPath := out.Name()
			// Defer the removal of the temp file. If the rename succeeds, tempPath will be set to "",
			// making this a no-op. This prevents an error trying to remove a non-existent file.
			defer func() {
				if absTempPath != "" {
					os.Remove(absTempPath)
				}
			}()

			// Pre-allocate file size to reduce fragmentation.
			if item.PathInfo.Size > 0 {
				_ = out.Truncate(item.PathInfo.Size)
			}

			// 3. Copy content
			var bytesWritten int64
			// Get a buffer from the pool for the copy operation.
			bufPtr := s.ioBufferPool.Get().(*[]byte)
			defer s.ioBufferPool.Put(bufPtr)
			// Dereference the bufPtr to get the actual slice
			buf := *bufPtr
			// IMPORTANT: Ensure length is maxed out before use
			// In case someone messed with it, always reset len to cap
			// strictly for io.Read/Copy purposes.
			buf = buf[:cap(buf)]

			if bytesWritten, err = io.CopyBuffer(out, in, buf); err != nil {
				return fmt.Errorf("failed to copy content from %s to %s: %w", absSrcPath, absTempPath, err)
			}

			// update metrics
			s.metrics.AddBytesWritten(int64(bytesWritten))

			// 4. Copy file permissions from the source.
			// CRITICAL: We must ensure the user always has write permission on the destination file
			// to prevent being locked out on subsequent runs (e.g., if the source was read-only).
			if err := out.Chmod(util.WithUserWritePermission(item.PathInfo.Mode)); err != nil {
				return fmt.Errorf("failed to set permissions on temporary file %s: %w", absTempPath, err)
			}

			// 5. Close the file.
			// This flushes data to disk. It MUST be done before Chtimes,
			// because closing/flushing might update the modification time.
			if err := out.Close(); err != nil {
				return fmt.Errorf("failed to close temporary file %s: %w", absTempPath, err)
			}

			// 6. Copy file timestamps
			// We do this via os.Chtimes (using the path) after the file is closed.
			if err := os.Chtimes(absTempPath, time.Unix(0, item.PathInfo.ModTime), time.Unix(0, item.PathInfo.ModTime)); err != nil {
				return fmt.Errorf("failed to set timestamps on %s: %w", absTempPath, err)
			}

			// 7. Atomically move the temporary file to the final destination.
			// os.Rename is atomic on POSIX and uses MoveFileEx with MOVEFILE_REPLACE_EXISTING on Windows (since Go 1.5).
			if err := os.Rename(absTempPath, absTrgPath); err != nil {
				return err
			}

			// 8. Clear tempPath to prevent the deferred os.Remove from running.
			absTempPath = ""
			return nil
		}()

		if lastErr == nil {
			return nil // Success
		}
	}
	return fmt.Errorf("failed to copy file from '%s' to '%s' after %d attempts: %w", absSrcPath, absTrgPath, retryCount, lastErr)
}

// copyFileDirect copies a file directly to the destination, avoiding temporary files and renames.
// This is faster (fewer metadata ops) but less safe against interruptions.
func (s *nativeSyncer) copyFileDirect(absSrcPath, absTrgPath string, item *syncItem, retryCount int, retryWait time.Duration) error {
	var lastErr error
	for i := range retryCount + 1 {
		if i > 0 {
			plog.Warn("Retrying file copy", "file", absSrcPath, "attempt", fmt.Sprintf("%d/%d", i, retryCount), "after", retryWait)
			time.Sleep(retryWait)
		}

		lastErr = func() error {
			in, err := os.Open(absSrcPath)
			if err != nil {
				return fmt.Errorf("failed to open source file %s: %w", absSrcPath, err)
			}
			defer in.Close()

			// Open destination file directly. os.O_TRUNC will clear the file if it exists.
			// The permissions from the source file are used, with the user-write bit always set.
			out, err := os.OpenFile(absTrgPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, util.WithUserWritePermission(item.PathInfo.Mode))
			if err != nil {
				return fmt.Errorf("failed to open destination file %s: %w", absTrgPath, err)
			}
			defer out.Close() // Ensure closed on error.

			// Explicitly set permissions to ensure they match source even if file existed.
			if err := out.Chmod(util.WithUserWritePermission(item.PathInfo.Mode)); err != nil {
				return fmt.Errorf("failed to set permissions on destination file %s: %w", absTrgPath, err)
			}

			// Pre-allocate file size to reduce fragmentation.
			if item.PathInfo.Size > 0 {
				_ = out.Truncate(item.PathInfo.Size)
			}

			var bytesWritten int64
			bufPtr := s.ioBufferPool.Get().(*[]byte)
			defer s.ioBufferPool.Put(bufPtr)
			buf := *bufPtr
			buf = buf[:cap(buf)]

			if bytesWritten, err = io.CopyBuffer(out, in, buf); err != nil {
				return fmt.Errorf("failed to copy content from %s to %s: %w", absSrcPath, absTrgPath, err)
			}

			s.metrics.AddBytesWritten(int64(bytesWritten))

			// Close the file to flush data to disk before setting timestamps.
			if err := out.Close(); err != nil {
				return fmt.Errorf("failed to close destination file %s: %w", absTrgPath, err)
			}

			if err := os.Chtimes(absTrgPath, time.Unix(0, item.PathInfo.ModTime), time.Unix(0, item.PathInfo.ModTime)); err != nil {
				return fmt.Errorf("failed to set timestamps on %s: %w", absTrgPath, err)
			}
			return nil
		}()

		if lastErr == nil {
			return nil // Success
		}
	}
	return fmt.Errorf("failed to copy file from '%s' to '%s' after %d attempts: %w", absSrcPath, absTrgPath, retryCount, lastErr)
}

// copySymlinkHelper handles the low-level details of creating a symlink.
// It ensures atomicity by creating a temporary link first and then renaming it.
func (s *nativeSyncer) copySymlinkHelper(target, absTrgPath string, retryCount int, retryWait time.Duration) error {
	var lastErr error
	for i := range retryCount + 1 {
		if i > 0 {
			plog.Warn("Retrying symlink creation", "file", absTrgPath, "attempt", fmt.Sprintf("%d/%d", i, retryCount), "after", retryWait)
			time.Sleep(retryWait)
		}

		lastErr = func() error {
			absTrgDir := filepath.Dir(absTrgPath)

			// Generate a temp name.
			f, err := os.CreateTemp(absTrgDir, "pgl-backup-symlink-*.tmp")
			if err != nil {
				return fmt.Errorf("failed to generate temp name for symlink: %w", err)
			}
			tempName := f.Name()
			f.Close()
			// os.CreateTemp creates a regular file. We only need the unique name.
			// We must remove the file so os.Symlink can create the link in its place.
			os.Remove(tempName)

			// Defer the removal of the temp file.
			defer func() {
				if tempName != "" {
					os.Remove(tempName)
				}
			}()

			if err := os.Symlink(target, tempName); err != nil {
				if runtime.GOOS == "windows" && strings.Contains(err.Error(), "privilege") {
					return fmt.Errorf("failed to create symlink (requires Admin or Developer Mode): %w", err)
				}
				return fmt.Errorf("failed to create symlink %s -> %s: %w", tempName, target, err)
			}

			if err := os.Rename(tempName, absTrgPath); err != nil {
				return fmt.Errorf("failed to rename temp symlink to %s: %w", absTrgPath, err)
			}

			tempName = "" // Prevent deferred removal
			return nil
		}()

		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("failed to create symlink at '%s' after %d attempts: %w", absTrgPath, retryCount, lastErr)
}

// processFileSync checks if a file needs to be copied (based on size/time)
// and triggers the copy operation if needed.
func (s *nativeSyncer) processFileSync(item *syncItem) error {
	if s.dryRun {
		plog.Notice("[DRY RUN] COPY", "path", item.RelPathKey)
		return nil
	}

	useSafeCopy := s.safeCopy

	// Convert the paths to the OS-native format for file access
	absSrcPath := util.DenormalizedAbsPath(s.src, item.RelPathKey)
	absTrgPath := util.DenormalizedAbsPath(s.trg, item.RelPathKey)

	// Read entry from cache
	var trgLstatInfo lstatInfo
	var err error
	info, found, parentCached := s.syncTargetLStatCache.LoadLstatInfo(item.RelPathKey)
	if parentCached {
		if found {
			trgLstatInfo = info
		} else {
			// ok the items parent was cached but the item doesn't exist.
			// WE are sure it doesn't exist in the filesystem, no need for an additional Lstat call!
			err = os.ErrNotExist
		}
	} else {
		// Fallback for paths without a parent or unexpected cache misses
		var fsInfo os.FileInfo
		if fsInfo, err = os.Lstat(absTrgPath); err == nil {
			trgLstatInfo = lstatInfo{
				Size:      fsInfo.Size(),
				ModTime:   fsInfo.ModTime().UnixNano(),
				Mode:      fsInfo.Mode(),
				IsDir:     fsInfo.IsDir(),
				IsRegular: fsInfo.Mode().IsRegular(),
				IsSymlink: fsInfo.Mode()&os.ModeSymlink != 0,
			}
		}
	}

	// Check if the destination file exists and if it matches source (size and mod time).
	if err == nil {
		// Destination path exists.
		if trgLstatInfo.IsRegular {
			// It's a regular file. Use the info from os.Lstat directly for comparison.
			// We skip the copy only if the modification times (within the configured window) and sizes are identical.
			// We truncate the times to handle filesystems with different timestamp resolutions.
			// NOTE: Permissions are intentionally not compared. This prevents unnecessary file copies
			// when only metadata (like the executable bit) changes, which is common with version
			// control systems like Git. The backup process already ensures the destination is writable.
			switch s.overwriteBehavior {
			case OverwriteNever:
				s.metrics.AddFilesUpToDate(1)
				return nil
			case OverwriteIfNewer:
				srcTime := time.Unix(0, item.PathInfo.ModTime)
				trgTime := time.Unix(0, trgLstatInfo.ModTime)

				// adjust times based on the configured modification time window.
				if s.modTimeWindow > 0 {
					srcTime = srcTime.Truncate(s.modTimeWindow)
					trgTime = trgTime.Truncate(s.modTimeWindow)
				}

				if !srcTime.After(trgTime) {
					s.metrics.AddFilesUpToDate(1)
					return nil // File is Newer
				}
			case OverwriteAlways:
				// Fall through to copy
			default: // OverwriteUpdate
				srcTime := time.Unix(0, item.PathInfo.ModTime)
				trgTime := time.Unix(0, trgLstatInfo.ModTime)

				// adjust times based on the configured modification time window.
				if s.modTimeWindow > 0 {
					srcTime = srcTime.Truncate(s.modTimeWindow)
					trgTime = trgTime.Truncate(s.modTimeWindow)
				}

				if srcTime.Equal(trgTime) && item.PathInfo.Size == trgLstatInfo.Size {
					s.metrics.AddFilesUpToDate(1)
					return nil // Not changed
				}
			}
		} else if trgLstatInfo.IsDir {
			// Destination is a directory. Rename cannot overwrite it, so we must remove it explicitly.
			plog.Warn("Destination is a directory, removing before copy", "path", item.RelPathKey)
			if err := os.RemoveAll(absTrgPath); err != nil {
				return fmt.Errorf("failed to remove directory at destination %s: %w", absTrgPath, err)
			}
			useSafeCopy = true
		} else {
			plog.Warn("Destination is not a regular file, removing before copy", "path", item.RelPathKey, "type", trgLstatInfo.Mode.String())
			if err := os.RemoveAll(absTrgPath); err != nil {
				return fmt.Errorf("failed to remove existing destination file %s: %w", absTrgPath, err)
			}
			useSafeCopy = true
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to retrieve destination info for %s: %w", absTrgPath, err)
	}

	if useSafeCopy {
		// Use Safe Copy for security.
		if err := s.copyFileSafe(absSrcPath, absTrgPath, item, s.retryCount, s.retryWait); err != nil {
			return fmt.Errorf("failed to copy file to %s: %w", absTrgPath, err)
		}
	} else {
		// Use Direct Copy for performance.
		if err := s.copyFileDirect(absSrcPath, absTrgPath, item, s.retryCount, s.retryWait); err != nil {
			return fmt.Errorf("failed to copy file to %s: %w", absTrgPath, err)
		}
	}

	plog.Notice("COPY", "path", item.RelPathKey)
	s.metrics.AddFilesCopied(1)
	return nil // File was actually copied/updated
}

// processSymlinkSync handles the creation or update of a symlink in the destination.
func (s *nativeSyncer) processSymlinkSync(item *syncItem) error {
	if s.dryRun {
		plog.Notice("[DRY RUN] SYMLINK", "path", item.RelPathKey)
		return nil
	}

	absSrcPath := util.DenormalizedAbsPath(s.src, item.RelPathKey)
	absTrgPath := util.DenormalizedAbsPath(s.trg, item.RelPathKey)
	var err error

	// Read the link target from the source.
	linkTarget, err := os.Readlink(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to read source symlink %s: %w", absSrcPath, err)
	}

	// Read entry from cache
	var trgLstatInfo lstatInfo
	info, found, parentCached := s.syncTargetLStatCache.LoadLstatInfo(item.RelPathKey)
	if parentCached {
		if found {
			trgLstatInfo = info
		} else {
			// ok the items parent was cached but the item doesn't exist.
			// WE are sure it doesn't exist in the filesystem, no need for an additional Lstat call!
			err = os.ErrNotExist
		}
	} else {
		// Fallback for paths without a parent or unexpected cache misses
		var fsInfo os.FileInfo
		if fsInfo, err = os.Lstat(absTrgPath); err == nil {
			trgLstatInfo = lstatInfo{
				Size:      fsInfo.Size(),
				ModTime:   fsInfo.ModTime().UnixNano(),
				Mode:      fsInfo.Mode(),
				IsDir:     fsInfo.IsDir(),
				IsRegular: fsInfo.Mode().IsRegular(),
				IsSymlink: fsInfo.Mode()&os.ModeSymlink != 0,
			}
		}
	}

	if err == nil {
		// Destination exists.
		if trgLstatInfo.IsSymlink {
			// It's a symlink. Check if targets match.
			dstLinkTarget, err := os.Readlink(absTrgPath)
			if err == nil && dstLinkTarget == linkTarget {
				s.metrics.AddFilesUpToDate(1)
				return nil // Up to date
			}
		} else if trgLstatInfo.IsDir {
			// Destination is a directory. Rename cannot overwrite it, so we must remove it explicitly.
			plog.Warn("Destination is a directory, removing before symlink creation", "path", item.RelPathKey)
			if err := os.RemoveAll(absTrgPath); err != nil {
				return fmt.Errorf("failed to remove existing destination directory %s: %w", absTrgPath, err)
			}
		} else {
			// Destination is a regular file or other type. Remove it to ensure clean state.
			plog.Warn("Destination is not a symlink, removing before symlink creation", "path", item.RelPathKey, "type", trgLstatInfo.Mode.String())
			if err := os.RemoveAll(absTrgPath); err != nil {
				return fmt.Errorf("failed to remove existing destination file %s: %w", absTrgPath, err)
			}
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to retrieve destination metadata for %s: %w", absTrgPath, err)
	}

	// Create the symlink atomically using a temporary name and rename.
	if err := s.copySymlinkHelper(linkTarget, absTrgPath, s.retryCount, s.retryWait); err != nil {
		return err
	}

	plog.Notice("SYMLINK", "path", item.RelPathKey, "linkTarget", linkTarget)
	s.metrics.AddFilesCopied(1)
	return nil
}

// processDirectorySync handles the creation and permission setting for a directory in the destination.
// It returns an error (specifically filepath.SkipDir) if the directory cannot be created.
func (s *nativeSyncer) processDirectorySync(item *syncItem) error {

	// 1. FAST PATH: Check the cache first.
	if s.syncedDirCache.Has(item.RelPathKey) {
		return nil // Already created.
	}

	// 2. Use singleflight to deduplicate concurrent requests for the same path.
	// In a highly concurrent scenario, many workers might try to create the same
	// parent directory simultaneously (e.g., for 1000 files in the same new folder).
	// The singleflight group ensures that only the *first* worker to request creation
	// for a specific path will execute the MkdirAll logic. All other workers for that
	// same path will block and wait for the first one to finish, receiving its result.
	// This prevents redundant I/O and potential race conditions.
	//
	// CRITICAL: This is especially critical when a file or symlink with the same name exists
	// at the target path. We must remove it before creating the directory. Without
	// singleflight, multiple workers would race to remove the same file, leading to errors.
	_, err, _ := s.syncedDirSFGroup.Do(item.RelPathKey, func() (any, error) {
		// Double-check cache now that we are the "chosen" worker for this path
		if s.syncedDirCache.Has(item.RelPathKey) {
			return nil, nil
		}

		if s.dryRun {
			// Atomically update cache to ensure we only log once per directory.
			if loaded := s.syncedDirCache.LoadOrStore(item.RelPathKey); !loaded {
				plog.Notice("[DRY RUN] DIR", "path", item.RelPathKey)
			}
			return nil, nil
		}

		// Convert the path to the OS-native format for file access
		absTrgPath := util.DenormalizedAbsPath(s.trg, item.RelPathKey)
		expectedPerms := util.WithUserWritePermission(item.PathInfo.Mode.Perm())

		// 3. Perform the concurrent I/O.
		// Check if the destination exists and handle type conflicts (e.g. File vs Dir).
		var dirCreated bool = false
		info, err := os.Lstat(absTrgPath)
		if err == nil {
			// Path exists.
			if !info.IsDir() {
				// It exists but is not a directory (e.g. it's a file or symlink).
				// We must remove it to create the directory.
				plog.Warn("Destination path exists but is not a directory, removing", "path", item.RelPathKey, "type", info.Mode().String())
				if err := os.RemoveAll(absTrgPath); err != nil {
					return nil, fmt.Errorf("failed to remove conflicting destination file %s: %w", item.RelPathKey, err)
				}
				// Create the directory.
				if err := os.MkdirAll(absTrgPath, expectedPerms); err != nil {
					plog.Warn("Failed to create destination directory, skipping", "path", item.RelPathKey, "error", err)
					return nil, filepath.SkipDir
				}
				dirCreated = true
			} else {
				// It is already a directory. Ensure permissions are correct.
				if err := os.Chmod(absTrgPath, expectedPerms); err != nil {
					plog.Warn("Failed to set permissions on destination directory", "path", item.RelPathKey, "error", err)
					return nil, fmt.Errorf("failed to set permissions on destination directory %s: %w", item.RelPathKey, err)
				}
			}
		} else if os.IsNotExist(err) {
			// Path does not exist. Create it.
			if err := os.MkdirAll(absTrgPath, expectedPerms); err != nil {
				plog.Warn("Failed to create destination directory, skipping", "path", item.RelPathKey, "error", err)
				return nil, filepath.SkipDir
			}
			dirCreated = true
		} else {
			// Unexpected error from Lstat.
			return nil, fmt.Errorf("failed to lstat destination directory %s: %w", item.RelPathKey, err)
		}

		// 4. Optimization: Cache the lstat entries of the target dir to avoid redundant lookups
		trgPathEntries, err := os.ReadDir(absTrgPath)
		if err == nil {
			// We build a standard Go map for the specific directory, and store it as an immutable snapshot
			snapshot := make(map[string]lstatInfo, len(trgPathEntries))
			for _, d := range trgPathEntries {
				if info, err := d.Info(); err == nil {
					snapshot[d.Name()] = lstatInfo{
						Size:      info.Size(),
						ModTime:   info.ModTime().UnixNano(),
						Mode:      info.Mode(),
						IsDir:     info.IsDir(),
						IsRegular: info.Mode().IsRegular(),
						IsSymlink: info.Mode()&os.ModeSymlink != 0,
					}
				}
			}
			s.syncTargetLStatCache.StoreSnapshot(item.RelPathKey, snapshot)
		}

		// 5. Finalize state
		// CRITICAL: Always store in cache if the directory exists and is valid (whether we created it or it was already there).
		// This prevents redundant Lstat/Chmod calls for every file in this directory by subsequent workers.
		s.syncedDirCache.Store(item.RelPathKey)
		if dirCreated {
			s.metrics.AddDirsCreated(1)
			plog.Notice("DIR", "path", item.RelPathKey)
		}
		return nil, nil
	})
	return err
}

// ensureParentDirectoryExists is called by file-processing workers to guarantee that the
// parent directory for a file exists before the file copy is attempted.
func (s *nativeSyncer) ensureParentDirectoryExists(relPathKey string) error {
	// 1. FAST PATH: Check the cache first.
	if s.syncedDirCache.Has(relPathKey) {
		return nil // Already created.
	}

	// 2. If not in cache, we need to create it now.
	// Instead of re-statting the source directory, we look up its info from the cache
	// populated by the syncItemProducer.
	val, ok := s.syncSourceDirInfoCache.Load(relPathKey)
	if !ok {
		// This should be logically impossible if the walker has processed the parent
		// directory before its child file. We return an error to be safe.
		return fmt.Errorf("internal logic error: PathInfo for parent directory %s not found in cache", relPathKey)
	}
	dirInfo := val.(lstatInfo)

	// 3. Create a synthetic directory item for the parent.
	// Use the pool to avoid allocation in this hot path.
	parentItem := s.syncItemPool.Get().(*syncItem)
	defer s.syncItemPool.Put(parentItem) // Ensure it's returned.
	parentItem.RelPathKey = relPathKey   // The relative path key of the parent directory
	parentItem.PathInfo = dirInfo        // The cached PathInfo for the parent directory

	// 4. Perform the I/O using the main directory handler.
	// If this fails, the file copy cannot proceed.
	return s.processDirectorySync(parentItem)
}

// syncItemProducer is a dedicated goroutine that walks the source directory tree,
// sending each syncItem to the syncItems channel for processing by workers.
func (s *nativeSyncer) syncItemProducer() {
	defer close(s.syncItemsChan) // Close syncItemsChan to signal syncWorkers to stop when walk is complete
	err := filepath.WalkDir(s.src, func(absSrcPath string, d os.DirEntry, err error) error {
		// Calculate relative path key immediately.
		// This is needed for both error handling (to prevent deletion) and normal processing.
		relPathKey, normErr := util.NormalizedRelPath(s.src, absSrcPath)
		if normErr != nil {
			return fmt.Errorf("could not get relative path for %s: %w", absSrcPath, normErr)
		}

		if err != nil {
			// CRITICAL: Record any discovered path unconditionally here
			// If it's a subdir/file we can't read, record it so we don't delete it from destination.
			if s.mirror {
				s.existingRelSourcePaths.Store(relPathKey)
			}

			// CRITICAL: If source root is unreadable, abort sync immediately.
			if relPathKey == "." {
				return fmt.Errorf("source root is unreadable: %w", err)
			}

			// Check if the error is due to context cancellation and if so propagat it.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				plog.Debug("Sync walker canceled", "path", absSrcPath)
				return err // Propagate cancellation.
			}

			// If we can't access a path, log the error but keep walking
			plog.Warn("SKIP", "reason", "error accessing path", "path", absSrcPath, "error", err)
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		s.metrics.AddEntriesProcessed(1)

		// Check for exclusions.
		// `relPathKey` is already normalized, `d.Name()` is the raw basename from the filesystem
		// and is passed directly to `isExcluded`, which handles all normalization.
		if s.isExcluded(relPathKey, d.Name(), d.IsDir()) {
			plog.Notice("EXCL", "reason", "excluded by pattern", "path", relPathKey)
			if d.IsDir() {
				s.metrics.AddDirsExcluded(1) // Track excluded directory
				return filepath.SkipDir      // Don't descend into this directory.
			}
			s.metrics.AddFilesExcluded(1) // Track excluded file
			return nil                    // It's an excluded file, do not process further.
		}

		// CRITICAL: Record the path unconditionally here
		// If we mirror and the item exists in the source, it MUST be recorded in the set
		// to prevent it from being deleted during the mirror phase.
		if s.mirror {
			s.existingRelSourcePaths.Store(relPathKey)
		}

		// Get Info for worker
		// WalkDir gives us a DirEntry. We need the FileInfo for timestamps/sizes later.
		// Doing it here saves the worker from doing an Lstat.
		info, err := d.Info()
		if err != nil {
			plog.Warn("SKIP", "reason", "failed to get file info", "path", absSrcPath, "error", err)
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil // It was a file we couldn't get info for, just skip it.
		}

		isDir := info.Mode().IsDir()
		isSymlink := info.Mode()&os.ModeSymlink != 0
		isRegular := info.Mode().IsRegular()
		if !isDir && !isRegular && !isSymlink {
			// Named Pipes, Sockets, etc. are discovered for mirror mode but not synced.
			plog.Notice("SKIP", "type", info.Mode().String(), "path", relPathKey)
			return nil
		}

		// Get an item from the pool to reduce allocations.
		item := s.syncItemPool.Get().(*syncItem)
		item.RelPathKey = relPathKey
		item.PathInfo.ModTime = info.ModTime().UnixNano()
		item.PathInfo.Size = info.Size()
		item.PathInfo.Mode = info.Mode()
		item.PathInfo.IsDir = isDir
		item.PathInfo.IsRegular = isRegular
		item.PathInfo.IsSymlink = isSymlink

		// If it's a directory, cache its PathInfo for workers to use later.
		if item.PathInfo.IsDir {
			s.syncSourceDirInfoCache.Store(item.RelPathKey, item.PathInfo)
		}

		// Send all regular files and directories to the workers.
		select {
		case <-s.ctx.Done():
			s.syncItemPool.Put(item)
			return s.ctx.Err() // Propagate cancellation error.
		case s.syncItemsChan <- item:
			return nil
		}
	})

	if err != nil {
		// If the walker fails, send the error and cancel everything.
		// This is a blocking send because a walker failure is critical and must be reported.
		// We use select to avoid leaking the goroutine if the receiver has stopped.
		select {
		case s.criticalSyncErrsChan <- fmt.Errorf("sync producer failed: %w", err):
		case <-s.ctx.Done():
		default:
			// If the channel is full, a critical error is already pending (likely from a worker).
			// We log this error and exit to ensure the channel is closed and workers can finish.
			plog.Warn("Sync producer failed, but critical error channel is full", "error", err)
		}
	}
}

// syncWorker acts as a Consumer. It reads items from the 'syncItems' channel,
// processes them (I/O).
//
// DESIGN NOTE on Race Conditions:
// A potential race exists where a worker confirms a parent directory exists, but a mirror
// worker could theoretically delete it before this worker copies a file into it.
// This is prevented because the entire sync phase (all syncWorkers) completes before
// the mirror phase (deletions) begins. This function relies on that sequential execution.
func (s *nativeSyncer) syncWorker() {
	defer s.syncWg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		case item, ok := <-s.syncItemsChan:
			if !ok {
				// Channel closed by Walker, work is done.
				return
			}

			// Anonymous Function (IIFE) for reliable defer
			func() {
				// Return the item to the pool after processing.
				defer s.syncItemPool.Put(item)

				if item.PathInfo.IsDir {
					// This is a dir item.
					if err := s.processDirectorySync(item); err != nil {
						plog.Warn("Failed to sync directory", "path", item.RelPathKey, "error", err)
					}
					return // dir created
				}
				// This is a file or symlink item.

				// 1. Ensure Parent Directory Exists (still required for files whose parent directory's
				// item hasn't been processed yet, ensuring order)

				// CRITICAL: Re-normalize the parent key. after `filepath.Dir` as it can return a path with
				// OS-specific separators (e.g., '\' on Windows)
				parentRelPathKey := util.NormalizePath(filepath.Dir(item.RelPathKey))

				if err := s.ensureParentDirectoryExists(parentRelPathKey); err != nil {
					fileErr := fmt.Errorf("failed to ensure parent directory %s exists and is writable: %w", parentRelPathKey, err)
					if s.failFast {
						// Fail-fast mode: treat this as a critical error.
						// Use a non-blocking send in case another worker has already sent a critical error.
						select {
						case s.criticalSyncErrsChan <- fileErr:
						default:
						}
					} else {
						// Default mode: record as a non-fatal error and continue.
						s.syncErrs.Store(item.RelPathKey, fileErr)
						plog.Warn("Sync failed for path; it will be preserved in the destination to prevent deletion",
							"path", item.RelPathKey,
							"error", fileErr)
					}
					return // no dir no filecopy
				}
				// 2. Process the sync (file or symlink)
				var err error
				if item.PathInfo.IsSymlink {
					err = s.processSymlinkSync(item)
				} else {
					err = s.processFileSync(item)
				}

				if err != nil {
					if s.failFast {
						// Fail-fast mode: treat this as a critical error.
						// Use a non-blocking send in case another worker has already sent a critical error.
						select {
						case s.criticalSyncErrsChan <- err:
						default:
						}
						return // Stop processing this file, the main loop will catch the error.
					}
					// Individual file I/O errors (e.g., file locked, permissions issue) are
					// considered non-fatal for the overall backup process. Instead of
					// immediately stopping, the error is recorded, and the worker continues
					// processing other files. This allows the backup to achieve partial
					// success, providing a comprehensive report of all failed files at the end.
					//
					// Data Integrity: The `syncItemProducer` has already added this `RelPathKey` to
					// `existingRelSourcePaths`. This ensures that even if the file copy fails, the
					// existing (potentially outdated) version in the destination will NOT be
					// deleted during the mirror phase, preventing data loss.
					s.syncErrs.Store(item.RelPathKey, err)
					plog.Warn("Sync failed for path; it will be preserved in the destination to prevent deletion",
						"path", item.RelPathKey,
						"error", err,
						"note", "This file may be inconsistent or outdated in the destination")
					return
				}
			}()
		}
	}
}

// handleSync coordinates the concurrent synchronization pipeline.
// It uses a Producer-Consumer pattern.
func (s *nativeSyncer) handleSync() error {
	plog.Notice("SYN", "from", s.src, "to", s.trg)

	// Clear our Sync Optimization Caches when we are finished
	defer func() {
		s.syncSourceDirInfoCache = &sharded.ShardedMap{} // Clear memory immediately
		s.syncTargetLStatCache = newLStatSnapshotStore() // Clear memory immediately
	}()

	// 1. Start syncWorkers (Consumers).
	// They read from 'syncItems' and store results in a concurrent map.
	for range s.numSyncWorkers {
		s.syncWg.Add(1)
		go s.syncWorker()
	}

	// 2. Start the syncItemProducer (Producer)
	// This goroutine walks the file tree and feeds paths into 'syncItems'.
	go s.syncItemProducer()

	// 3. Wait for all workers to finish processing all items.
	s.syncWg.Wait()

	// 4. Check for any critical errors captured by workers during the sync phase.
	select {
	case err := <-s.criticalSyncErrsChan:
		// A critical error occurred (e.g., walker failed), fail fast.
		return fmt.Errorf("critical sync error: %w", err)
	default:
		// No critical errors, check for non-fatal worker errors.
	}

	allErrors := s.syncErrs.Items()
	if len(allErrors) == 0 {
		return nil // No worker errors, success.
	}

	// Log a summary of all non-fatal errors, but return nil so the mirror phase can run.
	// Returning an error here would prevent deletions, which is not the desired behavior
	// for non-critical, individual file errors.
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d non-fatal errors occurred during sync:\n", len(allErrors)))
	for path, err := range allErrors {
		sb.WriteString(fmt.Sprintf("  - path: %s, error: %v\n", path, err))
	}
	plog.Warn(sb.String())
	return nil
}

// mirrorItemProducer is the producer for the deletion phase. It walks the destination
// directory and sends paths that need to be deleted to the mirrorItemsChan.
// It returns a slice of directory paths to be deleted after all files are gone.
func (s *nativeSyncer) mirrorItemProducer() {
	defer close(s.mirrorItemsChan)
	err := filepath.WalkDir(s.trg, func(absTrgPath string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to access path for deletion check: %w", err)
		}

		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}

		relPathKey, err := util.NormalizedRelPath(s.trg, absTrgPath)
		if err != nil {
			return err
		}

		s.metrics.AddEntriesProcessed(1)

		if s.existingRelSourcePaths.Has(relPathKey) {
			return nil // Path exists in source, keep it.
		}

		// CLEANUP: Check for stale temp files from previous crashed runs.
		// These are files like "pgl-backup-12345.tmp". Since handleMirror runs
		// strictly after handleSync is finished, any such file found here is stale.
		// We identify them here to bypass exclusion rules (e.g. *.tmp) and ensure deletion.
		isStaleTemp := !d.IsDir() && strings.HasPrefix(d.Name(), "pgl-backup-") && strings.HasSuffix(d.Name(), ".tmp")

		// Check for exclusions.
		// `relPathKey` is already normalized, but `d.Name()` is the raw basename from the filesystem
		// and is passed directly to `isExcluded`, which handles all normalization.
		if !isStaleTemp && s.isExcluded(relPathKey, d.Name(), d.IsDir()) {
			if d.IsDir() { // Do not log excluded directories during mirror, as they are not actioned upon.
				return filepath.SkipDir // Excluded dir, leave it and its contents.
			}
			return nil // Excluded file, leave it.
		}

		// This path needs to be deleted.
		if d.IsDir() {
			// For directories, we add them to a list to be deleted later.
			// We store them in the set to be sorted and deleted in handleMirror.
			s.mirrorDirsToDelete.Store(relPathKey)
		} else {
			// For files, we can send them to be deleted immediately.
			item := s.mirrorItemPool.Get().(*mirrorItem)
			item.RelPathKey = relPathKey
			select {
			case s.mirrorItemsChan <- item:
			case <-s.ctx.Done():
				s.mirrorItemPool.Put(item)
				return s.ctx.Err()
			}
		}
		return nil
	})

	if err != nil {
		// A walker failure is a critical error for the mirror phase.
		// Use select to avoid deadlock if the channel is full (e.g., a worker already failed fast).
		select {
		case s.criticalMirrorErrsChan <- fmt.Errorf("mirror producer failed: %w", err):
		case <-s.ctx.Done():
		default:
			// If the channel is full, a critical error is already pending. Log this one as a warning.
			plog.Warn("Mirror producer failed, but critical error channel is full", "error", err)
		}
	}
}

// mirrorWorker is the consumer for the deletion phase. It reads paths from
// mirrorItemsChan and deletes them.
func (s *nativeSyncer) mirrorWorker() {
	defer s.mirrorWg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		case item, ok := <-s.mirrorItemsChan:
			if !ok {
				return // Channel closed.
			}
			// Use an anonymous function to create a new scope for defer.
			// This ensures the item is returned to the pool at the end of each loop iteration.
			func() {
				// Return the item to the pool when this iteration is done.
				defer s.mirrorItemPool.Put(item)

				absPathToDelete := util.DenormalizedAbsPath(s.trg, item.RelPathKey)

				if s.dryRun {
					plog.Notice("[DRY RUN] DELETE", "path", item.RelPathKey)
					return
				}

				plog.Notice("DELETE", "path", item.RelPathKey)

				if err := os.RemoveAll(absPathToDelete); err != nil {
					if s.failFast {
						// In fail-fast mode, any deletion error is critical.
						// Use a non-blocking send in case another worker has already sent a critical error.
						select {
						case s.criticalMirrorErrsChan <- err:
						default:
						}
						return // The defer will run.
					}
					// In normal mode, record the error and continue.
					s.mirrorErrs.Store(item.RelPathKey, err)
				} else {
					s.metrics.AddFilesDeleted(1)
				}
			}()
		}
	}
}

// handleMirror performs a sequential walk on the destination to remove files
// and directories that do not exist in the source. This is only active in mirror mode.
func (s *nativeSyncer) handleMirror() error {
	plog.Notice("MIR", "from", s.src, "to", s.trg)
	// --- Phase 2A: Concurrent Deletion of Files ---
	// Start mirror workers.
	for range s.numMirrorWorkers {
		s.mirrorWg.Add(1)
		go s.mirrorWorker()
	}

	// Start the mirrorItemProducer (Producer) in a goroutine.
	go s.mirrorItemProducer()

	// Wait for all file deletions to complete.
	s.mirrorWg.Wait()

	// Check for critical errors first.
	select {
	case err := <-s.criticalMirrorErrsChan:
		return fmt.Errorf("critical mirror error: %w", err)
	default:
	}

	// --- Phase 2B: Sequential Deletion of Directories ---
	// Now that all files are gone, delete the obsolete directories.
	// We retrieve them from the set and sort by length descending to ensure children are removed before parents.
	// Explanation: A child path (e.g., "a/b") is always strictly longer than its parent path ("a").
	// By deleting longest paths first, we guarantee that we never attempt to delete a parent directory
	// before its children are gone, preventing "directory not empty" errors.
	relPathKeyDirsToDelete := s.mirrorDirsToDelete.Keys()
	// Sort descending from longest to shortest
	slices.SortFunc(relPathKeyDirsToDelete, func(a, b string) int {
		return len(b) - len(a)
	})

	for _, relPathKey := range relPathKeyDirsToDelete {
		absPathToDelete := util.DenormalizedAbsPath(s.trg, relPathKey)
		if s.dryRun {
			plog.Notice("[DRY RUN] DELETE", "path", relPathKey)
			continue
		}
		plog.Notice("DELETE", "path", relPathKey)

		// Use os.Remove first, as we expect the directory to be empty of files, which is faster.
		if err := os.Remove(absPathToDelete); err == nil {
			s.metrics.AddDirsDeleted(1)
		} else {
			// If os.Remove fails, the directory is likely not empty (e.g., contains excluded files
			// or files that failed to delete). We MUST NOT use os.RemoveAll here, as that would
			// force-delete excluded files that the user explicitly wanted to keep.
			plog.Debug("Directory removal skipped (not empty)", "path", relPathKey, "error", err)
		}
	}

	// If there were any non-fatal errors during deletion, log them as a summary.
	// We return nil because failing to delete an obsolete file is not a critical
	// failure for the overall backup run.
	allErrors := s.mirrorErrs.Items()
	if len(allErrors) == 0 {
		return nil // No worker errors, success.
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d non-fatal errors occurred during mirror phase:\n", len(allErrors)))
	for path, err := range allErrors {
		sb.WriteString(fmt.Sprintf("  - path: %s, error: %v\n", path, err))
	}
	plog.Warn(sb.String())
	return nil
}

// Sync coordinates the concurrent synchronization pipeline.
func (s *nativeSyncer) Sync(ctx context.Context, absSourcePath, absTargetPath string) error {

	s.ctx = ctx

	// store the paths
	s.src = absSourcePath
	s.trg = absTargetPath

	// 1. Run the main synchronization of files and directories.
	if err := s.handleSync(); err != nil {
		return err
	}

	// Check if context was cancelled externally.
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}

	// 2. Mirror Phase (Deletions)
	// Now that the sync is done and the map is fully populated, run the deletion phase.
	if s.mirror {
		return s.handleMirror()
	}
	return nil
}
