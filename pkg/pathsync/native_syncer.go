package pathsync

// --- ARCHITECTURAL OVERVIEW ---
// The native sync engine uses a single-pass Directory Diff architecture
// with a unified worker pool to ensure maximum speed, low memory usage,
// and mechanical disk friendliness.
//
// 1. Directory Diff Traversal:
//    - Traverses source and destination directories simultaneously in a depth-first walk.
//    - In each directory, source entries and target entries are read and compared by name.
//    - Files new in source: Enqueued directly to workers for copying (zero destination stats needed).
//    - Files existing in both: Size and modtime are compared. Up-to-date files are skipped
//      instantly in memory; modified files are enqueued for workers to copy.
//    - Files/dirs only in destination: If mirroring is enabled, orphan files are enqueued
//      to the same worker pool for deletion, and orphan directories are collected for post-order deletion.
//      If mirroring is disabled, extra files in destination are preserved untouched.
//
// 2. Unified Worker Pool:
//    - A single pool of syncWorkers handles both file copies (OpCopy) and file deletions (OpDelete)
//      concurrently, eliminating duplicate worker pools and channel infrastructure.
//
// 3. Memory Efficiency:
//    - Memory usage is O(directory depth), storing only active directory frames rather than
//      maintaining full-tree path sets.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/limiter"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/pool"
	"github.com/paulschiretz/pgl-backup/pkg/sharded"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

type syncOp int

const (
	opCopy syncOp = iota
	opDelete
)

// syncItem holds all the necessary metadata for a worker to process a copy or delete operation.
type syncItem struct {
	Op         syncOp
	RelPathKey string // Normalized, forward-slash key.
	PathInfo   lstatInfo
}

// syncDirEntry holds info on a single entry within a dir on disk
type syncDirEntry struct {
	entry    os.DirEntry
	excluded bool
}

type nativeSyncer struct {
	// Read buffer pool
	ioBufferPool *pool.FixedBufferPool
	ioBufferSize int64

	// Limiter for sequential writes
	readAheadLimiter *limiter.Memory
	readAheadLimit   int64
	readAheadPool    *pool.BucketedBufferPool

	// ctx is the cancellable context for the entire run.
	ctx    context.Context
	cancel context.CancelFunc

	src, trg       string
	numSyncWorkers int

	mirror            bool
	dryRun            bool
	failFast          bool
	safeCopy          bool
	sequentialWrite   bool
	sequentialWriteMu sync.Mutex

	fileExclusions exclusionSet
	dirExclusions  exclusionSet

	retryCount        int
	retryWait         time.Duration
	modTimeWindow     time.Duration
	overwriteBehavior OverwriteBehavior

	// metrics holds the counters for the sync operation.
	metrics Metrics

	// syncWg waits for syncWorkers to finish processing all copy and delete items.
	syncWg sync.WaitGroup

	// syncItemsChan is the unified channel where syncDirItems sends files to be copied or deleted.
	syncItemsChan chan *syncItem

	// criticalSyncErrsChan captures the first critical error from any worker.
	criticalSyncErrsChan chan error

	// syncErrs captures non-fatal I/O errors from any worker.
	syncErrs *sharded.Map

	// orphanTargetDirEntries tracks empty directories in the destination that need to be deleted.
	orphanTargetDirEntries *sharded.Set

	// Pool for syncItem structs to reduce GC pressure
	syncItemPool *sync.Pool

	// Pool for syncMetricWriter to reduce GC pressure
	syncMetricWriterPool *sync.Pool

	// Pool for syncMetricReader to reduce GC pressure
	syncMetricReaderPool *sync.Pool
}

func newNativeSyncer(
	mirror, dryRun, failFast, safeCopy, sequentialWrite bool,
	fileExclusions, dirExclusions exclusionSet,
	retryCount int,
	retryWait, modTimeWindow time.Duration,
	overwriteBehavior OverwriteBehavior,
	ioBufferPool *pool.FixedBufferPool, ioBufferSize int64,
	readAheadLimiter *limiter.Memory, readAheadLimit int64, readAheadPool *pool.BucketedBufferPool,
	numSyncWorkers int,
	metrics Metrics,
) (*nativeSyncer, error) {
	return &nativeSyncer{
		numSyncWorkers:         numSyncWorkers,
		metrics:                metrics,
		dryRun:                 dryRun,
		failFast:               failFast,
		safeCopy:               safeCopy,
		sequentialWrite:        sequentialWrite,
		fileExclusions:         fileExclusions,
		dirExclusions:          dirExclusions,
		retryCount:             retryCount,
		retryWait:              retryWait,
		modTimeWindow:          modTimeWindow,
		overwriteBehavior:      overwriteBehavior,
		mirror:                 mirror,
		syncItemsChan:          make(chan *syncItem, numSyncWorkers*1024),
		criticalSyncErrsChan:   make(chan error, 1),
		syncErrs:               sharded.NewMap(16),
		orphanTargetDirEntries: sharded.NewSet(16),
		ioBufferPool:           ioBufferPool,
		ioBufferSize:           ioBufferSize,
		readAheadLimiter:       readAheadLimiter,
		readAheadLimit:         readAheadLimit,
		readAheadPool:          readAheadPool,
		syncItemPool: &sync.Pool{
			New: func() any {
				return new(syncItem)
			},
		},
		syncMetricWriterPool: &sync.Pool{
			New: func() any {
				return &syncMetricWriter{metrics: metrics}
			},
		},
		syncMetricReaderPool: &sync.Pool{
			New: func() any {
				return &syncMetricReader{metrics: metrics}
			},
		},
	}, nil
}

// isExcluded checks if a given relative path key matches any of the exclusion patterns.
func (s *nativeSyncer) isExcluded(relPathKey, relPathBasename string, isDir bool) bool {
	if isDir {
		return s.dirExclusions.matches(relPathKey, relPathBasename)
	}
	return s.fileExclusions.matches(relPathKey, relPathBasename)
}

func (s *nativeSyncer) removeWithRetry(absPath string) error {
	const maxRetries = 5
	var err error
	for range maxRetries {
		if err = os.Remove(absPath); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return err
}

func (s *nativeSyncer) removeAllWithRetry(absPath string) error {
	const maxRetries = 5
	var err error
	for range maxRetries {
		if err = os.RemoveAll(absPath); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return err
}

// copyFile handles the details of copying a single file.
func (s *nativeSyncer) copyFile(absSrcPath, absTrgPath string, item *syncItem) error {
	var lastErr error
	for i := range s.retryCount + 1 {
		if i > 0 {
			plog.Warn("Retrying file copy", "file", absSrcPath, "attempt", fmt.Sprintf("%d/%d", i, s.retryCount), "after", s.retryWait)
			time.Sleep(s.retryWait)
		}

		lastErr = func() (err error) {
			// 1. Buffering/Read phase
			in, err := os.Open(absSrcPath)
			if err != nil {
				return fmt.Errorf("failed to open source file %s: %w", absSrcPath, err)
			}
			defer in.Close()

			bufPtr := s.ioBufferPool.Get()
			defer s.ioBufferPool.Put(bufPtr)

			mr := s.syncMetricReaderPool.Get().(*syncMetricReader)
			mr.Reset(in)
			defer s.syncMetricReaderPool.Put(mr)

			var reader io.Reader = mr

			if s.sequentialWrite {
				if s.readAheadLimiter.TryAcquire(item.PathInfo.Size) {
					defer s.readAheadLimiter.Release(item.PathInfo.Size)

					readAheadDataPtr := s.readAheadPool.Get(item.PathInfo.Size)
					defer s.readAheadPool.Put(readAheadDataPtr)

					if _, err := io.ReadFull(mr, *readAheadDataPtr); err != nil {
						return fmt.Errorf("failed to read file into memory: %w", err)
					}
					reader = bytes.NewReader(*readAheadDataPtr)
				}
			}

			// 2. Heavy I/O Phase
			var absWritePath string
			err = func() error {
				var out *os.File
				if s.sequentialWrite {
					s.sequentialWriteMu.Lock()
					defer s.sequentialWriteMu.Unlock()
				}

				if s.safeCopy {
					absTrgDir := filepath.Dir(absTrgPath)
					out, err = os.CreateTemp(absTrgDir, "pgl-backup-*.tmp")
					if err != nil {
						return fmt.Errorf("failed to create temporary file in %s: %w", absTrgDir, err)
					}
					absWritePath = out.Name()
				} else {
					out, err = os.OpenFile(absTrgPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, util.WithUserWritePermission(item.PathInfo.Mode))
					if err != nil {
						return fmt.Errorf("failed to open destination file %s: %w", absTrgPath, err)
					}
					absWritePath = absTrgPath
				}

				defer func() {
					if out != nil {
						out.Close()
					}
				}()

				if err := out.Chmod(util.WithUserWritePermission(item.PathInfo.Mode)); err != nil {
					return fmt.Errorf("failed to set permissions on %s: %w", absWritePath, err)
				}

				if item.PathInfo.Size > 0 {
					_ = out.Truncate(item.PathInfo.Size)
				}

				mw := s.syncMetricWriterPool.Get().(*syncMetricWriter)
				mw.Reset(out)
				defer s.syncMetricWriterPool.Put(mw)

				if _, err := io.CopyBuffer(mw, reader, *bufPtr); err != nil {
					return fmt.Errorf("failed to copy content to %s: %w", absWritePath, err)
				}

				if err := out.Close(); err != nil {
					return fmt.Errorf("failed to close file %s: %w", absWritePath, err)
				}
				return nil
			}()

			if err != nil {
				if s.safeCopy && absWritePath != "" {
					s.removeWithRetry(absWritePath)
				}
				return err
			}

			// 3. Metadata Phase
			if err := os.Chtimes(absWritePath, time.Unix(0, item.PathInfo.ModTime), time.Unix(0, item.PathInfo.ModTime)); err != nil {
				return fmt.Errorf("failed to set timestamps on %s: %w", absWritePath, err)
			}

			if s.safeCopy {
				if err := os.Rename(absWritePath, absTrgPath); err != nil {
					s.removeWithRetry(absWritePath)
					return err
				}
			}
			return nil
		}()

		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("failed to copy file from '%s' to '%s' after %d attempts: %w", absSrcPath, absTrgPath, s.retryCount, lastErr)
}

// copySymlink handles the creation of a symlink.
func (s *nativeSyncer) copySymlink(linkTarget, absTrgPath string) error {
	var lastErr error
	for i := range s.retryCount + 1 {
		if i > 0 {
			plog.Warn("Retrying symlink creation", "file", absTrgPath, "attempt", fmt.Sprintf("%d/%d", i, s.retryCount), "after", s.retryWait)
			time.Sleep(s.retryWait)
		}

		lastErr = func() error {
			if s.sequentialWrite {
				s.sequentialWriteMu.Lock()
				defer s.sequentialWriteMu.Unlock()
			}

			if err := os.Symlink(linkTarget, absTrgPath); err != nil {
				if runtime.GOOS == "windows" && strings.Contains(err.Error(), "privilege") {
					return fmt.Errorf("failed to create symlink (requires Admin or Developer Mode): %w", err)
				}
				return fmt.Errorf("failed to create symlink %s -> %s: %w", absTrgPath, linkTarget, err)
			}
			return nil
		}()

		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("failed to create symlink at '%s' after %d attempts: %w", absTrgPath, s.retryCount, lastErr)
}

func (s *nativeSyncer) processFileSync(item *syncItem) error {
	if s.dryRun {
		plog.Notice("[DRY RUN] COPY", "path", item.RelPathKey)
		return nil
	}

	absSrcPath := util.DenormalizedAbsPath(s.src, item.RelPathKey)
	absTrgPath := util.DenormalizedAbsPath(s.trg, item.RelPathKey)

	if err := s.copyFile(absSrcPath, absTrgPath, item); err != nil {
		return fmt.Errorf("failed to copy file to %s: %w", absTrgPath, err)
	}

	plog.Notice("COPY", "path", item.RelPathKey)
	s.metrics.AddFilesCopied(1)
	return nil
}

func (s *nativeSyncer) processSymlinkSync(item *syncItem) error {
	if s.dryRun {
		plog.Notice("[DRY RUN] SYMLINK", "path", item.RelPathKey)
		return nil
	}

	absSrcPath := util.DenormalizedAbsPath(s.src, item.RelPathKey)
	absTrgPath := util.DenormalizedAbsPath(s.trg, item.RelPathKey)

	linkTarget, err := os.Readlink(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to read source symlink %s: %w", absSrcPath, err)
	}

	if err = s.removeAllWithRetry(absTrgPath); err != nil {
		return err
	}

	if err = s.copySymlink(linkTarget, absTrgPath); err != nil {
		return err
	}

	plog.Notice("SYMLINK", "path", item.RelPathKey, "linkTarget", linkTarget)
	s.metrics.AddFilesCopied(1)
	return nil
}

func (s *nativeSyncer) processDeleteSync(item *syncItem) error {
	absPathToDelete := util.DenormalizedAbsPath(s.trg, item.RelPathKey)

	if s.dryRun {
		plog.Notice("[DRY RUN] DELETE", "path", item.RelPathKey)
		return nil
	}

	plog.Notice("DELETE", "path", item.RelPathKey)

	if err := s.removeAllWithRetry(absPathToDelete); err != nil {
		return err
	}

	s.metrics.AddFilesDeleted(1)
	return nil
}

func (s *nativeSyncer) syncWorker() {
	defer s.syncWg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		case item, ok := <-s.syncItemsChan:
			if !ok {
				return
			}

			func() {
				defer s.syncItemPool.Put(item)

				var err error
				switch item.Op {
				case opCopy:
					if item.PathInfo.IsSymlink {
						err = s.processSymlinkSync(item)
					} else {
						err = s.processFileSync(item)
					}
				case opDelete:
					err = s.processDeleteSync(item)
				}

				if err != nil {
					if s.failFast {
						select {
						case s.criticalSyncErrsChan <- err:
						default:
						}
						s.cancel()
						return
					}
					s.syncErrs.Store(item.RelPathKey, err)
					plog.Warn("Sync failed for path", "path", item.RelPathKey, "error", err)
				}
			}()
		}
	}
}

// syncDirItems reconciles a single directory pair between source and destination.
func (s *nativeSyncer) syncDirItems(relPathKey string) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	absSrcDir := util.DenormalizedAbsPath(s.src, relPathKey)
	absTrgDir := util.DenormalizedAbsPath(s.trg, relPathKey)

	// Special check for root directory "."
	if relPathKey == "." {
		s.metrics.AddEntriesProcessed(1)
		srcRootInfo, err := os.Stat(absSrcDir)
		if err != nil {
			return fmt.Errorf("source root is unreadable: %w", err)
		}
		if !srcRootInfo.IsDir() {
			return fmt.Errorf("source path is not a directory: %s", absSrcDir)
		}

		if s.mirror {
			s.metrics.AddEntriesProcessed(1)
		}

		if !s.dryRun {
			if err := os.MkdirAll(absTrgDir, util.UserWritableDirPerms); err != nil {
				return fmt.Errorf("failed to create destination root %s: %w", absTrgDir, err)
			}
		}
	}

	// 1. Read source directory entries
	srcEntries, err := os.ReadDir(absSrcDir)
	if err != nil {
		if relPathKey == "." {
			return fmt.Errorf("source root is unreadable: %w", err)
		}
		plog.Warn("SKIP", "reason", "error reading source directory", "path", absSrcDir, "error", err)
		return nil
	}

	// 2. Read destination directory entries (if destination exists)
	var trgEntries []os.DirEntry
	if trgEntriesRaw, err := os.ReadDir(absTrgDir); err == nil {
		trgEntries = trgEntriesRaw
	}

	// Index target entries
	orphanTrgDirEntries := make(map[string]syncDirEntry, len(trgEntries))
	for _, d := range trgEntries {
		childName := d.Name()
		var childRelKey string
		if relPathKey == "." {
			childRelKey = childName
		} else {
			childRelKey = path.Join(relPathKey, childName)
		}

		if s.mirror {
			s.metrics.AddEntriesProcessed(1)
		}

		isStaleTemp := !d.IsDir() && strings.HasPrefix(childName, "pgl-backup-") && strings.HasSuffix(childName, ".tmp")
		excluded := false
		if !isStaleTemp && s.isExcluded(childRelKey, childName, d.IsDir()) {
			excluded = true
		}

		orphanTrgDirEntries[childName] = syncDirEntry{
			entry:    d,
			excluded: excluded,
		}
	}

	// 3. Reconcile source entries against target
	for _, d := range srcEntries {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}

		childName := d.Name()
		var childRelKey string
		if relPathKey == "." {
			childRelKey = childName
		} else {
			childRelKey = path.Join(relPathKey, childName)
		}

		s.metrics.AddEntriesProcessed(1)

		if s.isExcluded(childRelKey, childName, d.IsDir()) {
			plog.Notice("EXCL", "reason", "excluded by pattern", "path", childRelKey)
			if d.IsDir() {
				s.metrics.AddDirsExcluded(1)
			} else {
				s.metrics.AddFilesExcluded(1)
			}
			continue
		}

		tData, inTarget := orphanTrgDirEntries[childName]
		if inTarget {
			// remove the entry from the orphanTrgDirEntries if found do not delete it in the mirror phase
			delete(orphanTrgDirEntries, childName)
		}

		childAbsSrc := util.DenormalizedAbsPath(s.src, childRelKey)
		childAbsTrg := util.DenormalizedAbsPath(s.trg, childRelKey)

		if d.IsDir() {
			if inTarget && !tData.entry.IsDir() {
				plog.Warn("Destination path exists but is not a directory, removing", "path", childRelKey)
				if !s.dryRun {
					_ = s.removeAllWithRetry(childAbsTrg)
				}
				inTarget = false
			}

			expectedPerms := util.UserWritableDirPerms
			if srcInfo, err := d.Info(); err == nil {
				expectedPerms = util.WithUserWritePermission(srcInfo.Mode().Perm())
			}

			if !inTarget {
				if s.dryRun {
					plog.Notice("[DRY RUN] DIR", "path", childRelKey)
				} else {
					if err := os.MkdirAll(childAbsTrg, expectedPerms); err != nil {
						plog.Warn("Failed to create destination directory, skipping", "path", childRelKey, "error", err)
						continue
					}
				}
				s.metrics.AddDirsCreated(1)
				plog.Notice("DIR", "path", childRelKey)
			} else {
				if !s.dryRun {
					if trgInfo, err := tData.entry.Info(); err == nil {
						if trgInfo.Mode().Perm() != expectedPerms {
							_ = os.Chmod(childAbsTrg, expectedPerms)
						}
					}
				}
			}

			// Recurse into directory
			if err := s.syncDirItems(childRelKey); err != nil {
				return err
			}
			continue
		}

		// File or Symlink
		isSymlink := d.Type()&os.ModeSymlink != 0
		isRegular := d.Type().IsRegular()
		if !isRegular && !isSymlink {
			plog.Notice("SKIP", "type", d.Type().String(), "path", childRelKey)
			continue
		}

		if inTarget && tData.entry.IsDir() {
			plog.Warn("Destination is a directory, removing before copy", "path", childRelKey)
			if !s.dryRun {
				_ = s.removeAllWithRetry(childAbsTrg)
			}
			inTarget = false
		}

		upToDate := false
		if inTarget {
			if isSymlink {
				if tData.entry.Type()&os.ModeSymlink != 0 {
					srcTarget, err1 := os.Readlink(childAbsSrc)
					dstTarget, err2 := os.Readlink(childAbsTrg)
					if err1 == nil && err2 == nil && srcTarget == dstTarget {
						upToDate = true
					}
				} else {
					// Target is a file, remove it
					if !s.dryRun {
						_ = s.removeAllWithRetry(childAbsTrg)
					}
				}
			} else {
				// Regular file
				if tData.entry.Type()&os.ModeSymlink != 0 {
					// Target is a symlink, remove it
					if !s.dryRun {
						_ = s.removeAllWithRetry(childAbsTrg)
					}
				} else if tData.entry.Type().IsRegular() {
					srcInfo, err1 := d.Info()
					trgInfo, err2 := tData.entry.Info()
					if err1 == nil && err2 == nil {
						srcLstat := makeLstatInfo(srcInfo)
						trgLstat := makeLstatInfo(trgInfo)

						srcTime := time.Unix(0, srcLstat.ModTime)
						trgTime := time.Unix(0, trgLstat.ModTime)
						if s.modTimeWindow > 0 {
							srcTime = srcTime.Truncate(s.modTimeWindow)
							trgTime = trgTime.Truncate(s.modTimeWindow)
						}

						switch s.overwriteBehavior {
						case OverwriteNever:
							upToDate = true
						case OverwriteIfNewer:
							if !srcTime.After(trgTime) {
								upToDate = true
							}
						case OverwriteAlways:
							upToDate = false
						default: // OverwriteUpdate
							if srcTime.Equal(trgTime) && srcLstat.Size == trgLstat.Size {
								upToDate = true
							}
						}
					}
				}
			}
		}

		if upToDate {
			s.metrics.AddFilesUpToDate(1)
			continue
		}

		srcInfo, err := d.Info()
		if err != nil {
			plog.Warn("SKIP", "reason", "failed to get file info", "path", childAbsSrc, "error", err)
			continue
		}

		item := s.syncItemPool.Get().(*syncItem)
		item.Op = opCopy
		item.RelPathKey = childRelKey
		item.PathInfo = makeLstatInfo(srcInfo)

		select {
		case <-s.ctx.Done():
			s.syncItemPool.Put(item)
			return s.ctx.Err()
		case s.syncItemsChan <- item:
		}
	}

	// 4. Mirror remaining target entries (deletions)
	if s.mirror {
		for name, tData := range orphanTrgDirEntries {
			if tData.excluded {
				continue // Excluded in destination, preserve!
			}

			var childRelKey string
			if relPathKey == "." {
				childRelKey = name
			} else {
				childRelKey = path.Join(relPathKey, name)
			}

			if tData.entry.IsDir() {
				if err := s.syncOrphanTargetDirItems(childRelKey); err != nil {
					return err
				}
			} else {
				item := s.syncItemPool.Get().(*syncItem)
				item.Op = opDelete
				item.RelPathKey = childRelKey
				select {
				case <-s.ctx.Done():
					s.syncItemPool.Put(item)
					return s.ctx.Err()
				case s.syncItemsChan <- item:
				}
			}
		}
	}

	return nil
}

// syncOrphanTargetDirItems walks an obsolete target directory to enqueue its files and dirs for deletion.
func (s *nativeSyncer) syncOrphanTargetDirItems(orphanRelKey string) error {
	absTrgDir := util.DenormalizedAbsPath(s.trg, orphanRelKey)
	return filepath.WalkDir(absTrgDir, func(absPath string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to access orphan path %s: %w", absPath, err)
		}

		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}

		relPathKey, normErr := util.NormalizedRelPath(s.trg, absPath)
		if normErr != nil {
			return normErr
		}
		if relPathKey == "." {
			return nil
		}

		s.metrics.AddEntriesProcessed(1)

		isStaleTemp := !d.IsDir() && strings.HasPrefix(d.Name(), "pgl-backup-") && strings.HasSuffix(d.Name(), ".tmp")
		if !isStaleTemp && s.isExcluded(relPathKey, d.Name(), d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if d.IsDir() {
			s.orphanTargetDirEntries.Store(relPathKey)
		} else {
			item := s.syncItemPool.Get().(*syncItem)
			item.Op = opDelete
			item.RelPathKey = relPathKey
			select {
			case <-s.ctx.Done():
				s.syncItemPool.Put(item)
				return s.ctx.Err()
			case s.syncItemsChan <- item:
			}
		}
		return nil
	})
}

func (s *nativeSyncer) handleOrphanTargetDirEntries() error {
	relPathKeyDirsToDelete := s.orphanTargetDirEntries.Keys()
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

		if err := s.removeWithRetry(absPathToDelete); err == nil {
			s.metrics.AddDirsDeleted(1)
		} else {
			plog.Debug("Directory removal skipped (not empty)", "path", relPathKey, "error", err)
		}
	}
	return nil
}

// Sync coordinates the synchronization pipeline using Directory Diff with a unified worker pool.
func (s *nativeSyncer) Sync(ctx context.Context, absSourcePath, absTargetPath string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.ctx = ctx
	s.cancel = cancel

	s.src = absSourcePath
	s.trg = absTargetPath

	plog.Notice("SYN", "from", s.src, "to", s.trg)

	// 1. Start unified sync workers (copies and deletions)
	for range s.numSyncWorkers {
		s.syncWg.Add(1)
		go s.syncWorker()
	}

	// 2. Run directory diff traversal
	syncDirItemsDone := make(chan error, 1)
	go func() {
		defer close(s.syncItemsChan)
		syncDirItemsDone <- s.syncDirItems(".")
	}()

	syncDirItemsErr := <-syncDirItemsDone

	// 3. Wait for all worker operations (copies & deletions) to finish
	s.syncWg.Wait()

	// 4. Delete orphan directories after all orphan files are deleted
	if s.mirror {
		if err := s.handleOrphanTargetDirEntries(); err != nil {
			plog.Warn("Failed to delete some orphan directories", "error", err)
		}
	}

	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	if syncDirItemsErr != nil {
		return syncDirItemsErr
	}

	select {
	case err := <-s.criticalSyncErrsChan:
		return fmt.Errorf("critical sync error: %w", err)
	default:
	}

	allSyncErrors := s.syncErrs.Items()
	if len(allSyncErrors) > 0 {
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("%d non-fatal errors occurred during sync:\n", len(allSyncErrors)))
		for p, err := range allSyncErrors {
			sb.WriteString(fmt.Sprintf("  - path: %s, error: %v\n", p, err))
		}
		plog.Warn(sb.String())
	}

	return nil
}
