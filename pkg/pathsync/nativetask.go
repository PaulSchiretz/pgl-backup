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
// 1. The Producer (`syncTaskProducer`):
//    - A single goroutine that walks the source directory tree (`filepath.WalkDir`).
//    - It handles directories directly: creating them in the destination and recording their
//      presence in the `discoveredSrcPaths` set.
//    - For files, it creates a `syncTask` and sends it to the `syncTasks` channel for workers.
//
// 2. The Consumers (`syncWorker` pool):
//    - A pool of worker goroutines that read `syncTask` items from the `syncTasks` channel.
//    - Each worker performs the I/O for a single file (checking, copying).
//    - The `syncTaskProducer` has already recorded the file's presence in the `discoveredSrcPaths` set.
//
// --- Phase 2: Mirroring (Deletions) ---
//
// 3. The Mirror Phase (`handleMirror`):
//    - If mirroring is enabled, this final pass walks the *destination* directory.
//    - For each item, it checks for its presence in the `discoveredSrcPaths` set.
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

	"github.com/paulschiretz/pgl-backup/pkg/pathsyncmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/sharded"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// compactPathInfo holds the essential, primitive data from an os.FileInfo.
// Storing this directly instead of the os.FileInfo interface avoids a pointer
// lookup and reduces GC pressure, as the data is inlined in the parent struct.
type compactPathInfo struct {
	ModTime   int64       // Unix Nano. Stored as int64 to avoid GC overhead of time.Time's internal pointer.
	Size      int64       // Size in bytes.
	Mode      os.FileMode // File mode bits.
	IsDir     bool        // True if the path is a directory.
	IsSymlink bool        // True if the path is a symlink.
}

// syncTask holds all the necessary metadata for a worker to process a file
// without re-calculating paths or re-fetching filesystem stats.
type syncTask struct {
	RelPathKey string          // Normalized, forward-slash and maybe otherwise modified key. NOT for direct FS access.
	PathInfo   compactPathInfo // Cached info from the Walker
}

// mirrorTask holds the necessary metadata for a worker to process a deletion.
type mirrorTask struct {
	// Normalized, forward-slash and maybe otherwise modified key. NOT for direct FS access.
	RelPathKey string
}

type exclusionType int

const (
	literalMatch exclusionType = iota
	prefixMatch
	suffixMatch
	globMatch
)

// exclusionSet holds the categorized exclusion patterns for efficient matching.
type exclusionSet struct {
	// literals are for exact full-path matches, which are the fastest to check.
	literals map[string]struct{}
	// basenameLiterals are for exact basename matches (e.g., "node_modules"), also very fast.
	basenameLiterals map[string]struct{}
	// nonLiterals are for patterns requiring more complex logic (wildcards, basename matches).
	nonLiterals []preProcessedExclusion
}

// preProcessedExclusion stores the pre-analyzed pattern details.
type preProcessedExclusion struct {
	pattern       string        // The original pattern for logging/debugging.
	cleanPattern  string        // The pattern without wildcards for prefix/suffix matching, or the full pattern for glob/literal.
	matchType     exclusionType // The type of match to perform (prefix, suffix, glob, literal).
	matchBasename bool          // If true, the match is against the path's basename; otherwise, the full relative path.
}

type nativeTask struct {
	*PathSyncer

	// ctx is the cancellable context for the entire run.
	ctx    context.Context
	cancel context.CancelFunc

	src, trg     string
	dryRun       bool
	failFast     bool
	fileExcludes exclusionSet
	dirExcludes  exclusionSet

	retryCount        int
	retryWait         time.Duration
	modTimeWindow     time.Duration
	overwriteBehavior OverwriteBehavior

	mirror bool

	// discoveredPaths is a concurrent set populated by the syncTaskProducer. It holds every
	// non-excluded path found in the source directory. During the mirror phase, it is
	// read to determine which paths in the destination are no longer present in the source
	// and should be deleted.
	discoveredPaths *sharded.ShardedSet

	// discoveredDirInfo is a concurrent map populated by the syncTaskProducer. It stores the
	// PathInfo for every directory found in the source. This serves as a cache to avoid
	// redundant Lstat calls by workers needing to create parent directories.
	discoveredDirInfo *sharded.ShardedMap

	// syncedDirCache tracks directories that have ALREADY been created in the destination
	// by any syncWorker. This prevents duplicate MkdirAll calls across the concurrent pool.
	syncedDirCache *sharded.ShardedSet

	// syncWg waits for the syncTaskProducer and syncWorkers to finish processing all sync tasks.
	syncWg sync.WaitGroup

	// syncTasksChan is the channel where the Walker sends pre-processed tasks.
	syncTasksChan chan *syncTask

	// criticalSyncErrsChan captures the first critical, unrecoverable error (e.g., walker failure)
	// to enable a "fail-fast" exit from the sync phase.
	criticalSyncErrsChan chan error

	// syncErrs is a concurrent map that captures non-fatal I/O errors from any worker,
	// keyed by the relative path of the file that failed.
	syncErrs *sharded.ShardedMap

	// mirrorWg waits for the mirrorTaskProducer and mirrorWorkers to finish processing all deletion tasks.
	mirrorWg sync.WaitGroup

	// mirrorTasksChan is the channel where the mirrorTaskProducer sends paths to be deleted.
	mirrorTasksChan chan *mirrorTask

	// criticalMirrorErrsChan captures the first critical error from the mirror phase.
	criticalMirrorErrsChan chan error

	// mirrorErrs captures non-fatal I/O errors from mirror workers.
	mirrorErrs *sharded.ShardedMap

	// mirrorDirsToDelete tracks directories in the destination that need to be deleted.
	// Populated by mirrorTaskProducer, consumed by handleMirror.
	mirrorDirsToDelete *sharded.ShardedSet

	// metrics holds the counters for the sync operation.
	metrics pathsyncmetrics.Metrics
}

// --- Helpers ---

// copyFileHelper handles the low-level details of copying a single file.
// It ensures atomicity by writing to a temporary file first and then renaming it.
func (t *nativeTask) copyFileHelper(absSrcPath, absTrgPath string, task *syncTask, retryCount int, retryWait time.Duration) error {
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

			absTempPath := out.Name()
			// Defer the removal of the temp file. If the rename succeeds, tempPath will be set to "",
			// making this a no-op. This prevents an error trying to remove a non-existent file.
			defer func() {
				if absTempPath != "" {
					os.Remove(absTempPath)
				}
			}()

			// Pre-allocate file size to reduce fragmentation.
			if task.PathInfo.Size > 0 {
				_ = out.Truncate(task.PathInfo.Size)
			}

			// 3. Copy content

			var bytesWritten int64
			// Get a buffer from the pool for the copy operation.
			bufPtr := t.ioBufferPool.Get().(*[]byte)
			defer t.ioBufferPool.Put(bufPtr)
			// Dereference the bufPtr to get the actual slice
			buf := *bufPtr
			// IMPORTANT: Ensure length is maxed out before use
			// In case someone messed with it, always reset len to cap
			// strictly for io.Read/Copy purposes.
			buf = buf[:cap(buf)]

			if bytesWritten, err = io.CopyBuffer(out, in, buf); err != nil {
				out.Close() // Close before returning on error, buffer is released by defer
				return fmt.Errorf("failed to copy content from %s to %s: %w", absSrcPath, absTempPath, err)
			}

			// update metrics
			t.metrics.AddBytesWritten(int64(bytesWritten))

			// 4. Copy file permissions from the source.
			// CRITICAL: We must ensure the user always has write permission on the destination file
			// to prevent being locked out on subsequent runs (e.g., if the source was read-only).
			if err := out.Chmod(util.WithUserWritePermission(task.PathInfo.Mode)); err != nil {
				out.Close() // Close before returning on error
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
			if err := os.Chtimes(absTempPath, time.Unix(0, task.PathInfo.ModTime), time.Unix(0, task.PathInfo.ModTime)); err != nil {
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

// copySymlinkHelper handles the low-level details of creating a symlink.
// It ensures atomicity by creating a temporary link first and then renaming it.
func (t *nativeTask) copySymlinkHelper(target, absTrgPath string, retryCount int, retryWait time.Duration) error {
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

// normalizeExclusionPattern converts a path or pattern into a standardized,
// case-insensitive key format (forward slashes, lowercase).
func normalizeExclusionPattern(p string) string {
	return strings.ToLower(filepath.ToSlash(p))
}

// preProcessExclusions analyzes and categorizes patterns to enable optimized matching later.
func preProcessExclusions(patterns []string) exclusionSet {
	set := exclusionSet{
		literals:         make(map[string]struct{}),
		basenameLiterals: make(map[string]struct{}),
		nonLiterals:      make([]preProcessedExclusion, 0, len(patterns)),
	}

	// A pattern should match against the basename if it does NOT contain a path separator.
	// This aligns with .gitignore behavior (e.g., "node_modules" matches anywhere).
	shouldMatchBasename := func(p string) bool { return !strings.Contains(p, "/") }

	for _, p := range patterns {
		// Normalize to a consistent, case-insensitive key.
		p = normalizeExclusionPattern(p)
		if strings.ContainsAny(p, "*?[]") {
			// If it's a prefix pattern like `node_modules/*`, we can optimize it.
			if strings.HasSuffix(p, "/*") {
				set.nonLiterals = append(set.nonLiterals, preProcessedExclusion{
					pattern:       p,
					cleanPattern:  strings.TrimSuffix(p, "/*"), // e.g., "build"
					matchType:     prefixMatch,
					matchBasename: false, // This is a full-path prefix.
				})
			} else if strings.HasSuffix(p, "*") && !strings.ContainsAny(p[:len(p)-1], "*?[]") {
				// A pattern like `~*` or `temp_*`.
				set.nonLiterals = append(set.nonLiterals, preProcessedExclusion{
					pattern:       p,
					cleanPattern:  strings.TrimSuffix(p, "*"), // e.g., "~"
					matchType:     prefixMatch,
					matchBasename: shouldMatchBasename(p),
				})
			} else if strings.HasPrefix(p, "*") && !strings.ContainsAny(p[1:], "*?[]") {
				// A pattern like `*.log` or `*.tmp`.
				set.nonLiterals = append(set.nonLiterals, preProcessedExclusion{
					pattern:       p,
					cleanPattern:  p[1:], // e.g., ".log"
					matchType:     suffixMatch,
					matchBasename: shouldMatchBasename(p),
				})
			} else {
				// Otherwise, it's a general glob pattern.
				set.nonLiterals = append(set.nonLiterals, preProcessedExclusion{
					pattern: p, cleanPattern: p, matchType: globMatch, matchBasename: shouldMatchBasename(p),
				})
			}
		} else {
			// No wildcards.
			if strings.HasSuffix(p, "/") {
				// A pattern like `build/` is explicitly a full-path prefix match.
				set.nonLiterals = append(set.nonLiterals, preProcessedExclusion{
					pattern:       p,
					cleanPattern:  strings.TrimSuffix(p, "/"),
					matchType:     prefixMatch,
					matchBasename: false,
				})
			} else {
				// A pattern like "node_modules" or "docs/config.json".
				// If it contains a path separator, it's a full-path literal match.
				// If not, it's a basename literal match.
				isBasenameMatch := shouldMatchBasename(p)
				if isBasenameMatch { // e.g., "node_modules"
					set.basenameLiterals[p] = struct{}{}
				} else { // e.g., "docs/config.json"
					set.literals[p] = struct{}{}
				}
			}
		}
	}
	return set
}

// truncateModTime adjusts a time based on the configured modification time window.
func (t *nativeTask) truncateModTime(ti time.Time) time.Time {
	if t.modTimeWindow > 0 {
		return ti.Truncate(t.modTimeWindow)
	}
	return ti
}

// normalizedRelPathParentKey calculates the relative path of the parent and normalizes it to a standardized key format
// (forward slashes, maybe otherwise modified key). This key is for internal logic, not direct filesystem access.
func (t *nativeTask) normalizedParentRelPathKey(relPathKey string) string {
	parentRelPathKey := filepath.Dir(relPathKey)
	// CRITICAL: Re-normalize the parent key. `filepath.Dir` can return a path with
	// OS-specific separators (e.g., '\' on Windows), but our cache keys are
	// standardized to use forward slashes.
	return util.NormalizePath(parentRelPathKey)
}

// normalizedRelPathKey calculates the relative path and normalizes it to a standardized key format
// (forward slashes, maybe otherwise modified key). This key is for internal logic, not direct filesystem access.
func (t *nativeTask) normalizedRelPathKey(base, absPath string) (string, error) {
	relPathKey, err := filepath.Rel(base, absPath)
	if err != nil {
		return "", fmt.Errorf("failed to get relative path for %s: %w", absPath, err)
	}
	return util.NormalizePath(relPathKey), nil
}

// denormalizedAbsPath converts the standardized (forward-slash, maybe otherwise modified key) relative path key
// back into the final, absolute, native OS path for filesystem access.
func (t *nativeTask) denormalizedAbsPath(base, relPathKey string) string {
	return filepath.Join(base, util.DenormalizePath(relPathKey))
}

// isExcluded checks if a given relative path key matches any of the exclusion patterns,
// using a tiered optimization strategy to avoid expensive glob matching when possible.
// It handles normalization (case-folding and path separators) internally.
func (t *nativeTask) isExcluded(relPathKey, relPathBasename string, isDir bool) bool {
	var patterns exclusionSet

	if isDir {
		patterns = t.dirExcludes
	} else {
		patterns = t.fileExcludes
	}

	// Normalize paths to the same case-insensitive format as the patterns.
	normalizedPath := normalizeExclusionPattern(relPathKey)
	normalizedBasename := normalizeExclusionPattern(relPathBasename)

	// 1. Check for O(1) full-path literal matches.
	if _, ok := patterns.literals[normalizedPath]; ok {
		return true
	}

	// 2. Check for O(1) basename literal matches if the map is not empty.
	if _, ok := patterns.basenameLiterals[normalizedBasename]; ok {
		return true
	}

	// 3. If no literal match, check other pattern types (wildcards).
	for _, p := range patterns.nonLiterals {
		pathToCheck := normalizedPath
		if p.matchBasename {
			pathToCheck = normalizedBasename
		}

		switch p.matchType {
		case prefixMatch:
			if strings.HasPrefix(pathToCheck, p.cleanPattern) {
				// For full-path directory prefixes ("build/"), we must avoid false positives on "build-tools".
				// This check is only relevant for full-path matches.
				if !p.matchBasename && strings.HasSuffix(p.pattern, "/") {
					if pathToCheck != p.cleanPattern && !strings.HasPrefix(pathToCheck, p.cleanPattern+"/") {
						continue // Not a true directory prefix match.
					}
				}
				return true
			}
		case suffixMatch:
			if strings.HasSuffix(pathToCheck, p.cleanPattern) {
				return true
			}

		case globMatch:
			match, err := filepath.Match(p.cleanPattern, pathToCheck)
			if err != nil {
				// Log the error for the invalid pattern but continue checking others.
				plog.Warn("Invalid exclusion pattern", "pattern", p.cleanPattern, "error", err)
				continue
			}
			if match {
				return true
			}
		}
	}
	return false
}

// processFileSync checks if a file needs to be copied (based on size/time)
// and triggers the copy operation if needed.
func (t *nativeTask) processFileSync(task *syncTask) error {
	if t.dryRun {
		plog.Notice("[DRY RUN] COPY", "path", task.RelPathKey)
		return nil
	}

	// Convert the paths to the OS-native format for file access
	absSrcPath := t.denormalizedAbsPath(t.src, task.RelPathKey)
	absTrgPath := t.denormalizedAbsPath(t.trg, task.RelPathKey)

	// Check if the destination file exists and if it matches source (size and mod time).
	// We use os.Lstat to get information about the file itself, not its target if it's a symlink.
	trgInfo, err := os.Lstat(absTrgPath)
	if err == nil {
		// Destination path exists.
		if trgInfo.Mode().IsRegular() {
			// It's a regular file. Use the info from os.Lstat directly for comparison.
			// We skip the copy only if the modification times (within the configured window) and sizes are identical.
			// We truncate the times to handle filesystems with different timestamp resolutions.
			// NOTE: Permissions are intentionally not compared. This prevents unnecessary file copies
			// when only metadata (like the executable bit) changes, which is common with version
			// control systems like Git. The backup process already ensures the destination is writable.
			switch t.overwriteBehavior {
			case OverwriteNever:
				t.metrics.AddFilesUpToDate(1)
				return nil
			case OverwriteIfNewer:
				srcTime := t.truncateModTime(time.Unix(0, task.PathInfo.ModTime))
				if !srcTime.After(t.truncateModTime(trgInfo.ModTime())) {
					t.metrics.AddFilesUpToDate(1)
					return nil
				}
			case OverwriteAlways:
				// Fall through to copy
			default: // OverwriteUpdate
				if t.truncateModTime(time.Unix(0, task.PathInfo.ModTime)).Equal(t.truncateModTime(trgInfo.ModTime())) && task.PathInfo.Size == trgInfo.Size() {
					t.metrics.AddFilesUpToDate(1)
					return nil // Not changed
				}
			}
		} else if trgInfo.IsDir() {
			// Destination is a directory. Rename cannot overwrite it, so we must remove it explicitly.
			plog.Warn("Destination is a directory, removing before copy", "path", task.RelPathKey)
			if err := os.RemoveAll(absTrgPath); err != nil {
				return fmt.Errorf("failed to remove directory at destination %s: %w", absTrgPath, err)
			}
		} else {
			// Destination is a symlink or special file. os.Rename will overwrite it atomically.
			plog.Warn("Destination is not a regular file, overwriting", "path", task.RelPathKey, "type", trgInfo.Mode().String())
		}
	} else if !os.IsNotExist(err) {
		// An unexpected error occurred while Lstat-ing the destination.
		return fmt.Errorf("failed to lstat destination file %s: %w", absTrgPath, err)
	}

	if err := t.copyFileHelper(absSrcPath, absTrgPath, task, t.retryCount, t.retryWait); err != nil {
		return fmt.Errorf("failed to copy file to %s: %w", absTrgPath, err)
	}

	plog.Notice("COPY", "path", task.RelPathKey)
	t.metrics.AddFilesCopied(1)
	return nil // File was actually copied/updated
}

// processSymlinkSync handles the creation or update of a symlink in the destination.
func (t *nativeTask) processSymlinkSync(task *syncTask) error {
	if t.dryRun {
		plog.Notice("[DRY RUN] SYMLINK", "path", task.RelPathKey)
		return nil
	}

	absSrcPath := t.denormalizedAbsPath(t.src, task.RelPathKey)
	absTrgPath := t.denormalizedAbsPath(t.trg, task.RelPathKey)

	// Read the link target from the source.
	target, err := os.Readlink(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to read source symlink %s: %w", absSrcPath, err)
	}

	// Check if the destination exists and matches.
	dstInfo, err := os.Lstat(absTrgPath)
	if err == nil {
		// Destination exists.
		if dstInfo.Mode()&os.ModeSymlink != 0 {
			// It's a symlink. Check if targets match.
			dstTarget, err := os.Readlink(absTrgPath)
			if err == nil && dstTarget == target {
				t.metrics.AddFilesUpToDate(1)
				return nil // Up to date
			}
		} else if dstInfo.IsDir() {
			// Destination is a directory. Rename cannot overwrite it, so we must remove it explicitly.
			plog.Warn("Destination is a directory, removing before symlink creation", "path", task.RelPathKey)
			if err := os.RemoveAll(absTrgPath); err != nil {
				return fmt.Errorf("failed to remove existing destination directory %s: %w", absTrgPath, err)
			}
		} else {
			// Destination is a regular file. os.Rename will overwrite it atomically.
			plog.Warn("Destination is not a symlink, overwriting", "path", task.RelPathKey, "type", dstInfo.Mode().String())
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to lstat destination %s: %w", absTrgPath, err)
	}

	// Create the symlink atomically using a temporary name and rename.
	if err := t.copySymlinkHelper(target, absTrgPath, t.retryCount, t.retryWait); err != nil {
		return err
	}

	plog.Notice("SYMLINK", "path", task.RelPathKey, "target", target)
	t.metrics.AddFilesCopied(1)
	return nil
}

// processDirectorySync handles the creation and permission setting for a directory in the destination.
// It returns an error (specifically filepath.SkipDir) if the directory cannot be created.
func (t *nativeTask) processDirectorySync(task *syncTask) error {
	// 1. FAST PATH: Check the cache first.
	if t.syncedDirCache.Has(task.RelPathKey) {
		return nil // Already created.
	}

	if t.dryRun {
		// Atomically update cache to ensure we only log once per directory.
		if alreadyExisted := t.syncedDirCache.LoadOrStore(task.RelPathKey); !alreadyExisted {
			plog.Notice("[DRY RUN] DIR", "path", task.RelPathKey)
		}
		return nil
	}

	// Convert the path to the OS-native format for file access
	absTrgPath := t.denormalizedAbsPath(t.trg, task.RelPathKey)
	expectedPerms := util.WithUserWritePermission(task.PathInfo.Mode.Perm())

	// 2. Perform the concurrent I/O.
	// Check if the destination exists and handle type conflicts (e.g. File vs Dir).
	var dirCreated bool = false
	info, err := os.Lstat(absTrgPath)
	if err == nil {
		// Path exists.
		if !info.IsDir() {
			// It exists but is not a directory (e.g. it's a file or symlink).
			// We must remove it to create the directory.
			plog.Warn("Destination path exists but is not a directory, removing", "path", task.RelPathKey, "type", info.Mode().String())
			if err := os.RemoveAll(absTrgPath); err != nil {
				return fmt.Errorf("failed to remove conflicting destination file %s: %w", task.RelPathKey, err)
			}
			// Create the directory.
			if err := os.MkdirAll(absTrgPath, expectedPerms); err != nil {
				plog.Warn("Failed to create destination directory, skipping", "path", task.RelPathKey, "error", err)
				return filepath.SkipDir
			}
			dirCreated = true
		} else {
			// It is already a directory. Ensure permissions are correct.
			if err := os.Chmod(absTrgPath, expectedPerms); err != nil {
				plog.Warn("Failed to set permissions on destination directory", "path", task.RelPathKey, "error", err)
				return fmt.Errorf("failed to set permissions on destination directory %s: %w", task.RelPathKey, err)
			}
		}
	} else if os.IsNotExist(err) {
		// Path does not exist. Create it.
		if err := os.MkdirAll(absTrgPath, expectedPerms); err != nil {
			plog.Warn("Failed to create destination directory, skipping", "path", task.RelPathKey, "error", err)
			return filepath.SkipDir
		}
		dirCreated = true
	} else {
		// Unexpected error from Lstat.
		return fmt.Errorf("failed to lstat destination directory %s: %w", task.RelPathKey, err)
	}

	// 3. Atomically update the cache and check if we were the first to do so.
	// If LoadOrStore returns true, it means the key already existed, and we should not increment the metric.
	if alreadyExisted := t.syncedDirCache.LoadOrStore(task.RelPathKey); alreadyExisted {
		return nil
	}

	if dirCreated {
		plog.Notice("DIR", "path", task.RelPathKey)
		t.metrics.AddDirsCreated(1)
	}
	return nil
}

// ensureParentDirectoryExists is called by file-processing workers to guarantee that the
// parent directory for a file exists before the file copy is attempted.
func (t *nativeTask) ensureParentDirectoryExists(relPathKey string) error {
	// 1. FAST PATH: Check the cache first.
	if t.syncedDirCache.Has(relPathKey) {
		return nil // Already created.
	}

	// 2. If not in cache, we need to create it now.
	// Instead of re-statting the source directory, we look up its info from the cache
	// populated by the syncTaskProducer.
	val, ok := t.discoveredDirInfo.Load(relPathKey)
	if !ok {
		// This should be logically impossible if the walker has processed the parent
		// directory before its child file. We return an error to be safe.
		return fmt.Errorf("internal logic error: PathInfo for parent directory %s not found in cache", relPathKey)
	}
	dirInfo := val.(compactPathInfo)

	// 3. Create a synthetic directory task for the parent.
	// Use the pool to avoid allocation in this hot path.
	parentTask := t.syncTaskPool.Get().(*syncTask)
	defer t.syncTaskPool.Put(parentTask) // Ensure it's returned.
	parentTask.RelPathKey = relPathKey   // The relative path key of the parent directory
	parentTask.PathInfo = dirInfo        // The cached PathInfo for the parent directory

	// 4. Perform the I/O using the main directory handler.
	// If this fails, the file copy cannot proceed.
	return t.processDirectorySync(parentTask)
}

// syncTaskProducer is a dedicated goroutine that walks the source directory tree,
// sending each syncTask to the syncTasks channel for processing by workers.
func (t *nativeTask) syncTaskProducer() {
	defer close(t.syncTasksChan) // Close syncTasksChan to signal syncWorkers to stop when walk is complete

	err := filepath.WalkDir(t.src, func(absSrcPath string, d os.DirEntry, err error) error {
		// Calculate relative path key immediately.
		// This is needed for both error handling (to prevent deletion) and normal processing.
		relPathKey, normErr := t.normalizedRelPathKey(t.src, absSrcPath)
		if normErr != nil {
			return fmt.Errorf("could not get relative path for %s: %w", absSrcPath, normErr)
		}

		if err != nil {
			// Check if the error is due to context cancellation.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				plog.Debug("Sync walker cancelled", "path", absSrcPath)
				return err // Propagate cancellation.
			}

			// CRITICAL: If we can't access a path (e.g. Permission Denied), we must ensure we don't
			// accidentally wipe the destination.
			if relPathKey == "." {
				return fmt.Errorf("source root is unreadable: %w", err) // Critical: If source root is unreadable, abort sync immediately.
			}

			// CRITICAL: Record the path unconditionally here
			// If it's a subdir/file we can't read, record it so we don't delete it from destination.
			if t.mirror {
				t.discoveredPaths.Store(relPathKey)
			}

			// If we can't access a path, log the error but keep walking
			plog.Warn("SKIP", "reason", "error accessing path", "path", absSrcPath, "error", err)
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// We skip processing the root dir itself of the walk.
		if relPathKey == "." {
			return nil
		}

		t.metrics.AddEntriesProcessed(1)

		// Check for exclusions.
		// `relPathKey` is already normalized, `d.Name()` is the raw basename from the filesystem
		// and is passed directly to `isExcluded`, which handles all normalization.
		if t.isExcluded(relPathKey, d.Name(), d.IsDir()) {
			plog.Notice("EXCL", "reason", "excluded by pattern", "path", relPathKey)
			if d.IsDir() {
				t.metrics.AddDirsExcluded(1) // Track excluded directory
				return filepath.SkipDir      // Don't descend into this directory.
			}
			t.metrics.AddFilesExcluded(1) // Track excluded file
			return nil                    // It's an excluded file, do not process further.
		}

		// CRITICAL: Record the path unconditionally here
		// If we mirror and the item exists in the source, it MUST be recorded in the set
		// to prevent it from being deleted during the mirror phase.
		if t.mirror {
			t.discoveredPaths.Store(relPathKey)
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
		if !isDir && !info.Mode().IsRegular() && !isSymlink {
			// Named Pipes, Sockets, etc. are discovered for mirror mode but not synced.
			plog.Notice("SKIP", "type", info.Mode().String(), "path", relPathKey)
			return nil
		}

		// Get a task from the pool to reduce allocations.
		task := t.syncTaskPool.Get().(*syncTask)
		task.RelPathKey = relPathKey
		task.PathInfo.ModTime = info.ModTime().UnixNano()
		task.PathInfo.Size = info.Size()
		task.PathInfo.Mode = info.Mode()
		task.PathInfo.IsDir = isDir
		task.PathInfo.IsSymlink = isSymlink

		// If it's a directory, cache its PathInfo for workers to use later.
		if task.PathInfo.IsDir {
			t.discoveredDirInfo.Store(task.RelPathKey, task.PathInfo)
		}

		// Send all regular files and directories to the workers.
		select {
		case <-t.ctx.Done():
			t.syncTaskPool.Put(task)
			return t.ctx.Err() // Propagate cancellation error.
		case t.syncTasksChan <- task:
			return nil
		}
	})

	if err != nil {
		// If the walker fails, send the error and cancel everything.
		// This is a blocking send because a walker failure is critical and must be reported.
		// We use select to avoid leaking the goroutine if the receiver has stopped.
		select {
		case t.criticalSyncErrsChan <- fmt.Errorf("sync producer failed: %w", err):
		case <-t.ctx.Done():
		default:
			// If the channel is full, a critical error is already pending (likely from a worker).
			// We log this error and exit to ensure the channel is closed and workers can finish.
			plog.Warn("Sync producer failed, but critical error channel is full", "error", err)
		}
	}
}

// syncWorker acts as a Consumer. It reads tasks from the 'syncTasks' channel,
// processes them (I/O).
//
// DESIGN NOTE on Race Conditions:
// A potential race exists where a worker confirms a parent directory exists, but a mirror
// worker could theoretically delete it before this worker copies a file into it.
// This is prevented because the entire sync phase (all syncWorkers) completes before
// the mirror phase (deletions) begins. This function relies on that sequential execution.
func (t *nativeTask) syncWorker() {
	defer t.syncWg.Done()

	for {
		select {
		case <-t.ctx.Done():
			return
		case task, ok := <-t.syncTasksChan:
			if !ok {
				// Channel closed by Walker, work is done.
				return
			}

			// Anonymous Function (IIFE) for reliable defer
			func() {
				// Return the task to the pool after processing.
				defer t.syncTaskPool.Put(task)

				if task.PathInfo.IsDir {
					// This is a dir task.
					if err := t.processDirectorySync(task); err != nil {
						plog.Warn("Failed to sync directory", "path", task.RelPathKey, "error", err)
					}
					return // dir created
				}
				// This is a file or symlink task.
				// 1. Ensure Parent Directory Exists (still required for files whose parent directory's
				// task hasn't been processed yet, ensuring order)
				parentRelPathKey := t.normalizedParentRelPathKey(task.RelPathKey)

				if parentRelPathKey != "." {
					if err := t.ensureParentDirectoryExists(parentRelPathKey); err != nil {
						fileErr := fmt.Errorf("failed to ensure parent directory %s exists and is writable: %w", parentRelPathKey, err)
						if t.failFast {
							// Fail-fast mode: treat this as a critical error.
							// Use a non-blocking send in case another worker has already sent a critical error.
							select {
							case t.criticalSyncErrsChan <- fileErr:
							default:
							}
						} else {
							// Default mode: record as a non-fatal error and continue.
							t.syncErrs.Store(task.RelPathKey, fileErr)
							plog.Warn("Sync failed for path; it will be preserved in the destination to prevent deletion",
								"path", task.RelPathKey,
								"error", fileErr)
						}
						return // no dir no filecopy
					}
				}
				// 2. Process the sync (file or symlink)
				var err error
				if task.PathInfo.IsSymlink {
					err = t.processSymlinkSync(task)
				} else {
					err = t.processFileSync(task)
				}

				if err != nil {
					if t.failFast {
						// Fail-fast mode: treat this as a critical error.
						// Use a non-blocking send in case another worker has already sent a critical error.
						select {
						case t.criticalSyncErrsChan <- err:
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
					// Data Integrity: The `syncTaskProducer` has already added this `RelPathKey` to
					// `discoveredPaths`. This ensures that even if the file copy fails, the
					// existing (potentially outdated) version in the destination will NOT be
					// deleted during the mirror phase, preventing data loss.
					t.syncErrs.Store(task.RelPathKey, err)
					plog.Warn("Sync failed for path; it will be preserved in the destination to prevent deletion",
						"path", task.RelPathKey,
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
func (t *nativeTask) handleSync() error {
	plog.Notice("SYN", "from", t.src, "to", t.trg)
	// 1. Start syncWorkers (Consumers).
	// They read from 'syncTasks' and store results in a concurrent map.
	for range t.numSyncWorkers {
		t.syncWg.Add(1)
		go t.syncWorker()
	}

	// 2. Start the syncTaskProducer (Producer)
	// This goroutine walks the file tree and feeds paths into 'syncTasks'.
	go t.syncTaskProducer()

	// 3. Wait for all workers to finish processing all tasks.
	t.syncWg.Wait()

	// 4. Check for any critical errors captured by workers during the sync phase.
	select {
	case err := <-t.criticalSyncErrsChan:
		// A critical error occurred (e.g., walker failed), fail fast.
		return fmt.Errorf("critical sync error: %w", err)
	default:
		// No critical errors, check for non-fatal worker errors.
	}

	allErrors := t.syncErrs.Items()
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

// mirrorTaskProducer is the producer for the deletion phase. It walks the destination
// directory and sends paths that need to be deleted to the mirrorTasksChan.
// It returns a slice of directory paths to be deleted after all files are gone.
func (t *nativeTask) mirrorTaskProducer() {
	defer close(t.mirrorTasksChan)
	err := filepath.WalkDir(t.trg, func(absTrgPath string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to access path for deletion check: %w", err)
		}

		select {
		case <-t.ctx.Done():
			return t.ctx.Err()
		default:
		}

		relPathKey, err := t.normalizedRelPathKey(t.trg, absTrgPath)
		if err != nil {
			return err
		}

		if relPathKey == "." {
			return nil
		}

		t.metrics.AddEntriesProcessed(1)

		if t.discoveredPaths.Has(relPathKey) {
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
		if !isStaleTemp && t.isExcluded(relPathKey, d.Name(), d.IsDir()) {
			if d.IsDir() { // Do not log excluded directories during mirror, as they are not actioned upon.
				return filepath.SkipDir // Excluded dir, leave it and its contents.
			}
			return nil // Excluded file, leave it.
		}

		// This path needs to be deleted.
		if d.IsDir() {
			// For directories, we add them to a list to be deleted later.
			// We store them in the set to be sorted and deleted in handleMirror.
			t.mirrorDirsToDelete.Store(relPathKey)
		} else {
			// For files, we can send them to be deleted immediately.
			task := t.mirrorTaskPool.Get().(*mirrorTask)
			task.RelPathKey = relPathKey
			select {
			case t.mirrorTasksChan <- task:
			case <-t.ctx.Done():
				t.mirrorTaskPool.Put(task)
				return t.ctx.Err()
			}
		}
		return nil
	})

	if err != nil {
		// A walker failure is a critical error for the mirror phase.
		// Use select to avoid deadlock if the channel is full (e.g., a worker already failed fast).
		select {
		case t.criticalMirrorErrsChan <- fmt.Errorf("mirror producer failed: %w", err):
		case <-t.ctx.Done():
		default:
			// If the channel is full, a critical error is already pending. Log this one as a warning.
			plog.Warn("Mirror producer failed, but critical error channel is full", "error", err)
		}
	}
}

// mirrorWorker is the consumer for the deletion phase. It reads paths from
// mirrorTasksChan and deletes them.
func (t *nativeTask) mirrorWorker() {
	defer t.mirrorWg.Done()

	for {
		select {
		case <-t.ctx.Done():
			return
		case task, ok := <-t.mirrorTasksChan:
			if !ok {
				return // Channel closed.
			}
			// Use an anonymous function to create a new scope for defer.
			// This ensures the task is returned to the pool at the end of each loop iteration.
			func() {
				// Return the task to the pool when this iteration is done.
				defer t.mirrorTaskPool.Put(task)

				absPathToDelete := t.denormalizedAbsPath(t.trg, task.RelPathKey)

				if t.dryRun {
					plog.Notice("[DRY RUN] DELETE", "path", task.RelPathKey)
					return
				}

				plog.Notice("DELETE", "path", task.RelPathKey)

				if err := os.RemoveAll(absPathToDelete); err != nil {
					if t.failFast {
						// In fail-fast mode, any deletion error is critical.
						// Use a non-blocking send in case another worker has already sent a critical error.
						select {
						case t.criticalMirrorErrsChan <- err:
						default:
						}
						return // The defer will run.
					}
					// In normal mode, record the error and continue.
					t.mirrorErrs.Store(task.RelPathKey, err)
				} else {
					t.metrics.AddFilesDeleted(1)
				}
			}()
		}
	}
}

// handleMirror performs a sequential walk on the destination to remove files
// and directories that do not exist in the source. This is only active in mirror mode.
func (t *nativeTask) handleMirror() error {
	plog.Notice("MIR", "from", t.src, "to", t.trg)
	// --- Phase 2A: Concurrent Deletion of Files ---
	// Start mirror workers.
	for range t.numMirrorWorkers {
		t.mirrorWg.Add(1)
		go t.mirrorWorker()
	}

	// Start the mirrorTaskProducer (Producer) in a goroutine.
	go t.mirrorTaskProducer()

	// Wait for all file deletions to complete.
	t.mirrorWg.Wait()

	// Check for critical errors first.
	select {
	case err := <-t.criticalMirrorErrsChan:
		return fmt.Errorf("critical mirror error: %w", err)
	default:
	}

	// --- Phase 2B: Sequential Deletion of Directories ---
	// Now that all files are gone, delete the obsolete directories.
	// We retrieve them from the set and sort by length descending to ensure children are removed before parents.
	// Explanation: A child path (e.g., "a/b") is always strictly longer than its parent path ("a").
	// By deleting longest paths first, we guarantee that we never attempt to delete a parent directory
	// before its children are gone, preventing "directory not empty" errors.
	relPathKeyDirsToDelete := t.mirrorDirsToDelete.Keys()
	// Sort descending from longest to shortest
	slices.SortFunc(relPathKeyDirsToDelete, func(a, b string) int {
		return len(b) - len(a)
	})

	for _, relPathKey := range relPathKeyDirsToDelete {
		absPathToDelete := t.denormalizedAbsPath(t.trg, relPathKey)
		if t.dryRun {
			plog.Notice("[DRY RUN] DELETE", "path", relPathKey)
			continue
		}
		plog.Notice("DELETE", "path", relPathKey)

		// Use os.Remove first, as we expect the directory to be empty of files, which is faster.
		if err := os.Remove(absPathToDelete); err == nil {
			t.metrics.AddDirsDeleted(1)
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
	allErrors := t.mirrorErrs.Items()
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

// execute coordinates the concurrent synchronization pipeline.
func (t *nativeTask) execute() error {
	// The context passed in is now decorated with a cancel function that this
	// run can use to signal a stop to all its goroutines.
	// We defer the cancel to ensure resources are cleaned up on exit.
	t.ctx, t.cancel = context.WithCancel(t.ctx)

	// Start progress reporting
	t.metrics.StartProgress("Sync progress", 10*time.Second)
	defer func() {
		t.metrics.StopProgress()
		t.metrics.LogSummary("Sync finished")
		t.cancel()
	}()

	// 1. Run the main synchronization of files and directories.
	if err := t.handleSync(); err != nil {
		return err
	}

	// Check if context was cancelled externally.
	if t.ctx.Err() != nil {
		return t.ctx.Err()
	}

	// 2. Mirror Phase (Deletions)
	// Now that the sync is done and the map is fully populated, run the deletion phase.
	if t.mirror {
		return t.handleMirror()
	}
	return nil
}
