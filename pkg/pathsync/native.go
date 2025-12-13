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
// 1. The Producer (`syncWalker`):
//    - A single goroutine that walks the source directory tree (`filepath.WalkDir`).
//    - It handles directories directly: creating them in the destination and recording their
//      presence in the `discoveredSrcPaths` set.
//    - For files, it creates a `syncTask` and sends it to the `syncTasks` channel for workers.
//
// 2. The Consumers (`syncWorker` pool):
//    - A pool of worker goroutines that read `syncTask` items from the `syncTasks` channel.
//    - Each worker performs the I/O for a single file (checking, copying).
//    - The `syncWalker` has already recorded the file's presence in the `discoveredSrcPaths` set.
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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/metrics"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
	"pixelgardenlabs.io/pgl-backup/pkg/sharded"
	"pixelgardenlabs.io/pgl-backup/pkg/util"
)

// compactPathInfo holds the essential, primitive data from an os.FileInfo.
// Storing this directly instead of the os.FileInfo interface avoids a pointer
// lookup and reduces GC pressure, as the data is inlined in the parent struct.
type compactPathInfo struct {
	ModTime int64       // Unix Nano. Stored as int64 to avoid GC overhead of time.Time's internal pointer.
	Size    int64       // Size in bytes.
	Mode    os.FileMode // File mode bits.
	IsDir   bool        // True if the path is a directory.
}

// syncTask holds all the necessary metadata for a worker to process a file
// without re-calculating paths or re-fetching filesystem stats.
type syncTask struct {
	RelPathKey string          // Normalized, forward-slash, lowercase (if applicable) key. NOT for direct FS access.
	PathInfo   compactPathInfo // Cached info from the Walker
}

// mirrorTask holds the necessary metadata for a worker to process a deletion.
type mirrorTask struct {
	// Normalized, forward-slash, lowercase (if applicable) key. NOT for direct FS access.
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
	literals    map[string]struct{}
	nonLiterals []preProcessedExclusion
}

// preProcessedExclusion stores the pre-analyzed pattern details.
type preProcessedExclusion struct {
	pattern      string
	cleanPattern string // The pattern without the wildcard for prefix/suffix matching
	matchType    exclusionType
}

type syncRun struct {
	src, trg                 string
	mirror, dryRun, failFast bool
	numSyncWorkers           int
	numMirrorWorkers         int
	caseInsensitive          bool
	fileExcludes             exclusionSet
	dirExcludes              exclusionSet
	retryCount               int
	retryWait                time.Duration
	modTimeWindow            time.Duration // The time window to consider file modification times equal.
	ioBufferPool             *sync.Pool    // pointer to avoid copying the noCopy field if the struct is ever passed by value
	syncTaskPool             *sync.Pool
	mirrorTaskPool           *sync.Pool

	// discoveredPaths is a concurrent set populated by the syncWalker. It holds every
	// non-excluded path found in the source directory. During the mirror phase, it is
	// read to determine which paths in the destination are no longer present in the source
	// and should be deleted.
	discoveredPaths *sharded.ShardedSet

	// discoveredDirInfo is a concurrent map populated by the syncWalker. It stores the
	// PathInfo for every directory found in the source. This serves as a cache to avoid
	// redundant Lstat calls by workers needing to create parent directories.
	discoveredDirInfo *sharded.ShardedMap

	// syncedDirCache tracks directories that have ALREADY been created in the destination
	// by any syncWorker. This prevents duplicate MkdirAll calls across the concurrent pool.
	syncedDirCache *sharded.ShardedSet

	// syncWg waits for the syncWalker and syncWorkers to finish processing all sync tasks.
	syncWg sync.WaitGroup

	// syncTasksChan is the channel where the Walker sends pre-processed tasks.
	syncTasksChan chan *syncTask

	// criticalSyncErrsChan captures the first critical, unrecoverable error (e.g., walker failure)
	// to enable a "fail-fast" exit from the sync phase.
	criticalSyncErrsChan chan error

	// syncErrs is a concurrent map that captures non-fatal I/O errors from any worker,
	// keyed by the relative path of the file that failed.
	syncErrs *sharded.ShardedMap

	// mirrorWg waits for the mirrorWalker and mirrorWorkers to finish processing all deletion tasks.
	mirrorWg sync.WaitGroup

	// mirrorTasksChan is the channel where the mirrorWalker sends paths to be deleted.
	mirrorTasksChan chan *mirrorTask

	// criticalMirrorErrsChan captures the first critical error from the mirror phase.
	criticalMirrorErrsChan chan error

	// mirrorErrs captures non-fatal I/O errors from mirror workers.
	mirrorErrs *sharded.ShardedMap

	// ctx is the cancellable context for the entire run.
	ctx    context.Context
	cancel context.CancelFunc

	// metrics holds the counters for the sync operation.
	metrics metrics.Metrics
}

// --- Helpers ---

// copyFileHelper handles the low-level details of copying a single file.
// It ensures atomicity by writing to a temporary file first and then renaming it.
func (r *syncRun) copyFileHelper(absSrcPath, absTrgPath string, task *syncTask, retryCount int, retryWait time.Duration) error {
	var lastErr error
	for i := 0; i <= retryCount; i++ {
		if i > 0 {
			plog.Warn("Retrying file copy", "file", absSrcPath, "attempt", fmt.Sprintf("%d/%d", i, retryCount), "after", retryWait)
			time.Sleep(retryWait)
		}

		lastErr = func() (err error) {
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

			// Get a buffer from the pool for the copy operation.
			bufPtr := r.ioBufferPool.Get().(*[]byte)
			defer r.ioBufferPool.Put(bufPtr)

			// 3. Copy content
			if _, err = io.CopyBuffer(out, in, *bufPtr); err != nil {
				out.Close() // Close before returning on error, buffer is released by defer
				return fmt.Errorf("failed to copy content from %s to %s: %w", absSrcPath, absTempPath, err)
			}

			// 4. Copy file permissions from the source PathInfo
			if err := out.Chmod(task.PathInfo.Mode); err != nil {
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
	return fmt.Errorf("failed to copy file %s after %d attempts: %w", absSrcPath, retryCount, lastErr)
}

// preProcessExclusions analyzes and categorizes patterns to enable optimized matching later.
func preProcessExclusions(patterns []string, isDirPatterns bool, caseInsensitive bool) exclusionSet {
	set := exclusionSet{
		literals:    make(map[string]struct{}),
		nonLiterals: make([]preProcessedExclusion, 0, len(patterns)),
	}

	for _, p := range patterns {
		// Normalize to use forward slashes for consistent matching logic.
		p = filepath.ToSlash(p)

		// On case-insensitive systems, convert pattern to lowercase to match normalized paths.
		if caseInsensitive {
			p = strings.ToLower(p)
		}

		if strings.ContainsAny(p, "*?[]") {
			// If it's a prefix pattern like `node_modules/*`, we can optimize it.
			if strings.HasSuffix(p, "/*") {
				set.nonLiterals = append(set.nonLiterals, preProcessedExclusion{
					pattern:      p,
					cleanPattern: strings.TrimSuffix(p, "/*"),
					matchType:    prefixMatch,
				})
			} else if strings.HasPrefix(p, "*") && !strings.ContainsAny(p[1:], "*?[]") {
				// If it's a suffix pattern like `*.log`, we can also optimize it.
				set.nonLiterals = append(set.nonLiterals, preProcessedExclusion{
					pattern:      p,
					cleanPattern: p[1:], // The part after the *, e.g., ".log"
					matchType:    suffixMatch,
				})
			} else {
				// Otherwise, it's a general glob pattern.
				set.nonLiterals = append(set.nonLiterals, preProcessedExclusion{pattern: p, matchType: globMatch})
			}
		} else {
			// No wildcards. Check if it's a directory prefix or a literal match.
			// Refinement: If this is the directory exclusion list OR the pattern ends in a slash,
			// we treat it as a prefix match to exclude contents inside.
			if isDirPatterns || strings.HasSuffix(p, "/") {
				set.nonLiterals = append(set.nonLiterals, preProcessedExclusion{
					pattern:      p,
					cleanPattern: strings.TrimSuffix(p, "/"),
					matchType:    prefixMatch,
				})
			} else {
				// Pure literal file match (e.g., "README.md")
				set.literals[p] = struct{}{}
			}
		}
	}
	return set
}

// truncateModTime adjusts a time based on the configured modification time window.
func (r *syncRun) truncateModTime(t time.Time) time.Time {
	if r.modTimeWindow > 0 {
		return t.Truncate(r.modTimeWindow)
	}
	return t
}

// normalizePathKey normalizes the pathKey to a standardized key format
// (forward slashes, lowercase if applicable). This key is for internal logic, not direct filesystem access.
func (r *syncRun) normalizePathKey(pathKey string) string {
	// 1. Ensures the map key is consistent across all OS types.
	pathKey = filepath.ToSlash(pathKey)

	// 2. Apply case-insensitivity if required (for Windows/macOS key comparison)
	if r.caseInsensitive {
		pathKey = strings.ToLower(pathKey)
	}
	return pathKey
}

// denormalizePathKey converts the standardized (forward-slash) relative path key
// back into native OS path.
func (r *syncRun) denormalizePathKey(pathKey string) string {
	return filepath.FromSlash(pathKey)
}

// normalizedRelPathParentKey calculates the relative path of the parent and normalizes it to a standardized key format
// (forward slashes, lowercase if applicable). This key is for internal logic, not direct filesystem access.
func (r *syncRun) normalizedParentRelPathKey(relPathKey string) string {
	parentRelPathKey := filepath.Dir(relPathKey)
	// CRITICAL: Re-normalize the parent key. `filepath.Dir` can return a path with
	// OS-specific separators (e.g., '\' on Windows), but our cache keys are
	// standardized to use forward slashes.
	return r.normalizePathKey(parentRelPathKey)
}

// normalizedRelPathKey calculates the relative path and normalizes it to a standardized key format
// (forward slashes, lowercase if applicable). This key is for internal logic, not direct filesystem access.
func (r *syncRun) normalizedRelPathKey(base, absPath string) (string, error) {
	relPathKey, err := filepath.Rel(base, absPath)
	if err != nil {
		return "", fmt.Errorf("failed to get relative path for %s: %w", absPath, err)
	}
	return r.normalizePathKey(relPathKey), nil
}

// denormalizedAbsPath converts the standardized (forward-slash) relative path key
// back into the final, absolute, native OS path for filesystem access.
func (r *syncRun) denormalizedAbsPath(base, relPathKey string) string {
	return filepath.Join(base, r.denormalizePathKey(relPathKey))
}

// isExcluded checks if a given relative path key matches any of the exclusion patterns,
// using a tiered optimization strategy to avoid expensive glob matching when possible.
// It assumes `relPathKey` has already been normalized.
func (r *syncRun) isExcluded(relPathKey string, isDir bool) bool {
	var patterns exclusionSet

	if isDir {
		patterns = r.dirExcludes
	} else {
		patterns = r.fileExcludes
	}

	// 1. Check for O(1) literal matches first.
	if _, ok := patterns.literals[relPathKey]; ok {
		return true
	}

	// 2. If no literal match, check other pattern types.
	for _, p := range patterns.nonLiterals {
		switch p.matchType {
		case prefixMatch:
			// Check 1: Exact match for the excluded directory/folder name (e.g., relPathKey == "build")
			if relPathKey == p.cleanPattern {
				return true
			}

			// Check 2: Match any file/dir inside the excluded folder (e.g., relPathKey starts with "build/")
			if strings.HasPrefix(relPathKey, p.cleanPattern+"/") {
				return true
			}
		case suffixMatch:
			// A pattern like "*.log" is cleaned to ".log". A simple suffix check is sufficient
			// because we only care if the path ends with this string. Unlike prefix matching,
			// there is no container/directory that needs a separate literal check.
			if strings.HasSuffix(relPathKey, p.cleanPattern) {
				return true
			}

		case globMatch:
			match, err := filepath.Match(p.pattern, relPathKey)
			if err != nil {
				// Log the error for the invalid pattern but continue checking others.
				plog.Warn("Invalid exclusion pattern", "pattern", p.pattern, "error", err)
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
func (r *syncRun) processFileSync(task *syncTask) error {
	if r.dryRun {
		plog.Info("[DRY RUN] COPY", "path", task.RelPathKey)
		return nil
	}

	// Convert the paths to the OS-native format for file access
	absSrcPath := r.denormalizedAbsPath(r.src, task.RelPathKey)
	absTrgPath := r.denormalizedAbsPath(r.trg, task.RelPathKey)

	// Check if the destination file exists and if it matches source (size and mod time).
	// We use os.Lstat to get information about the file itself, not its target if it's a symlink.
	trgInfo, err := os.Lstat(absTrgPath)
	if err == nil {
		// Destination path exists.
		if trgInfo.Mode().IsRegular() {
			// It's a regular file. Use the info from os.Lstat directly for comparison.
			// We skip the copy only if the modification times (within the configured window) and sizes are identical.
			// We truncate the times to handle filesystems with different timestamp resolutions.
			if r.truncateModTime(time.Unix(0, task.PathInfo.ModTime)).Equal(r.truncateModTime(trgInfo.ModTime())) && task.PathInfo.Size == trgInfo.Size() {
				r.metrics.AddFilesUpToDate(1)
				return nil // Not changed
			}
		} else {
			// The destination exists but is not a regular file (e.g., it's a directory, symlink, or other special file).
			// To ensure a consistent state, we must remove it before copying the source file.
			plog.Warn("Destination is not a regular file, removing before copy", "path", task.RelPathKey, "type", trgInfo.Mode().String())
			if err := os.RemoveAll(absTrgPath); err != nil {
				return fmt.Errorf("failed to remove non-regular file at destination %s: %w", absTrgPath, err)
			}
			// After removal, proceed to copy the file.
		}
	} else if !os.IsNotExist(err) {
		// An unexpected error occurred while Lstat-ing the destination.
		return fmt.Errorf("failed to lstat destination file %s: %w", absTrgPath, err)
	}

	if err := r.copyFileHelper(absSrcPath, absTrgPath, task, r.retryCount, r.retryWait); err != nil {
		return fmt.Errorf("failed to copy file to %s: %w", absTrgPath, err)
	}

	plog.Info("COPY", "path", task.RelPathKey)
	r.metrics.AddFilesCopied(1)
	return nil // File was actually copied/updated
}

// processDirectorySync handles the creation and permission setting for a directory in the destination.
// It returns filepath.SkipDir if the directory cannot be created, signaling the walker to not descend.
func (r *syncRun) processDirectorySync(task *syncTask) error {

	if r.dryRun {
		plog.Info("[DRY RUN] DIR", "path", task.RelPathKey)
		return nil
	}

	// 1. FAST PATH: Check the cache first.
	if r.syncedDirCache.Has(task.RelPathKey) {
		return nil // Already created.
	}

	// Convert the path to the OS-native format for file access
	absTrgPath := r.denormalizedAbsPath(r.trg, task.RelPathKey)
	expectedPerms := util.WithUserWritePermission(task.PathInfo.Mode.Perm())

	// 2. Perform the concurrent I/O.
	// Optimistic creation: Try Chmod first (cheapest syscall).
	// If it works, dir exists. If not, MkdirAll.
	// This avoids the internal Stat() loop of MkdirAll for existing directories.
	if err := os.Chmod(absTrgPath, expectedPerms); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(absTrgPath, expectedPerms); err != nil {
				plog.Warn("Failed to create destination directory, skipping", "path", task.RelPathKey, "error", err)
				// Path is already recorded, but we can't descend.
				return filepath.SkipDir
			}
		} else {
			plog.Warn("Failed to set permissions on destination directory", "path", task.RelPathKey, "error", err)
			// A Chmod failure on an existing dir should not typically cause a SkipDir.
			// If we cannot set permissions, it's an error for the file copy that relies on this directory.
			// This error will be propagated to the syncWorker, which will record it as a non-fatal error.
			return fmt.Errorf("failed to set permissions on destination directory %s: %w", task.RelPathKey, err)
		}
	}

	// 3. Atomically update the cache and check if we were the first to do so.
	// If LoadOrStore returns true, it means the key already existed, and we should not increment the metric.
	if alreadyExisted := r.syncedDirCache.LoadOrStore(task.RelPathKey); alreadyExisted {
		return nil
	}

	plog.Info("DIR", "path", task.RelPathKey)
	r.metrics.AddDirsCreated(1)
	return nil
}

// ensureParentDirectoryExists is called by file-processing workers to guarantee that the
// parent directory for a file exists before the file copy is attempted.
func (r *syncRun) ensureParentDirectoryExists(relPathKey string) error {
	// 1. FAST PATH: Check the cache first.
	if r.syncedDirCache.Has(relPathKey) {
		return nil // Already created.
	}

	// 2. If not in cache, we need to create it now.
	// Instead of re-statting the source directory, we look up its info from the cache
	// populated by the syncWalker.
	val, ok := r.discoveredDirInfo.Load(relPathKey)
	if !ok {
		// This should be logically impossible if the walker has processed the parent
		// directory before its child file. We return an error to be safe.
		return fmt.Errorf("internal logic error: PathInfo for parent directory %s not found in cache", relPathKey)
	}
	dirInfo := val.(compactPathInfo)

	// 2. Create a synthetic directory task for the parent.
	parentTask := syncTask{
		RelPathKey: relPathKey, // The relative path key of the parent directory
		PathInfo:   dirInfo,    // The cached PathInfo for the parent directory
	}

	// 3. Perform the I/O using the main directory handler.
	// If this fails, the file copy cannot proceed.
	return r.processDirectorySync(&parentTask)
}

// syncWalker is a dedicated goroutine that walks the source directory tree,
// sending each syncTask to the syncTasks channel for processing by workers.
func (r *syncRun) syncWalker() {
	defer close(r.syncTasksChan) // Close syncTasksChan to signal syncWorkers to stop when walk is complete

	err := filepath.WalkDir(r.src, func(absSrcPath string, d os.DirEntry, err error) error {
		if err != nil {
			// If we can't access a path, log the error but keep walking
			plog.Warn("SKIP", "reason", "error accessing path", "path", absSrcPath, "error", err)
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		relPathKey, err := r.normalizedRelPathKey(r.src, absSrcPath)
		if err != nil {
			// Something really is off, fail fast, this should never happen!
			return fmt.Errorf("could not get relative path for %s: %w", absSrcPath, err)
		}

		// We skip processing the root dir itself of the walk.
		if relPathKey == "." {
			return nil
		}

		// Check for exclusions.
		if r.isExcluded(relPathKey, d.IsDir()) {
			plog.Info("EXCL", "reason", "excluded by pattern", "path", relPathKey)
			if d.IsDir() {
				r.metrics.AddDirsExcluded(1) // Track excluded directory
				return filepath.SkipDir      // Don't descend into this directory.
			}
			r.metrics.AddFilesExcluded(1) // Track excluded file
			return nil                    // It's an excluded file, do not process further.
		}

		// 3. Get Info for worker
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

		// --- CRITICAL: Record the path unconditionally here ---
		// If we mirror and the item exists in the source, it MUST be recorded in the set
		// to prevent it from being deleted during the mirror phase.
		if r.mirror {
			r.discoveredPaths.Store(relPathKey)
		}
		// ----------------------------------------------------------------

		isDir := info.Mode().IsDir()
		if !isDir && !info.Mode().IsRegular() {
			// Symlinks, Named Pipes, etc. are discovered for mirror mode but not synced.
			plog.Info("SKIP", "type", info.Mode().String(), "path", relPathKey)
			return nil
		}

		// Get a task from the pool to reduce allocations.
		task := r.syncTaskPool.Get().(*syncTask)
		task.RelPathKey = relPathKey
		task.PathInfo.ModTime = info.ModTime().UnixNano()
		task.PathInfo.Size = info.Size()
		task.PathInfo.Mode = info.Mode()
		task.PathInfo.IsDir = isDir

		// If it's a directory, cache its PathInfo for workers to use later.
		if task.PathInfo.IsDir {
			r.discoveredDirInfo.Store(task.RelPathKey, task.PathInfo)
		}

		// Send all regular files and directories to the workers.
		select {
		case <-r.ctx.Done():
			return r.ctx.Err() // Propagate cancellation error.
		case r.syncTasksChan <- task:
			return nil
		}
	})

	if err != nil {
		// If the walker fails, send the error and cancel everything.
		// This is a blocking send because a walker failure is critical and must be reported.
		// The handleSync function will pick this up and terminate the process.
		r.criticalSyncErrsChan <- fmt.Errorf("walker failed: %w", err)
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
func (r *syncRun) syncWorker() {
	defer r.syncWg.Done()

	for {
		select {
		case <-r.ctx.Done():
			return
		case task, ok := <-r.syncTasksChan:
			if !ok {
				// Channel closed by Walker, work is done.
				return
			}

			// Anonymous Function (IIFE) for reliable defer
			func() {
				// Return the task to the pool after processing.
				defer r.syncTaskPool.Put(task)

				if task.PathInfo.IsDir {
					// This is a dir task.
					if err := r.processDirectorySync(task); err != nil {
						plog.Warn("Failed to sync directory", "path", task.RelPathKey, "error", err)
					}
					return // dir created
				}
				// This is a file task.
				// 1. Ensure Parent Directory Exists (still required for files whose parent directory's
				// task hasn't been processed yet, ensuring order)
				parentRelPathKey := r.normalizedParentRelPathKey(task.RelPathKey)

				if parentRelPathKey != "." {
					if err := r.ensureParentDirectoryExists(parentRelPathKey); err != nil {
						fileErr := fmt.Errorf("failed to ensure parent directory %s exists and is writable: %w", parentRelPathKey, err)
						if r.failFast {
							// Fail-fast mode: treat this as a critical error.
							// Use a non-blocking send in case another worker has already sent a critical error.
							select {
							case r.criticalSyncErrsChan <- fileErr:
							default:
							}
						} else {
							// Default mode: record as a non-fatal error and continue.
							r.syncErrs.Store(task.RelPathKey, fileErr)
							plog.Warn("Sync failed for path; it will be preserved in the destination to prevent deletion",
								"path", task.RelPathKey,
								"error", fileErr)
						}
						return // no dir no filecopy
					}
				}
				// 2. Process the file sync
				if err := r.processFileSync(task); err != nil {
					if r.failFast {
						// Fail-fast mode: treat this as a critical error.
						// Use a non-blocking send in case another worker has already sent a critical error.
						select {
						case r.criticalSyncErrsChan <- err:
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
					// Data Integrity: The `syncWalker` has already added this `RelPathKey` to
					// `discoveredPaths`. This ensures that even if the file copy fails, the
					// existing (potentially outdated) version in the destination will NOT be
					// deleted during the mirror phase, preventing data loss.
					r.syncErrs.Store(task.RelPathKey, err)
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
func (r *syncRun) handleSync() error {
	plog.Info("SYN", "from", r.src, "to", r.trg)
	// 1. Start syncWorkers (Consumers).
	// They read from 'syncTasks' and store results in a concurrent map.
	for i := 0; i < r.numSyncWorkers; i++ {
		r.syncWg.Add(1)
		go r.syncWorker()
	}

	// 3. Start the syncWalker (Producer)
	// This goroutine walks the file tree and feeds paths into 'syncTasks'.
	go r.syncWalker()

	// 3. Wait for all workers to finish processing all tasks.
	r.syncWg.Wait()

	// 4. Check for any critical errors captured by workers during the sync phase.
	select {
	case err := <-r.criticalSyncErrsChan:
		// A critical error occurred (e.g., walker failed), fail fast.
		return fmt.Errorf("critical sync error: %w", err)
	default:
		// No critical errors, check for non-fatal worker errors.
	}

	allErrors := r.syncErrs.Items()
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

// mirrorWalker is the producer for the deletion phase. It walks the destination
// directory and sends paths that need to be deleted to the mirrorTasksChan.
// It returns a slice of directory paths to be deleted after all files are gone.
func (r *syncRun) mirrorWalker() []string {
	defer close(r.mirrorTasksChan)
	var relPathKeyDirsToDelete []string
	err := filepath.WalkDir(r.trg, func(absTrgPath string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to access path for deletion check: %w", err)
		}

		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
		}

		relPathKey, err := r.normalizedRelPathKey(r.trg, absTrgPath)
		if err != nil {
			return err
		}

		if relPathKey == "." {
			return nil
		}

		if r.discoveredPaths.Has(relPathKey) {
			return nil // Path exists in source, keep it.
		}

		if r.isExcluded(relPathKey, d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir // Excluded dir, leave it and its contents.
			}
			return nil // Excluded file, leave it.
		}

		// This path needs to be deleted.
		if d.IsDir() {
			// For directories, we add them to a list to be deleted later.
			// This ensures we delete contents before the directory itself (post-order).
			relPathKeyDirsToDelete = append(relPathKeyDirsToDelete, relPathKey)
		} else {
			// For files, we can send them to be deleted immediately.
			task := r.mirrorTaskPool.Get().(*mirrorTask)
			task.RelPathKey = relPathKey
			select {
			case r.mirrorTasksChan <- task:
			case <-r.ctx.Done():
				return r.ctx.Err()
			}
		}
		return nil
	})

	if err != nil {
		// A walker failure is a critical error for the mirror phase.
		r.criticalMirrorErrsChan <- fmt.Errorf("mirror walker failed: %w", err)
	}
	return relPathKeyDirsToDelete
}

// mirrorWorker is the consumer for the deletion phase. It reads paths from
// mirrorTasksChan and deletes them.
func (r *syncRun) mirrorWorker() {
	defer r.mirrorWg.Done()

	for {
		select {
		case <-r.ctx.Done():
			return
		case task, ok := <-r.mirrorTasksChan:
			if !ok {
				return // Channel closed.
			}
			// Use an anonymous function to create a new scope for defer.
			// This ensures the task is returned to the pool at the end of each loop iteration.
			func() {
				// Return the task to the pool when this iteration is done.
				defer r.mirrorTaskPool.Put(task)

				absPathToDelete := r.denormalizedAbsPath(r.trg, task.RelPathKey)

				if r.dryRun {
					plog.Info("[DRY RUN] DELETE", "path", task.RelPathKey)
					return
				}

				plog.Info("DELETE", "path", task.RelPathKey)

				if err := os.RemoveAll(absPathToDelete); err != nil {
					if r.failFast {
						// In fail-fast mode, any deletion error is critical.
						// Use a non-blocking send in case another worker has already sent a critical error.
						select {
						case r.criticalMirrorErrsChan <- err:
						default:
						}
						return // The defer will run.
					}
					// In normal mode, record the error and continue.
					r.mirrorErrs.Store(task.RelPathKey, err)
				} else {
					r.metrics.AddFilesDeleted(1)
				}
			}()
		}
	}
}

// handleMirror performs a sequential walk on the destination to remove files
// and directories that do not exist in the source. This is only active in mirror mode.
func (r *syncRun) handleMirror() error {
	plog.Info("MIR", "from", r.src, "to", r.trg)
	// --- Phase 2A: Concurrent Deletion of Files ---
	// Start mirror workers.
	for i := 0; i < r.numMirrorWorkers; i++ {
		r.mirrorWg.Add(1)
		go r.mirrorWorker()
	}

	// Walk the destination and send files to be deleted to the workers.
	// This returns a list of directories that also need to be deleted.
	relPathKeyDirsToDelete := r.mirrorWalker()

	// Wait for all file deletions to complete.
	r.mirrorWg.Wait()

	// Check for critical errors first.
	select {
	case err := <-r.criticalMirrorErrsChan:
		return fmt.Errorf("critical mirror error: %w", err)
	default:
	}

	// --- Phase 2B: Sequential Deletion of Directories ---
	// Now that all files are gone, delete the obsolete directories.
	// We do this sequentially and in reverse order to ensure children are removed before parents.
	for i := len(relPathKeyDirsToDelete) - 1; i >= 0; i-- {
		relPathKey := relPathKeyDirsToDelete[i]
		absPathToDelete := r.denormalizedAbsPath(r.trg, relPathKey)
		if r.dryRun {
			plog.Info("[DRY RUN] DELETE", "path", relPathKey)
			continue
		}
		plog.Info("DELETE", "path", relPathKey)

		// Use os.Remove first, as we expect the directory to be empty of files, which is faster.
		if err := os.Remove(absPathToDelete); err == nil {
			r.metrics.AddDirsDeleted(1)
		} else {
			// Attempt os.RemoveAll fallback
			// This might happen if a file inside couldn't be deleted earlier due to permissions,
			// leaving the directory non-empty. As a fallback, try a recursive removal.
			if err := os.RemoveAll(absPathToDelete); err == nil {
				r.metrics.AddDirsDeleted(1)
			} else {
				// If even RemoveAll fails, log a warning. This indicates a more
				// serious issue like permissions on the directory itself.
				plog.Warn("Directory removal failed", "path", relPathKey, "error", err)
			}
		}
	}

	// If there were any non-fatal errors during deletion, log them as a summary.
	// We return nil because failing to delete an obsolete file is not a critical
	// failure for the overall backup run.
	allErrors := r.mirrorErrs.Items()
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
func (r *syncRun) execute() error {
	// The context passed in is now decorated with a cancel function that this
	// run can use to signal a stop to all its goroutines.
	// We defer the cancel to ensure resources are cleaned up on exit.
	r.ctx, r.cancel = context.WithCancel(r.ctx)
	defer func() {
		r.metrics.Log()
		r.cancel()
	}()

	// 1. Run the main synchronization of files and directories.
	if err := r.handleSync(); err != nil {
		return err
	}

	// Check if context was cancelled externally.
	if r.ctx.Err() != nil {
		return r.ctx.Err()
	}

	// 2. Mirror Phase (Deletions)
	// Now that the sync is done and the map is fully populated, run the deletion phase.
	if !r.mirror {
		return nil
	}
	return r.handleMirror()
}

// handleNative initializes the sync run structure and kicks off the execution.
func (s *PathSyncer) handleNative(ctx context.Context, src, trg string, mirror bool, excludeFiles, excludeDirs []string, enableMetrics bool) error {
	isCaseInsensitive := util.IsCaseInsensitiveFS()

	var m metrics.Metrics
	if enableMetrics {
		m = &metrics.SyncMetrics{}
	} else {
		// Use the No-op implementation if metrics are disabled.
		m = &metrics.NoopMetrics{}
	}

	run := &syncRun{
		src:              src,
		trg:              trg,
		mirror:           mirror,
		dryRun:           s.dryRun,
		failFast:         s.failFast,
		caseInsensitive:  isCaseInsensitive,
		fileExcludes:     preProcessExclusions(excludeFiles, false, isCaseInsensitive),
		dirExcludes:      preProcessExclusions(excludeDirs, true, isCaseInsensitive),
		numSyncWorkers:   s.engine.Performance.SyncWorkers,
		numMirrorWorkers: s.engine.Performance.MirrorWorkers,
		retryCount:       s.engine.RetryCount,
		retryWait:        time.Duration(s.engine.RetryWaitSeconds) * time.Second,
		modTimeWindow:    time.Duration(s.engine.ModTimeWindowSeconds) * time.Second,
		ioBufferPool: &sync.Pool{
			New: func() interface{} {
				// Buffer size is configured in KB, so multiply by 1024.
				b := make([]byte, s.engine.Performance.CopyBufferSizeKB*1024)
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
		discoveredPaths:   sharded.NewShardedSet(),
		discoveredDirInfo: sharded.NewShardedMap(),
		syncedDirCache:    sharded.NewShardedSet(),
		// Buffer 'syncTasksChan' to absorb bursts of small files discovered by the walker.
		syncTasksChan:          make(chan *syncTask, s.engine.Performance.SyncWorkers*100),
		mirrorTasksChan:        make(chan *mirrorTask, s.engine.Performance.MirrorWorkers*100),
		criticalSyncErrsChan:   make(chan error, 1),
		syncErrs:               sharded.NewShardedMap(),
		criticalMirrorErrsChan: make(chan error, 1),
		mirrorErrs:             sharded.NewShardedMap(),
		ctx:                    ctx,
		metrics:                m, // Use the selected metrics implementation.
	}
	s.lastRun = run // Store the run instance for testing.
	return run.execute()
}
