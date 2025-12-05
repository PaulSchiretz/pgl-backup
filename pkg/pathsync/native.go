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
	"runtime"
	"strings"
	"sync"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/plog"
	"pixelgardenlabs.io/pgl-backup/pkg/sharded"
)

// isCaseInsensitiveFS checks if the current operating system has a case-insensitive filesystem by default.
func isCaseInsensitiveFS() bool {
	return runtime.GOOS == "windows" || runtime.GOOS == "darwin"
}

// compactFileInfo holds the essential, primitive data from an os.FileInfo.
// Storing this directly instead of the os.FileInfo interface avoids a pointer
// lookup and reduces GC pressure, as the data is inlined in the parent struct.
type compactFileInfo struct {
	ModTime int64 // Unix Nano. Stored as int64 to avoid GC overhead of time.Time's internal pointer.
	Size    int64
	Mode    os.FileMode
}

// syncTask holds all the necessary metadata for a worker to process a file
// without re-calculating paths or re-fetching filesystem stats.
type syncTask struct {
	RelPathKey string          // Normalized, forward-slash, lowercase (if applicable) key. NOT for direct FS access.
	FileInfo   compactFileInfo // Cached info from the Walker
	IsDir      bool
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

type nativeSyncRun struct {
	src, trg              string
	preserveSourceDirName bool
	mirror, dryRun, quiet bool
	numSyncWorkers        int
	caseInsensitive       bool
	fileExcludes          exclusionSet
	dirExcludes           exclusionSet
	retryCount            int
	retryWait             time.Duration
	modTimeWindow         time.Duration // The time window to consider file modification times equal.
	ioBufferPool          *sync.Pool    // pointer to avoid copying the noCopy field if the struct is ever passed by value

	// discoveredSrcPaths is a concurrent set populated by the syncWalker. It holds every
	// non-excluded path found in the source directory. During the mirror phase, it is
	// read to determine which paths in the destination are no longer present in the source
	// and should be deleted.
	discoveredSrcPaths *sharded.ShardedSet

	// syncedDirCache tracks directories that have ALREADY been created in the destination
	// by any syncWorker. This prevents duplicate MkdirAll calls across the concurrent pool.
	syncedDirCache *sharded.ShardedSet

	// syncWg waits for syncWorkers to finish processing tasks.
	syncWg sync.WaitGroup

	// syncTasksChan is the channel where the Walker sends pre-processed tasks.
	syncTasksChan chan syncTask

	// mirrorTasksChan is the channel where the mirrorWalker sends paths to be deleted.
	mirrorTasksChan chan string

	// syncErrs captures the first critical error from any worker to be reported at the end of the run.
	syncErrs chan error

	// ctx is the cancellable context for the entire run.
	ctx context.Context
}

// --- Helpers ---

// copyFileHelper handles the low-level details of copying a single file.
// It ensures atomicity by writing to a temporary file first and then renaming it.
func (r *nativeSyncRun) copyFileHelper(absSrcPath, absTrgPath string, task *syncTask, retryCount int, retryWait time.Duration) error {
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

			// 4. Copy file permissions
			if err := out.Chmod(task.FileInfo.Mode); err != nil {
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
			if err := os.Chtimes(absTempPath, time.Unix(0, task.FileInfo.ModTime), time.Unix(0, task.FileInfo.ModTime)); err != nil {
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
func (r *nativeSyncRun) truncateModTime(t time.Time) time.Time {
	if r.modTimeWindow > 0 {
		return t.Truncate(r.modTimeWindow)
	}
	return t
}

// normalizedRelPathKey calculates the relative path and normalizes it to a standardized key format
// (forward slashes, lowercase if applicable). This key is for internal logic, not direct filesystem access.
func (r *nativeSyncRun) normalizedRelPathKey(base, absPath string) (string, error) {
	relPathKey, err := filepath.Rel(base, absPath)
	if err != nil {
		return "", fmt.Errorf("failed to get relative path for %s: %w", absPath, err)
	}

	// 1. Ensures the map key is consistent across all OS types.
	relPathKey = filepath.ToSlash(relPathKey)

	// 2. Apply case-insensitivity if required (for Windows/macOS key comparison)
	if r.caseInsensitive {
		relPathKey = strings.ToLower(relPathKey)
	}
	return relPathKey, nil
}

// denormalizedAbsPath converts the standardized (forward-slash) relative path key
// back into the final, absolute, native OS path for filesystem access.
func (r *nativeSyncRun) denormalizedAbsPath(base, relPathKey string) string {
	relPath := filepath.FromSlash(relPathKey)
	return filepath.Join(base, relPath)
}

// withBackupWritePermission ensures that any directory/file permission has the owner-write
// bit (0200) set. This prevents the backup user from being locked out on subsequent runs.
func withBackupWritePermission(basePerm os.FileMode) os.FileMode {
	// Ensure the backup user always retains write permission.
	return basePerm | 0200
}

// isExcluded checks if a given relative path key matches any of the exclusion patterns,
// using a tiered optimization strategy to avoid expensive glob matching when possible.
// It assumes `relPathKey` has already been normalized.
func (r *nativeSyncRun) isExcluded(relPathKey string, isDir bool) bool {
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
func (r *nativeSyncRun) processFileSync(task *syncTask) error {
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
			if r.truncateModTime(time.Unix(0, task.FileInfo.ModTime)).Equal(r.truncateModTime(trgInfo.ModTime())) && task.FileInfo.Size == trgInfo.Size() {
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

	if !r.quiet {
		plog.Info("COPY", "path", task.RelPathKey)
	}
	return nil // File was actually copied/updated
}

// processDirectorySync handles the creation and permission setting for a directory in the destination.
// It returns filepath.SkipDir if the directory cannot be created, signaling the walker to not descend.
func (r *nativeSyncRun) processDirectorySync(task *syncTask) error {

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
	expectedPerms := withBackupWritePermission(task.FileInfo.Mode.Perm())

	// 2. Perform the concurrent I/O.
	// Optimistic creation: Try Chmod first (cheapest syscall).
	// If it works, dir exists. If not, MkdirAll.
	// This avoids the internal Stat() loop of MkdirAll for existing directories.
	if err := os.Chmod(absTrgPath, expectedPerms); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(absTrgPath, expectedPerms); err != nil {
				plog.Warn("Failed to create destination directory, skipping", "path", absTrgPath, "error", err)
				// Path is already recorded, but we can't descend.
				return filepath.SkipDir
			}
		} else {
			plog.Warn("Failed to set permissions on destination directory, skipping", "path", absTrgPath, "error", err)
			// A Chmod failure on an existing dir should not typically cause a SkipDir.
			// We log the warning and continue descent.
			return nil
		}
	}

	// 3. Update the cache for all other workers.
	r.syncedDirCache.Store(task.RelPathKey)

	if !r.quiet {
		plog.Info("DIR", "path", task.RelPathKey)
	}
	return nil // Directory exists and permissions are set.
}

// ensureParentDirectoryExists is called by file-processing workers to guarantee that the
// parent directory for a file exists before the file copy is attempted.
func (r *nativeSyncRun) ensureParentDirectoryExists(relPathKey string) error {
	// 1. FAST PATH: Check the cache first.
	if r.syncedDirCache.Has(relPathKey) {
		return nil // Already created.
	}

	// If not in cache, we need to create it NOW.

	// We need source info to construct a task to get correct permissions.
	absSrcPath := r.denormalizedAbsPath(r.src, relPathKey)
	info, err := os.Lstat(absSrcPath)
	if err != nil {
		// Cannot stat source directory to determine permissions.
		return fmt.Errorf("cannot stat source directory %s to determine permissions: %w", absSrcPath, err)
	}

	// 2. Create a synthetic directory task for the parent.
	parentTask := syncTask{
		RelPathKey: relPathKey,
		IsDir:      true,
		FileInfo: compactFileInfo{
			ModTime: info.ModTime().UnixNano(),
			Size:    info.Size(),
			Mode:    info.Mode(),
		},
	}

	// 3. Perform the I/O using the main directory handler.
	// If this fails, the file copy cannot proceed.
	return r.processDirectorySync(&parentTask)
}

// syncWalker is a dedicated goroutine that walks the source directory tree,
// sending each syncTask to the syncTasks channel for processing by workers.
func (r *nativeSyncRun) syncWalker() {
	defer close(r.syncTasksChan) // Close syncTasksChan to signal syncWorkers to stop when walk is complete

	err := filepath.WalkDir(r.src, func(absSrcPath string, d os.DirEntry, err error) error {
		if err != nil {
			// If we can't access a path, log the error but keep walking
			plog.Warn("Error accessing path, skipping", "path", absSrcPath, "error", err)
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

		// If preserveSourceDirName is true, the root directory itself needs to be processed
		// to ensure its permissions are set in the destination.
		// If preserveSourceDirName is false, we are syncing *contents* into an already
		// existing destination, so we skip processing the root of the walk.
		if relPathKey == "." {
			if !r.preserveSourceDirName {
				return nil
			}
		}

		// Check for exclusions.
		if r.isExcluded(relPathKey, d.IsDir()) {
			if !r.quiet {
				plog.Info("SKIP", "reason", "excluded by pattern", "path", relPathKey)
			}
			if d.IsDir() {
				return filepath.SkipDir // Don't descend into this directory.
			}
			return nil // It's an excluded file, leave it alone.
		}

		// 3. Get Info for worker
		// WalkDir gives us a DirEntry. We need the FileInfo for timestamps/sizes later.
		// Doing it here saves the worker from doing an Lstat.
		info, err := d.Info()
		if err != nil {
			plog.Warn("Failed to get file info, skipping", "path", absSrcPath, "error", err)
			return nil
		}

		// --- CRITICAL: Record the path unconditionally here ---
		// If we mirror and the item exists in the source, it MUST be recorded in the set
		// to prevent it from being deleted during the mirror phase (Phase 2).
		if r.mirror {
			r.discoveredSrcPaths.Store(relPathKey)
		}
		// ----------------------------------------------------------------

		task := syncTask{
			RelPathKey: relPathKey,
			FileInfo: compactFileInfo{
				ModTime: info.ModTime().UnixNano(),
				Size:    info.Size(),
				Mode:    info.Mode(),
			},
			IsDir: info.Mode().IsDir(),
		}

		if !task.IsDir && !info.Mode().IsRegular() {
			// Symlinks, Named Pipes, etc.
			if !r.quiet {
				plog.Info("SKIP", "type", info.Mode().String(), "path", relPathKey)
			}
			return nil
		}

		// Send all regular files and directories to the workers.
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case r.syncTasksChan <- task:
			return nil
		}
	})

	if err != nil {
		// If the walker fails, send the error and cancel everything.
		select {
		case r.syncErrs <- fmt.Errorf("walker failed: %w", err):
		default:
		}
	}
}

// syncWorker acts as a Consumer. It reads tasks from the 'syncTasks' channel,
// processes them (I/O).
func (r *nativeSyncRun) syncWorker() {
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

			if task.IsDir {
				if err := r.processDirectorySync(&task); err != nil {
					plog.Warn("Failed to sync directory", "path", task.RelPathKey, "error", err)
				}
			} else {
				// This is a file task.
				// 1. Ensure Parent Directory Exists(still required for files whose parent directory
				// task hasn't been processed yet, ensuring order)
				parentRelPathKey := filepath.Dir(task.RelPathKey)
				// --- CRITICAL FIX: Re-normalize the parent key ---
				parentRelPathKey = filepath.ToSlash(parentRelPathKey)
				if r.caseInsensitive {
					parentRelPathKey = strings.ToLower(parentRelPathKey)
				}
				// --------------------------------------------------

				if parentRelPathKey != "." || r.preserveSourceDirName {
					if err := r.ensureParentDirectoryExists(parentRelPathKey); err != nil {
						plog.Warn("Failed to create parent directory, skipping file", "parent_path", parentRelPathKey, "file", task.RelPathKey, "error", err)
						continue
					}
				}
				// 2. Process the file sync
				err := r.processFileSync(&task)
				if err != nil {
					// If I/O fails, report the error non-blockingly.
					select {
					case r.syncErrs <- err:
					default:
					}
					plog.Warn("Sync failed for path; it will be preserved in the destination to prevent deletion",
						"path", task.RelPathKey,
						"error", err,
						"note", "This file may be inconsistent or outdated in the destination")
					// We rely on the syncWalker having already added this path to discoveredSrcPaths, preventing it from being deleted in the mirror phase.
				}
			}
		}
	}
}

// handleSync coordinates the concurrent synchronization pipeline.
// It uses a Producer-Consumer pattern.
func (r *nativeSyncRun) handleSync() error {
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
	case err := <-r.syncErrs:
		// A worker encountered a critical I/O error.
		return fmt.Errorf("sync worker failed: %w", err)
	default:
		// No errors from workers, proceed.
	}
	return nil
}

// mirrorWalker is the producer for the deletion phase. It walks the destination
// directory and sends paths that need to be deleted to the mirrorTasksChan.
// It returns a slice of directories to be deleted after all files are gone.
func (r *nativeSyncRun) mirrorWalker() ([]string, error) {
	var dirsToDelete []string

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

		if r.discoveredSrcPaths.Has(relPathKey) {
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
			dirsToDelete = append(dirsToDelete, absTrgPath)
		} else {
			// For files, we can send them to be deleted immediately.
			select {
			case r.mirrorTasksChan <- absTrgPath:
			case <-r.ctx.Done():
				return r.ctx.Err()
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return dirsToDelete, nil
}

// mirrorWorker is the consumer for the deletion phase. It reads paths from
// mirrorTasksChan and deletes them.
func (r *nativeSyncRun) mirrorWorker() {
	defer r.syncWg.Done()

	for {
		select {
		case <-r.ctx.Done():
			return
		case pathToDelete, ok := <-r.mirrorTasksChan:
			if !ok {
				return // Channel closed.
			}

			if r.dryRun {
				plog.Info("[DRY RUN] DELETE", "path", pathToDelete)
				continue
			}

			if !r.quiet {
				plog.Info("DELETE", "path", pathToDelete)
			}

			if err := os.RemoveAll(pathToDelete); err != nil {
				// Report error but don't stop other deletions.
				plog.Warn("Failed to delete path", "path", pathToDelete, "error", err)
			}
		}
	}
}

// handleMirror performs a sequential walk on the destination to remove files
// and directories that do not exist in the source. This is only active in mirror mode.
func (r *nativeSyncRun) handleMirror() error {
	plog.Info("Starting mirror phase (deletions)")

	// --- Phase 2A: Concurrent Deletion of Files ---
	// Start mirror workers.
	for i := 0; i < r.numSyncWorkers; i++ {
		r.syncWg.Add(1)
		go r.mirrorWorker()
	}

	// Walk the destination and send files to be deleted to the workers.
	// This returns a list of directories that also need to be deleted.
	dirsToDelete, err := r.mirrorWalker()
	if err != nil {
		return fmt.Errorf("mirror walker failed: %w", err)
	}

	// All files have been sent; close the channel to signal workers.
	close(r.mirrorTasksChan)

	// Wait for all file deletions to complete.
	r.syncWg.Wait()

	// --- Phase 2B: Sequential Deletion of Directories ---
	// Now that all files are gone, delete the obsolete directories.
	// We do this sequentially and in reverse order to ensure children are removed before parents.
	for i := len(dirsToDelete) - 1; i >= 0; i-- {
		dir := dirsToDelete[i]
		if r.dryRun {
			plog.Info("[DRY RUN] DELETE", "path", dir)
			continue
		}
		if !r.quiet {
			plog.Info("DELETE", "path", dir)
		}
		if err := os.Remove(dir); err != nil {
			plog.Warn("Failed to delete directory, it might not be empty", "path", dir, "error", err)
		}
	}

	return nil
}

// execute coordinates the concurrent synchronization pipeline.
func (r *nativeSyncRun) execute() error {
	// Create a cancellable context for this run.
	// If any component fails, 'cancel()' will stop all other components.
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()
	r.ctx = ctx // This updates the struct, so all methods see the new context

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
func (s *PathSyncer) handleNative(ctx context.Context, src, trg string, preserveSourceDirName, mirror bool, excludeFiles, excludeDirs []string) error {
	plog.Info("Starting native sync", "from", src, "to", trg)

	isCaseInsensitive := isCaseInsensitiveFS()

	run := &nativeSyncRun{
		src:                   src,
		trg:                   trg,
		preserveSourceDirName: preserveSourceDirName,
		mirror:                mirror,
		dryRun:                s.dryRun,
		quiet:                 s.quiet,
		caseInsensitive:       isCaseInsensitive,
		fileExcludes:          preProcessExclusions(excludeFiles, false, isCaseInsensitive),
		dirExcludes:           preProcessExclusions(excludeDirs, true, isCaseInsensitive),
		numSyncWorkers:        s.engine.Performance.SyncWorkers,
		retryCount:            s.engine.NativeEngineRetryCount,
		retryWait:             time.Duration(s.engine.NativeEngineRetryWaitSeconds) * time.Second,
		modTimeWindow:         time.Duration(s.engine.NativeEngineModTimeWindowSeconds) * time.Second,
		ioBufferPool: &sync.Pool{
			New: func() interface{} {
				// Buffer size is configured in KB, so multiply by 1024.
				b := make([]byte, s.engine.NativeEngineCopyBufferSizeKB*1024)
				return &b
			},
		},
		discoveredSrcPaths: sharded.NewShardedSet(),
		syncedDirCache:     sharded.NewShardedSet(),
		// Buffer 'syncTasksChan' to absorb bursts of small files discovered by the walker.
		syncTasksChan:   make(chan syncTask, s.engine.Performance.SyncWorkers*100),
		mirrorTasksChan: make(chan string, s.engine.Performance.SyncWorkers*100),
		syncErrs:        make(chan error, 1),
		ctx:             ctx,
	}
	return run.execute()
}
