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
	AbsPath  string
	RelPath  string
	FileInfo compactFileInfo // Cached info from the Walker
}

type exclusionType int

const (
	literalMatch exclusionType = iota
	prefixMatch
	suffixMatch
	globMatch
)

// preProcessedExclusion stores the pre-analyzed pattern details.
type preProcessedExclusion struct {
	pattern      string
	cleanPattern string // The pattern without the wildcard for prefix/suffix matching
	matchType    exclusionType
}

// nativeSyncRun encapsulates the state and logic for a single native sync operation.
// It coordinates the Producer (Walker), Consumers (Workers), and Collector.
type nativeSyncRun struct {
	src, trg                 string
	mirror, dryRun, quiet    bool
	numSyncWorkers           int
	caseInsensitive          bool
	preProcessedFileExcludes []preProcessedExclusion
	preProcessedDirExcludes  []preProcessedExclusion
	retryCount               int
	retryWait                time.Duration
	modTimeWindow            time.Duration // The time window to consider file modification times equal.
	ioBufferPool             *sync.Pool    // pointer to avoid copying the noCopy field if the struct is ever passed by value

	// discoveredSrcPaths is a concurrent set populated by the syncWalker. It holds every
	// non-excluded path found in the source directory. During the mirror phase, it is
	// read to determine which paths in the destination are no longer present in the source
	// and should be deleted.
	// A ShardedSet is used to minimize lock contention, as the syncWalker will be
	// writing to it, while the handleMirror goroutine might be reading from it
	// (though in the current design, reads happen after all writes are complete).
	discoveredSrcPaths *sharded.ShardedSet

	// syncWg waits for syncWorkers to finish processing tasks.
	syncWg sync.WaitGroup

	// syncTasks is the channel where the Walker sends pre-processed tasks.
	syncTasks chan syncTask

	// syncErrs captures the first critical error from any worker to be reported at the end of the run.
	syncErrs chan error

	// ctx is the cancellable context for the entire run.
	ctx context.Context
}

// --- Helpers ---

// copyFileHelper handles the low-level details of copying a single file.
// It ensures atomicity by writing to a temporary file first and then renaming it.
func (r *nativeSyncRun) copyFileHelper(task *syncTask, retryCount int, retryWait time.Duration) error {
	var lastErr error
	for i := 0; i <= retryCount; i++ {
		if i > 0 {
			plog.Warn("Retrying file copy", "file", task.AbsPath, "attempt", fmt.Sprintf("%d/%d", i, retryCount), "after", retryWait)
			time.Sleep(retryWait)
		}

		lastErr = func() (err error) {
			src := task.AbsPath
			trg := filepath.Join(r.trg, task.RelPath)
			in, err := os.Open(src)
			if err != nil {
				return fmt.Errorf("failed to open source file %s: %w", src, err)
			}
			defer in.Close()

			trgDir := filepath.Dir(trg)

			// 2. Create a temporary file in the destination directory.
			out, err := os.CreateTemp(trgDir, "pgl-backup-*.tmp")
			if err != nil {
				return fmt.Errorf("failed to create temporary file in %s: %w", trgDir, err)
			}

			tempPath := out.Name()
			// Defer the removal of the temp file. If the rename succeeds, tempPath will be set to "",
			// making this a no-op. This prevents an error trying to remove a non-existent file.
			defer func() {
				if tempPath != "" {
					os.Remove(tempPath)
				}
			}()

			// Get a buffer from the pool for the copy operation.
			bufPtr := r.ioBufferPool.Get().(*[]byte)
			defer r.ioBufferPool.Put(bufPtr)

			// 3. Copy content
			if _, err = io.CopyBuffer(out, in, *bufPtr); err != nil {
				out.Close() // Close before returning on error, buffer is released by defer
				return fmt.Errorf("failed to copy content from %s to %s: %w", src, tempPath, err)
			}

			// 4. Copy file permissions
			if err := out.Chmod(task.FileInfo.Mode); err != nil {
				out.Close() // Close before returning on error
				return fmt.Errorf("failed to set permissions on temporary file %s: %w", tempPath, err)
			}

			// 5. Close the file.
			// This flushes data to disk. It MUST be done before Chtimes,
			// because closing/flushing might update the modification time.
			if err := out.Close(); err != nil {
				return fmt.Errorf("failed to close temporary file %s: %w", tempPath, err)
			}

			// 6. Copy file timestamps
			// We do this via os.Chtimes (using the path) after the file is closed.
			if err := os.Chtimes(tempPath, time.Unix(0, task.FileInfo.ModTime), time.Unix(0, task.FileInfo.ModTime)); err != nil {
				return fmt.Errorf("failed to set timestamps on %s: %w", tempPath, err)
			}

			// 7. Atomically move the temporary file to the final destination.
			if err := os.Rename(tempPath, trg); err != nil {
				return err
			}

			// 8. Clear tempPath to prevent the deferred os.Remove from running.
			tempPath = ""
			return nil
		}()

		if lastErr == nil {
			return nil // Success
		}
	}
	return fmt.Errorf("failed to copy file %s after %d retries: %w", task.AbsPath, retryCount, lastErr)
}

// preProcessExclusions analyzes and categorizes patterns to enable optimized matching later.
func preProcessExclusions(patterns []string, isDirPatterns bool) []preProcessedExclusion {
	preProcessed := make([]preProcessedExclusion, 0, len(patterns))
	for _, p := range patterns {
		// Normalize to use forward slashes for consistent matching logic.
		p = filepath.ToSlash(p)

		if strings.ContainsAny(p, "*?[]") {
			// If it's a prefix pattern like `node_modules/*`, we can optimize it.
			if strings.HasSuffix(p, "/*") {
				preProcessed = append(preProcessed, preProcessedExclusion{
					pattern:      p,
					cleanPattern: strings.TrimSuffix(p, "/*"),
					matchType:    prefixMatch,
				})
			} else if strings.HasPrefix(p, "*") && !strings.ContainsAny(p[1:], "*?[]") {
				// If it's a suffix pattern like `*.log`, we can also optimize it.
				preProcessed = append(preProcessed, preProcessedExclusion{
					pattern:      p,
					cleanPattern: p[1:], // The part after the *, e.g., ".log"
					matchType:    suffixMatch,
				})
			} else {
				// Otherwise, it's a general glob pattern.
				preProcessed = append(preProcessed, preProcessedExclusion{pattern: p, matchType: globMatch})
			}
		} else {
			// No wildcards. Check if it's a directory prefix or a literal match.
			// Refinement: If this is the directory exclusion list OR the pattern ends in a slash,
			// we treat it as a prefix match to exclude contents inside.
			if isDirPatterns || strings.HasSuffix(p, "/") {
				preProcessed = append(preProcessed, preProcessedExclusion{
					pattern:      p,
					cleanPattern: strings.TrimSuffix(p, "/"),
					matchType:    prefixMatch,
				})
			} else {
				// Pure literal file match (e.g., "README.md")
				preProcessed = append(preProcessed, preProcessedExclusion{pattern: p, matchType: literalMatch})
			}
		}
	}
	return preProcessed
}

// truncateModTime adjusts a time based on the configured modification time window.
func (r *nativeSyncRun) truncateModTime(t time.Time) time.Time {
	if r.modTimeWindow > 0 {
		return t.Truncate(r.modTimeWindow)
	}
	return t
}

// normalizedRelPath calculates the relative path and normalizes it to lowercase if case-insensitivity is enabled.
func (r *nativeSyncRun) normalizedRelPath(base, absPath string) (string, error) {
	relPath, err := filepath.Rel(base, absPath)
	if err != nil {
		return "", fmt.Errorf("failed to get relative path for %s: %w", absPath, err)
	}
	if r.caseInsensitive {
		relPath = strings.ToLower(relPath)
	}
	return relPath, nil
}

// withBackupWritePermission ensures that any directory/file permission has the owner-write
// bit (0200) set. This prevents the backup user from being locked out on subsequent runs.
func withBackupWritePermission(basePerm os.FileMode) os.FileMode {
	// Ensure the backup user always retains write permission.
	return basePerm | 0200
}

// isExcluded checks if a given relative path matches any of the exclusion patterns,
// using a tiered optimization strategy to avoid expensive glob matching when possible.
func (r *nativeSyncRun) isExcluded(relPath string, isDir bool) bool {
	// On Windows, WalkDir provides paths with `\`. We need to normalize them
	// to `/` for consistent matching with patterns.
	if filepath.Separator != '/' {
		relPath = filepath.ToSlash(relPath)
	}

	var patterns []preProcessedExclusion
	if isDir {
		patterns = r.preProcessedDirExcludes
	} else {
		patterns = r.preProcessedFileExcludes
	}

	for _, p := range patterns {
		switch p.matchType {
		case literalMatch:
			if relPath == p.pattern {
				return true
			}
		case prefixMatch:
			// Check 1: Exact match for the excluded directory/folder name (e.g., relPath == "build")
			if relPath == p.cleanPattern {
				return true
			}

			// Check 2: Match any file/dir inside the excluded folder (e.g., relPath starts with "build/")
			if strings.HasPrefix(relPath, p.cleanPattern+"/") {
				return true
			}
		case suffixMatch:
			// A pattern like "*.log" is cleaned to ".log". A simple suffix check is sufficient
			// because we only care if the path ends with this string. Unlike prefix matching,
			// there is no container/directory that needs a separate literal check.
			if strings.HasSuffix(relPath, p.cleanPattern) {
				return true
			}

		case globMatch:
			match, err := filepath.Match(p.pattern, relPath)
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
		plog.Info("[DRY RUN] COPY", "path", task.RelPath)
		return nil
	}

	trgAbsPath := filepath.Join(r.trg, task.RelPath)

	// Check if the destination file exists and if it matches source (size and mod time).
	// We use os.Lstat to get information about the file itself, not its target if it's a symlink.
	trgInfo, err := os.Lstat(trgAbsPath)
	if err == nil {
		// Destination path exists.
		if trgInfo.Mode().IsRegular() {
			// It's a regular file. Use the info from os.Lstat directly for comparison.
			// We skip the copy only if the modification times and sizes are identical.
			// We truncate the times to a configured window to handle filesystems with different timestamp resolutions.srcInfoModTime := time.Unix(0, task.SrcInfo.ModTime)
			if r.truncateModTime(time.Unix(0, task.FileInfo.ModTime)).Equal(r.truncateModTime(trgInfo.ModTime())) && task.FileInfo.Size == trgInfo.Size() {
				return nil // Not changed
			}
		} else {
			// The destination exists but is not a regular file (e.g., it's a directory, symlink, or other special file).
			// To ensure a consistent state, we must remove it before copying the source file.
			plog.Warn("Destination is not a regular file, removing before copy", "path", task.RelPath, "type", trgInfo.Mode().String())
			if err := os.RemoveAll(trgAbsPath); err != nil {
				return fmt.Errorf("failed to remove non-regular file at destination %s: %w", trgAbsPath, err)
			}
			// After removal, proceed to copy the file.
		}
	} else if !os.IsNotExist(err) {
		// An unexpected error occurred while Lstat-ing the destination.
		return fmt.Errorf("failed to lstat destination file %s: %w", trgAbsPath, err)
	}

	if err := r.copyFileHelper(task, r.retryCount, r.retryWait); err != nil {
		return fmt.Errorf("failed to copy file to %s: %w", trgAbsPath, err)
	}

	if !r.quiet {
		plog.Info("COPY", "path", task.RelPath)
	}
	return nil // File was actually copied/updated
}

// syncWalker is a dedicated goroutine that walks the source directory tree,
// sending each syncTask to the syncTasks channel for processing by workers.
func (r *nativeSyncRun) syncWalker() {
	defer close(r.syncTasks) // Close syncTasks to signal syncWorkers to stop when walk is complete

	err := filepath.WalkDir(r.src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// If we can't access a path, log the error but keep walking
			plog.Warn("Error accessing path, skipping", "path", path, "error", err)
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		relPath, err := r.normalizedRelPath(r.src, path)
		if err != nil {
			// This should not happen if path is from WalkDir on r.src
			plog.Warn("Could not get relative path, skipping", "path", path, "error", err.Error())
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// Check for exclusions.
		if r.isExcluded(relPath, d.IsDir()) {
			if !r.quiet {
				plog.Info("SKIP", "reason", "excluded by pattern", "path", relPath)
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
			plog.Warn("Failed to get file info, skipping", "path", path, "error", err)
			return nil
		}

		// --- CRITICAL: Record the path unconditionally here ---
		// If we mirror and the item exists in the source, it MUST be recorded in the set
		// to prevent it from being deleted during the mirror phase (Phase 2).
		if r.mirror {
			r.discoveredSrcPaths.Store(relPath)
		}
		// ----------------------------------------------------------------

		// The walker is the single producer and discovers directories sequentially.
		// By making it responsible for creating directories, we remove this task
		// and the associated synchronization overhead (syncedDirCache) from the
		// concurrent workers, simplifying their logic and improving performance.
		if d.IsDir() {
			// Directory
			expectedPerms := withBackupWritePermission(info.Mode().Perm())
			trgAbsPath := filepath.Join(r.trg, relPath)

			// Optimistic creation: Try Chmod first (cheapest syscall).
			// If it works, dir exists. If not, MkdirAll.
			// This avoids the internal Stat() loop of MkdirAll for existing directories.
			if err := os.Chmod(trgAbsPath, expectedPerms); err != nil {
				if os.IsNotExist(err) {
					if err := os.MkdirAll(trgAbsPath, expectedPerms); err != nil {
						plog.Warn("Failed to create destination directory, skipping", "path", trgAbsPath, "error", err)
						// Path is already recorded, but we can't descend.
						return filepath.SkipDir
					}
				} else {
					plog.Warn("Failed to set permissions on destination directory, skipping", "path", trgAbsPath, "error", err)
					// Error setting permissions, but the directory *exists* and is recorded.
					// We should NOT skip the directory here, as its contents might still sync fine.
					// ONLY skip if MkdirAll fails or if a non-MkdirAll error is critical (which is rare).
					// A Chmod failure on an existing dir should not typically cause a SkipDir.
					// The existing code uses SkipDir which is too aggressive.
					// We should only return an error to stop the walk if it's unrecoverable,
					// otherwise, return nil to continue walking the contents.
					return nil // We log the warning and continue descent.
				}
			}
			return nil // We've handled the directory
		} else if info.Mode().IsRegular() {
			// Regular File
			task := syncTask{
				AbsPath: path,
				RelPath: relPath,
				FileInfo: compactFileInfo{
					ModTime: info.ModTime().UnixNano(),
					Size:    info.Size(),
					Mode:    info.Mode(),
				},
			}

			select {
			case <-r.ctx.Done():
				return r.ctx.Err()
			case r.syncTasks <- task:
				return nil
			}
		} else {
			// Symlinks, Named Pipes, etc.
			if !r.quiet {
				plog.Info("SKIP", "type", info.Mode().String(), "path", relPath)
			}
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
		case task, ok := <-r.syncTasks:
			if !ok {
				// Channel closed by Walker, work is done.
				return
			}

			// Process the file sync
			err := r.processFileSync(&task)
			if err != nil {
				// If I/O fails, report the error non-blockingly.
				select {
				case r.syncErrs <- err:
				default:
				}
				plog.Warn("Sync failed for path; it will be preserved in the destination to prevent deletion",
					"path", task.RelPath,
					"error", err,
					"note", "This file may be inconsistent or outdated in the destination")
				// We rely on the syncWalker having already added this path to discoveredSrcPaths, preventing it from being deleted in the mirror phase.
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

// handleMirror performs a sequential walk on the destination to remove files
// and directories that do not exist in the source. This is only active in mirror mode.
func (r *nativeSyncRun) handleMirror() error {
	plog.Info("Starting mirror phase (deletions)")

	// WalkDir is efficient and allows us to skip trees we delete.
	err := filepath.WalkDir(r.trg, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// If path is already gone (e.g. parent deleted), just continue.
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to access path for deletion check: %w", err)
		}

		// Responsive cancellation check
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
		}

		relPath, err := r.normalizedRelPath(r.trg, path)
		if err != nil {
			return err // The helper function already wraps the error.
		}

		if relPath == "." {
			return nil
		}

		// Check if the destination path existed in the source.
		exists := r.discoveredSrcPaths.Has(relPath)
		if exists {
			// The path exists in the source, so we keep it.
			return nil
		}

		// The path is not in the source. Now we must check if it was excluded.
		// If it was excluded, we must NOT delete it.
		if r.isExcluded(relPath, d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir // It's an excluded dir, leave it and its contents alone.
			}
			return nil // It's an excluded file, leave it alone.
		}

		// The path is not in the source and is not excluded, so it must be deleted.
		if r.dryRun {
			plog.Info("[DRY RUN] DELETE", "path", relPath)
			// In dry run, we can't skip dir because we didn't actually delete it,
			// so we must visit children to log their deletion too.
			return nil
		}

		if !r.quiet {
			plog.Info("DELETE", "path", relPath)
		}

		// RemoveAll handles both files and directories recursively.
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to delete %s: %w", path, err)
		}

		// Optimization: Since we deleted the directory, don't bother
		// walking into it to check its children. They are gone.
		if d.IsDir() {
			return filepath.SkipDir
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("deletion phase failed: %w", err)
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
func (s *PathSyncer) handleNative(ctx context.Context, src, trg string, mirror bool, excludeFiles, excludeDirs []string) error {
	plog.Info("Starting native sync", "from", src, "to", trg)

	run := &nativeSyncRun{
		src:                      src,
		trg:                      trg,
		mirror:                   mirror,
		dryRun:                   s.dryRun,
		quiet:                    s.quiet,
		caseInsensitive:          runtime.GOOS == "windows" || runtime.GOOS == "darwin",
		numSyncWorkers:           s.engine.NativeEngineWorkers,
		preProcessedFileExcludes: preProcessExclusions(excludeFiles, false),
		preProcessedDirExcludes:  preProcessExclusions(excludeDirs, true),
		retryCount:               s.engine.NativeEngineRetryCount,
		retryWait:                time.Duration(s.engine.NativeEngineRetryWaitSeconds) * time.Second,
		modTimeWindow:            time.Duration(s.engine.NativeEngineModTimeWindowSeconds) * time.Second,
		ioBufferPool: &sync.Pool{
			New: func() interface{} {
				// Buffer size is configured in KB, so multiply by 1024.
				b := make([]byte, s.engine.NativeEngineCopyBufferSizeKB*1024)
				return &b
			},
		},
		discoveredSrcPaths: sharded.NewShardedSet(),
		// Buffer 'syncTasks' increase buffer size to absorb bursts of small files discovery by the walker.
		syncTasks: make(chan syncTask, s.engine.NativeEngineWorkers*100),
		syncErrs:  make(chan error, 1),
		ctx:       ctx,
	}

	return run.execute()
}
