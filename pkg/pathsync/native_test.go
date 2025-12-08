package pathsync

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/sharded"
)

// helper to create a file with specific content and mod time.
func createFile(t *testing.T, path, content string, modTime time.Time) {
	t.Helper()
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("failed to create dir for test file: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("failed to set mod time for test file: %v", err)
	}
}

// helper to create a directory with specific permissions and mod time.
func createDir(t *testing.T, path string, perm os.FileMode, modTime time.Time) {
	t.Helper()
	// Create with default perms first, then apply specific ones to bypass umask.
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create dir for test: %v", err)
	}
	if err := os.Chmod(path, perm); err != nil {
		t.Fatalf("failed to set perms for test dir: %v", err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("failed to set mod time for test dir: %v", err)
	}
}

// helper to check if a path exists.
func pathExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	t.Fatalf("unexpected error checking path %s: %v", path, err)
	return false
}

// helper to get file content.
func getFileContent(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read file content from %s: %v", path, err)
	}
	return string(content)
}

// helper to get file mod time.
func getFileModTime(t *testing.T, path string) time.Time {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to get stat for %s: %v", path, err)
	}
	return info.ModTime()
}

// helper to get file/dir info.
func getPathInfo(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to get stat for %s: %v", path, err)
	}
	return info
}

// testFile defines a file to be created for a test case.
type testFile struct {
	path          string // This is the OS-specific path for file creation.
	content       string // For regular files
	modTime       time.Time
	symlinkTarget string // If non-empty, creates a symlink instead of a regular file
}

// helper to create a symlink.
func createSymlink(t *testing.T, oldname, newname string) {
	t.Helper()
	dir := filepath.Dir(newname)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("failed to create dir for test symlink: %v", err)
	}
	err := os.Symlink(oldname, newname)
	if err != nil {
		// On Windows, creating symlinks requires special privileges.
		// If the error indicates this, we skip the test gracefully.
		if runtime.GOOS == "windows" && strings.Contains(err.Error(), "A required privilege is not held by the client") {
			t.Skip("Skipping symlink test: creating symlinks on Windows requires administrator privileges or Developer Mode.")
		}
		t.Fatalf("failed to create symlink from %s to %s: %v", oldname, newname, err)
	}
}

// testDir defines a directory to be created for a test case.
type testDir struct {
	path    string
	perm    os.FileMode
	modTime time.Time
}

type nativeSyncTestRunner struct {
	t *testing.T
	// Inputs
	mirror       bool
	dryRun       bool
	failFast     bool
	excludeFiles []string
	excludeDirs  []string
	srcFiles     []testFile
	srcDirs      []testDir
	dstFiles     []testFile
	dstDirs      []testDir
	modTimeWin   *int
	// Internal state
	srcDir      string
	dstDir      string
	runInstance *syncRun
}

func (r *nativeSyncTestRunner) setup() {
	r.srcDir = r.t.TempDir()
	r.dstDir = r.t.TempDir()

	if err := os.RemoveAll(r.dstDir); err != nil {
		r.t.Fatalf("failed to clean up dst dir before test: %v", err)
	}

	for _, f := range r.srcFiles {
		createFile(r.t, filepath.Join(r.srcDir, f.path), f.content, f.modTime)
	}
	for _, d := range r.srcDirs {
		createDir(r.t, filepath.Join(r.srcDir, d.path), d.perm, d.modTime)
	}
	for _, d := range r.dstDirs {
		createDir(r.t, filepath.Join(r.dstDir, d.path), d.perm, d.modTime)
	}
	for _, f := range r.dstFiles {
		if f.symlinkTarget != "" {
			createSymlink(r.t, f.symlinkTarget, filepath.Join(r.dstDir, f.path))
		} else {
			createFile(r.t, filepath.Join(r.dstDir, f.path), f.content, f.modTime)
		}
	}

	if !r.dryRun {
		os.MkdirAll(r.dstDir, 0755)
	}
}

func (r *nativeSyncTestRunner) run() error {
	cfg := config.NewDefault()
	cfg.DryRun = r.dryRun
	cfg.FailFast = r.failFast
	cfg.Engine.Performance.SyncWorkers = 2
	cfg.Engine.Performance.CopyBufferSizeKB = 4
	cfg.Engine.RetryCount = 0
	syncer := NewPathSyncer(cfg)
	modTimeWindowSeconds := 1
	if r.modTimeWin != nil {
		modTimeWindowSeconds = *r.modTimeWin
	}
	syncer.engine.NativeEngineModTimeWindowSeconds = modTimeWindowSeconds

	r.runInstance = &syncRun{
		src:              r.srcDir,
		trg:              r.dstDir,
		mirror:           r.mirror,
		dryRun:           r.dryRun,
		failFast:         r.failFast,
		caseInsensitive:  isCaseInsensitiveFS(),
		fileExcludes:     preProcessExclusions(r.excludeFiles, false, isCaseInsensitiveFS()),
		dirExcludes:      preProcessExclusions(r.excludeDirs, true, isCaseInsensitiveFS()),
		numSyncWorkers:   syncer.engine.Performance.SyncWorkers,
		numMirrorWorkers: syncer.engine.Performance.MirrorWorkers,
		retryCount:       syncer.engine.RetryCount,
		retryWait:        time.Duration(syncer.engine.RetryWaitSeconds) * time.Second,
		modTimeWindow:    time.Duration(modTimeWindowSeconds) * time.Second,
		ioBufferPool: &sync.Pool{
			New: func() interface{} {
				b := make([]byte, syncer.engine.Performance.CopyBufferSizeKB*1024)
				return &b
			},
		},
		syncTaskPool: &sync.Pool{
			New: func() interface{} { return new(syncTask) },
		},
		mirrorTaskPool: &sync.Pool{
			New: func() interface{} { return new(mirrorTask) },
		},
		discoveredPaths:        sharded.NewShardedSet(),
		discoveredDirInfo:      sharded.NewShardedMap(),
		syncedDirCache:         sharded.NewShardedSet(),
		syncTasksChan:          make(chan *syncTask, syncer.engine.Performance.SyncWorkers*100),
		mirrorTasksChan:        make(chan *mirrorTask, syncer.engine.Performance.MirrorWorkers*100),
		criticalSyncErrsChan:   make(chan error, 1),
		syncErrs:               sharded.NewShardedMap(),
		criticalMirrorErrsChan: make(chan error, 1),
		mirrorErrs:             sharded.NewShardedMap(),
		ctx:                    context.Background(),
	}
	return r.runInstance.execute()
}

type expectedMetrics struct {
	copied       int64
	deleted      int64 // filesDeleted
	excluded     int64 // filesExcluded
	upToDate     int64
	dirsCreated  int64
	dirsDeleted  int64
	dirsExcluded int64
}

func TestNativeSync_EndToEnd(t *testing.T) {
	baseTime := time.Now().Add(-24 * time.Hour).Truncate(time.Second)

	// --- Test Cases ---
	testCases := []struct {
		name                    string
		mirror                  bool
		dryRun                  bool
		failFast                bool
		excludeFiles            []string
		excludeDirs             []string
		srcFiles                []testFile                          // Files to create in the source directory.
		srcDirs                 []testDir                           // Dirs with special metadata to create in source.
		dstDirs                 []testDir                           // Dirs to create in the destination directory.
		dstFiles                []testFile                          // Files to create in the destination directory.
		expectedDstFiles        map[string]testFile                 // Files that must exist in the destination after sync, keyed by normalized path.
		expectedMissingDstFiles []string                            // Paths that must NOT exist in the destination after sync.
		modTimeWindow           *int                                // Optional override for mod time window. If nil, uses default.
		verify                  func(t *testing.T, src, dst string) // Optional custom verification.
		expectedMetrics         *expectedMetrics                    // Optional metrics verification.
		expectedErrorContains   string                              // If non-empty, asserts that the sync error contains this string.
	}{
		{
			name:   "Simple Copy",
			mirror: false,
			srcFiles: []testFile{
				{path: "file1.txt", content: "hello", modTime: baseTime},
				{path: "subdir/file2.txt", content: "world", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"file1.txt":        {path: "file1.txt", content: "hello", modTime: baseTime},
				"subdir/file2.txt": {path: "subdir/file2.txt", content: "world", modTime: baseTime},
			},
		},
		{
			name:   "Update File",
			mirror: false,
			srcFiles: []testFile{
				{path: "file1.txt", content: "new content", modTime: baseTime.Add(time.Hour)},
			},
			dstFiles: []testFile{
				{path: "file1.txt", content: "old content", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"file1.txt": {path: "file1.txt", content: "new content", modTime: baseTime.Add(time.Hour)},
			},
		},
		{
			name:   "Skip Unchanged File",
			mirror: false,
			srcFiles: []testFile{
				{path: "file1.txt", content: "same", modTime: baseTime},
			},
			dstFiles: []testFile{
				{path: "file1.txt", content: "same", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"file1.txt": {path: "file1.txt", content: "same", modTime: baseTime},
			},
		},
		{
			name:          "Exact ModTime Match - Window 0",
			modTimeWindow: new(int), // Set to 0
			srcFiles: []testFile{
				// Source file with a high-precision timestamp.
				{path: "file.txt", content: "content", modTime: baseTime.Add(500 * time.Millisecond)},
			},
			dstFiles: []testFile{
				// Destination file with same content but slightly different time (within the 1s default window).
				{path: "file.txt", content: "content", modTime: baseTime.Add(600 * time.Millisecond)},
			},
			expectedDstFiles: map[string]testFile{
				// With a 0s window, the times are not equal, so the file MUST be copied.
				"file.txt": {path: "file.txt", content: "content", modTime: baseTime.Add(500 * time.Millisecond)},
			},
		},
		{
			name:   "Mirror Deletion",
			mirror: true,
			dstFiles: []testFile{
				{path: "obsolete.txt", content: "delete me", modTime: baseTime},
				{path: "obsolete_dir/file.txt", content: "delete me too", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"obsolete.txt", "obsolete_dir"},
		},
		{
			name:         "Exclude Files",
			mirror:       true,
			excludeFiles: []string{"*.log", "temp.txt"},
			srcFiles: []testFile{
				{path: "important.dat", content: "data", modTime: baseTime},
				{path: "app.log", content: "logging", modTime: baseTime},
				{path: "temp.txt", content: "temporary", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"important.dat": {path: "important.dat", content: "data", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"app.log", "temp.txt"},
		},
		{
			name:         "Exclusion with literal and wildcard",
			mirror:       true,
			excludeFiles: []string{"*.log", "temp.txt"},
			srcFiles: []testFile{
				{path: "important.dat", content: "data", modTime: baseTime},
				{path: "app.log", content: "logging", modTime: baseTime},
				{path: "temp.txt", content: "temporary", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"important.dat": {path: "important.dat", content: "data", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"app.log", "temp.txt"},
		},
		{
			name:         "Exclude Files with Suffix Pattern",
			mirror:       true,
			excludeFiles: []string{"*.tmp"},
			srcFiles: []testFile{
				{path: "document.txt", content: "content", modTime: baseTime},
				{path: "session.tmp", content: "temporary", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"document.txt": {path: "document.txt", content: "content", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"session.tmp"},
		},
		{
			name:        "Exclude Dirs",
			mirror:      true,
			excludeDirs: []string{"node_modules", "tmp"},
			srcFiles: []testFile{
				{path: "index.js", content: "code", modTime: baseTime},
				{path: "node_modules/lib.js", content: "library", modTime: baseTime},
				{path: "tmp/cache.dat", content: "cache", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"index.js": {path: "index.js", content: "code", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"node_modules", "tmp"},
		},
		{
			name:        "Exclude Dirs with Prefix Pattern",
			mirror:      true,
			excludeDirs: []string{"build/"},
			srcFiles: []testFile{
				{path: "index.html", content: "root file", modTime: baseTime},
				{path: "build/app.js", content: "should be excluded", modTime: baseTime},
				{path: "build/assets/icon.png", content: "should also be excluded", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"index.html": {path: "index.html", content: "root file", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"build"},
		},
		{
			name:        "Exclude Dirs without Trailing Slash",
			mirror:      true,
			excludeDirs: []string{"dist"},
			srcFiles: []testFile{
				{path: "index.html", content: "root file", modTime: baseTime},
				{path: "dist/bundle.js", content: "should be excluded", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"index.html": {path: "index.html", content: "root file", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"dist"},
		},
		{
			name:         "Mirror Deletion - Keep Excluded File in Dest",
			mirror:       true,
			excludeFiles: []string{"*.log"},
			dstFiles: []testFile{
				{path: "app.log", content: "existing log", modTime: baseTime},
				{path: "obsolete.txt", content: "delete me", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"app.log": {path: "app.log", content: "existing log", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"obsolete.txt"},
		},
		{
			name:   "Dry Run - No Copy",
			mirror: false,
			dryRun: true,
			srcFiles: []testFile{
				{path: "file1.txt", content: "hello", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"file1.txt"},
		},
		{
			name:   "Dry Run - No Deletion",
			mirror: true,
			dryRun: true,
			dstFiles: []testFile{
				{path: "obsolete.txt", content: "do not delete", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"obsolete.txt": {path: "obsolete.txt", content: "do not delete", modTime: baseTime},
			},
		},
		{
			name:   "Directory Permission Sync",
			mirror: false,
			srcDirs: []testDir{
				// Create a source dir with non-default permissions.
				{path: "special_dir", perm: 0700, modTime: baseTime.Add(-time.Hour)},
			},
			srcFiles: []testFile{
				// Add a file to ensure the directory is processed.
				{path: "special_dir/file.txt", content: "content", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"special_dir/file.txt": {path: "special_dir/file.txt", content: "content", modTime: baseTime},
			},
			verify: func(t *testing.T, src, dst string) {
				// This is the key assertion: verify the destination directory's permissions match the source.
				srcDirInfo := getPathInfo(t, filepath.Join(src, "special_dir"))
				dstDirInfo := getPathInfo(t, filepath.Join(dst, "special_dir"))

				// The expected permissions should include the backup write bit.
				expectedPerm := withBackupWritePermission(srcDirInfo.Mode().Perm())
				if expectedPerm != dstDirInfo.Mode().Perm() {
					t.Errorf("expected destination dir permissions to be %v, but got %v", expectedPerm, dstDirInfo.Mode().Perm())
				}
			},
		},
		{
			name:         "Exclusion with Backslashes on Windows",
			mirror:       true,
			excludeFiles: []string{`logs\app.log`}, // Use backslash in pattern
			excludeDirs:  []string{`vendor\`},      // Use backslash in pattern
			srcFiles: []testFile{
				{path: "main.go", content: "package main", modTime: baseTime},
				{path: filepath.Join("logs", "app.log"), content: "log data", modTime: baseTime},
				{path: filepath.Join("vendor", "lib", "library.go"), content: "lib code", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"main.go": {path: "main.go", content: "package main", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{
				filepath.Join("logs", "app.log"),
				"vendor",
			},
		},
		{
			name:   "Case Insensitive Mirror - Keep Mismatched Case",
			mirror: true,
			srcFiles: []testFile{
				{path: "Image.PNG", content: "new content", modTime: baseTime.Add(time.Hour)},
			},
			dstFiles: []testFile{
				{path: "image.png", content: "old content", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				// On case-insensitive systems, the destination file should be updated, not deleted and recreated.
				// The final casing might depend on the OS, but the content and time must match the source.
				// We check against the original destination path `image.png`.
				"image.png": {path: "image.png", content: "new content", modTime: baseTime.Add(time.Hour)},
			},
			expectedMissingDstFiles: []string{}, // Nothing should be deleted
			verify: func(t *testing.T, src, dst string) {
				// Crucially, verify that the differently-cased source file was NOT created.
				// On a case-insensitive OS, os.Stat("Image.PNG") would succeed even if only "image.png" exists.
				// We must read the directory to get the actual filename on disk.
				entries, err := os.ReadDir(dst)
				if err != nil {
					t.Fatalf("failed to read destination directory: %v", err)
				}
				if isCaseInsensitiveFS() {
					for _, entry := range entries {
						// On case-insensitive systems, we expect the original file 'image.png' to be updated in place.
						// A new file 'Image.PNG' should NOT be created.
						if entry.Name() == "Image.PNG" {
							t.Error("expected file 'Image.PNG' not to be created in destination, but it was")
						}
						if entry.Name() != "image.png" {
							t.Errorf("unexpected file found in destination: %s", entry.Name())
						}
					}
				} else {
					// On case-sensitive systems (like Linux), we expect a NEW file 'Image.PNG' to be created,
					// and the old 'image.png' to be deleted by the mirror.
					if !pathExists(t, filepath.Join(dst, "Image.PNG")) {
						t.Error("expected file 'Image.PNG' to be created on case-sensitive filesystem, but it was not")
					}
					// Also assert the old file is gone.
					if pathExists(t, filepath.Join(dst, "image.png")) {
						t.Error("expected file 'image.png' to be deleted by mirror on case-sensitive filesystem, but it still exists")
					}
				}
			},
		},
		{
			name:   "Overwrite Destination Directory with File",
			mirror: false,
			srcFiles: []testFile{
				{path: "item.txt", content: "this is a file", modTime: baseTime},
			},
			// Pre-create a directory in the destination with the same name as the source file.
			dstDirs: []testDir{
				{path: "item.txt", perm: 0755, modTime: baseTime},
			},
			// After the sync, this directory should be replaced by the file.
			expectedDstFiles: map[string]testFile{
				"item.txt": {path: "item.txt", content: "this is a file", modTime: baseTime},
			},
			verify: func(t *testing.T, src, dst string) {
				// Verify that the destination item is now a regular file.
				info := getPathInfo(t, filepath.Join(dst, "item.txt"))
				if !info.Mode().IsRegular() {
					t.Errorf("expected destination item to be a regular file, but it is %v", info.Mode())
				}
			},
		},
		{
			name:   "Overwrite Destination Symlink with File",
			mirror: false,
			srcFiles: []testFile{
				// This is a regular file that will be synced.
				{path: "file_to_sync.txt", content: "this is the real file", modTime: baseTime.Add(time.Hour)},
				// This is a file that the symlink in dst *could* point to, but it's irrelevant for the test.
				// It's here to ensure the symlink target doesn't cause issues if it exists.
				{path: "symlink_target_file.txt", content: "original target content", modTime: baseTime},
			},
			dstFiles: []testFile{
				// Pre-create a symlink in the destination with the same name as the source file.
				// It points to a dummy target (which may or may not exist).
				{path: "file_to_sync.txt", symlinkTarget: "dummy_symlink_target.txt", modTime: baseTime},
			},
			// After the sync, this symlink should be replaced by the regular file.
			expectedDstFiles: map[string]testFile{
				"file_to_sync.txt":        {path: "file_to_sync.txt", content: "this is the real file", modTime: baseTime.Add(time.Hour)},
				"symlink_target_file.txt": {path: "symlink_target_file.txt", content: "original target content", modTime: baseTime},
			},
			verify: func(t *testing.T, src, dst string) {
				// Verify that the destination item is now a regular file and not a symlink.
				info := getPathInfo(t, filepath.Join(dst, "file_to_sync.txt"))
				if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
					t.Errorf("expected destination item to be a regular file, but it is %v", info.Mode())
				}
			},
		},
		{
			name: "Error Aggregation for Multiple Failures",
			srcFiles: []testFile{
				{path: "unwritable_dir/file1.txt", content: "content1", modTime: baseTime},
				{path: "unwritable_dir/file2.txt", content: "content2", modTime: baseTime},
				{path: "writable_dir/file3.txt", content: "content3", modTime: baseTime},
			},
			dstFiles: []testFile{
				// Pre-create a FILE in the destination where a directory is expected.
				// This will cause an OS-agnostic "is not a directory" error when the sync tries to create the parent dir.
				{path: "unwritable_dir", content: "i am a file, not a directory", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				// The sync for file3.txt should succeed.
				"writable_dir/file3.txt": {path: "writable_dir/file3.txt", content: "content3", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{
				"unwritable_dir/file1.txt",
				"unwritable_dir/file2.txt",
			},
			// This test now expects a nil error, as non-critical errors are logged
			// but do not cause the sync to fail.
			expectedErrorContains: "",
		},
		{
			name:     "Fail-Fast on First Error",
			failFast: true,
			srcFiles: []testFile{
				// This file will cause the error.
				{path: filepath.Join("unwritable_dir", "file1.txt"), content: "content1", modTime: baseTime},
				// This file should NOT be processed because the creation of its parent dir will fail.
				{path: filepath.Join("unwritable_dir", "file2.txt"), content: "content2", modTime: baseTime},
			},
			dstFiles: []testFile{
				// Pre-create a FILE in the destination where a directory is expected.
				// This will cause ensureParentDirectoryExists to fail.
				{path: "unwritable_dir", content: "i am a file, not a directory", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				// The pre-existing file should still be there.
				"unwritable_dir": {path: "unwritable_dir", content: "i am a file, not a directory", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{
				filepath.Join("unwritable_dir", "file1.txt"), // This file should not have been synced.
				filepath.Join("unwritable_dir", "file2.txt"), // This file should not have been synced.
			},
			expectedErrorContains: "critical sync error", // The error should be wrapped as critical.
		},
		{
			name:         "Metrics Counting",
			mirror:       true,
			excludeFiles: []string{"*.log", "config.json"},
			excludeDirs:  []string{"ignored_dir"},
			srcFiles: []testFile{
				// 1. To be copied (new file)
				{path: filepath.Join("dir1", "new_file.txt"), content: "new", modTime: baseTime},
				// 2. To be updated (different content) -> counts as copied
				{path: "updated.txt", content: "new content", modTime: baseTime.Add(time.Hour)},
				// 3. To be up-to-date
				{path: "uptodate.txt", content: "same", modTime: baseTime},
				// 4. To be excluded (file by pattern)
				{path: "app.log", content: "logging", modTime: baseTime},
				// 5. To be excluded (file by name)
				{path: "config.json", content: "secret", modTime: baseTime},
				// 6. Not counted (inside excluded dir)
				{path: filepath.Join("ignored_dir", "some_file.txt"), content: "should not be seen", modTime: baseTime},
			},
			dstFiles: []testFile{
				// File to be updated
				{path: "updated.txt", content: "old content", modTime: baseTime},
				// File that is already up-to-date
				{path: "uptodate.txt", content: "same", modTime: baseTime},
				// File to be deleted
				{path: "obsolete.txt", content: "delete me", modTime: baseTime},
			},
			dstDirs: []testDir{
				// Directory to be deleted
				{path: "obsolete_dir", perm: 0755, modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				filepath.Join("dir1", "new_file.txt"): {path: filepath.Join("dir1", "new_file.txt"), content: "new", modTime: baseTime},
				"updated.txt":                         {path: "updated.txt", content: "new content", modTime: baseTime.Add(time.Hour)},
				"uptodate.txt":                        {path: "uptodate.txt", content: "same", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"obsolete.txt", "obsolete_dir", "app.log", "config.json", "ignored_dir"},
			expectedMetrics: &expectedMetrics{
				copied:       2, // dir1/new_file.txt, updated.txt
				deleted:      1, // obsolete.txt
				excluded:     2, // app.log, config.json
				upToDate:     1, // uptodate.txt
				dirsCreated:  1, // dir1
				dirsDeleted:  1, // obsolete_dir
				dirsExcluded: 1, // ignored_dir
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// --- Test-specific setup ---
			if tc.name == "Case Insensitive Mirror - Keep Mismatched Case" {
				if runtime.GOOS != "windows" && runtime.GOOS != "darwin" {
					t.Skip("Skipping case-insensitive test on case-sensitive OS")
				}
			}

			runner := &nativeSyncTestRunner{
				t:            t,
				mirror:       tc.mirror,
				dryRun:       tc.dryRun,
				failFast:     tc.failFast,
				excludeFiles: tc.excludeFiles,
				excludeDirs:  tc.excludeDirs,
				srcFiles:     tc.srcFiles,
				srcDirs:      tc.srcDirs,
				dstFiles:     tc.dstFiles,
				dstDirs:      tc.dstDirs,
				modTimeWin:   tc.modTimeWindow,
			}
			runner.setup()
			err := runner.run()

			// Assert on error
			if tc.expectedErrorContains != "" {
				if err == nil {
					t.Fatalf("expected an error containing %q, but got nil", tc.expectedErrorContains)
				}
				if !strings.Contains(err.Error(), tc.expectedErrorContains) {
					t.Fatalf("expected error to contain %q, but got: %v", tc.expectedErrorContains, err)
				}
			} else if err != nil {
				t.Fatalf("handleNative failed unexpectedly: %v", err)
			}

			// Assert
			for relPathKey, expectedFile := range tc.expectedDstFiles {
				fullPath := filepath.Join(runner.dstDir, expectedFile.path)
				if !pathExists(t, fullPath) {
					t.Errorf("expected file to exist in destination: %s", expectedFile.path)
					continue
				}
				if content := getFileContent(t, fullPath); content != expectedFile.content {
					t.Errorf("expected content for %s to be %q, but got %q", relPathKey, expectedFile.content, content)
				}
				// For mod time comparison, use the same window as the syncer.
				window := runner.runInstance.modTimeWindow
				modTime := getFileModTime(t, fullPath)
				expectedModTime := expectedFile.modTime

				if window > 0 && !modTime.Truncate(window).Equal(expectedModTime.Truncate(window)) || window == 0 && !modTime.Equal(expectedModTime) {
					t.Errorf("expected modTime for %s to be %v, but got %v", relPathKey, expectedFile.modTime, modTime)
				}
			}
			for _, p := range tc.expectedMissingDstFiles {
				if pathExists(t, filepath.Join(runner.dstDir, p)) {
					t.Errorf("expected path to be missing from destination: %s", p)
				}
			}
			// Allow for additional custom verification
			if tc.verify != nil {
				tc.verify(t, runner.srcDir, runner.dstDir)
			}

			// Verify metrics if provided
			if tc.expectedMetrics != nil {
				metrics := &runner.runInstance.metrics
				if got := metrics.filesCopied.Load(); got != tc.expectedMetrics.copied {
					t.Errorf("metric 'copied': expected %d, got %d", tc.expectedMetrics.copied, got)
				}
				if got := metrics.filesDeleted.Load(); got != tc.expectedMetrics.deleted {
					t.Errorf("metric 'deleted': expected %d, got %d", tc.expectedMetrics.deleted, got)
				}
				if got := metrics.filesExcluded.Load(); got != tc.expectedMetrics.excluded {
					t.Errorf("metric 'excluded': expected %d, got %d", tc.expectedMetrics.excluded, got)
				}
				if got := metrics.filesUpToDate.Load(); got != tc.expectedMetrics.upToDate {
					t.Errorf("metric 'upToDate': expected %d, got %d", tc.expectedMetrics.upToDate, got)
				}
				if got := metrics.dirsCreated.Load(); got != tc.expectedMetrics.dirsCreated {
					t.Errorf("metric 'dirsCreated': expected %d, got %d", tc.expectedMetrics.dirsCreated, got)
				}
				if got := metrics.dirsDeleted.Load(); got != tc.expectedMetrics.dirsDeleted {
					t.Errorf("metric 'dirsDeleted': expected %d, got %d", tc.expectedMetrics.dirsDeleted, got)
				}
				if got := metrics.dirsExcluded.Load(); got != tc.expectedMetrics.dirsExcluded {
					t.Errorf("metric 'dirsExcluded': expected %d, got %d", tc.expectedMetrics.dirsExcluded, got)
				}
			}
		})
	}
}
