package pathsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/pathsyncmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/util"
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
	info, err := os.Lstat(path)
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
	mirror                bool
	preserveSourceDirName bool
	dryRun                bool
	failFast              bool
	enableMetrics         bool
	excludeFiles          []string
	excludeDirs           []string
	srcFiles              []testFile
	srcDirs               []testDir
	dstFiles              []testFile
	dstDirs               []testDir
	modTimeWin            *int
	// Internal state
	baseDir string
	srcDir  string
}

func (r *nativeSyncTestRunner) setup() {
	artifactDir := r.t.ArtifactDir()
	r.srcDir = filepath.Join(artifactDir, "src")
	r.baseDir = filepath.Join(artifactDir, "base")

	if err := os.MkdirAll(r.srcDir, 0755); err != nil {
		r.t.Fatalf("failed to create src dir: %v", err)
	}

	if err := os.RemoveAll(r.baseDir); err != nil {
		r.t.Fatalf("failed to clean up dst dir before test: %v", err)
	}

	for _, f := range r.srcFiles {
		if f.symlinkTarget != "" {
			createSymlink(r.t, f.symlinkTarget, filepath.Join(r.srcDir, f.path))
		} else {
			createFile(r.t, filepath.Join(r.srcDir, f.path), f.content, f.modTime)
		}
	}
	for _, d := range r.srcDirs {
		createDir(r.t, filepath.Join(r.srcDir, d.path), d.perm, d.modTime)
	}
	for _, d := range r.dstDirs {
		createDir(r.t, filepath.Join(r.baseDir, d.path), d.perm, d.modTime)
	}
	for _, f := range r.dstFiles {
		if f.symlinkTarget != "" {
			createSymlink(r.t, f.symlinkTarget, filepath.Join(r.baseDir, f.path))
		} else {
			createFile(r.t, filepath.Join(r.baseDir, f.path), f.content, f.modTime)
		}
	}

	if !r.dryRun {
		os.MkdirAll(r.baseDir, 0755)
	}
}

type expectedMetrics struct {
	copied           int64
	deleted          int64 // filesDeleted
	excluded         int64 // filesExcluded
	upToDate         int64
	bytesWritten     int64
	dirsCreated      int64
	dirsDeleted      int64
	dirsExcluded     int64
	entriesProcessed int64
}

func TestNativeSync_EndToEnd(t *testing.T) {
	baseTime := time.Now().Add(-24 * time.Hour).Truncate(time.Minute)

	// --- Test Cases ---
	testCases := []struct {
		name                    string
		disabled                bool
		preserveSourceDirName   bool
		mirror                  bool
		dryRun                  bool
		failFast                bool
		enableMetrics           bool
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
		overwriteBehavior       OverwriteBehavior                   // Optional override for overwrite behavior.
		disableSafeCopy         bool                                // Optional override for safe copy.
	}{
		{
			name:                  "Simple Copy",
			mirror:                false,
			preserveSourceDirName: false,
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
			name:                  "Simple Copy (Safe Mode)",
			mirror:                false,
			preserveSourceDirName: false,
			disableSafeCopy:       false,
			srcFiles: []testFile{
				{path: "safe.txt", content: "safe content", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"safe.txt": {path: "safe.txt", content: "safe content", modTime: baseTime},
			},
		},
		{
			name:                  "Simple Copy (Direct Mode)",
			mirror:                false,
			preserveSourceDirName: false,
			disableSafeCopy:       true,
			srcFiles: []testFile{
				{path: "direct.txt", content: "direct content", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"direct.txt": {path: "direct.txt", content: "direct content", modTime: baseTime},
			},
		},
		{
			name:                  "Update File",
			mirror:                false,
			preserveSourceDirName: false,
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
			name:                  "Skip Unchanged File",
			mirror:                false,
			preserveSourceDirName: false,
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
			name:                  "Exact ModTime Match - Window 0",
			preserveSourceDirName: false,
			enableMetrics:         true,
			modTimeWindow:         new(int), // Set to 0
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
			name:                  "OverwriteIfNewer with ModTimeWindow",
			mirror:                false,
			preserveSourceDirName: false,
			modTimeWindow:         new(int(2)), // 2 seconds window
			overwriteBehavior:     OverwriteIfNewer,
			srcFiles: []testFile{
				// Source is 1.5s newer than base. With 2s window, they should truncate to same time.
				{path: "file.txt", content: "newer content", modTime: baseTime.Add(1500 * time.Millisecond)},
			},
			dstFiles: []testFile{
				{path: "file.txt", content: "older content", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"file.txt": {path: "file.txt", content: "older content", modTime: baseTime}, // Should NOT copy
			},
		},
		{
			name:                  "Sync Symlink",
			mirror:                false,
			preserveSourceDirName: false,
			srcFiles: []testFile{
				{path: "target.txt", content: "target content", modTime: baseTime},
				{path: "link.txt", symlinkTarget: "target.txt", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"target.txt": {path: "target.txt", content: "target content", modTime: baseTime},
				"link.txt":   {path: "link.txt", symlinkTarget: "target.txt", modTime: baseTime},
			},
		},
		{
			name:                  "Mirror Deletion",
			mirror:                true,
			preserveSourceDirName: false,
			dstFiles: []testFile{
				{path: "obsolete.txt", content: "delete me", modTime: baseTime},
				{path: "obsolete_dir/file.txt", content: "delete me too", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"obsolete.txt", "obsolete_dir"},
		},
		{
			name:                  "Exclude Files",
			mirror:                true,
			preserveSourceDirName: false,
			excludeFiles:          []string{"*.log", "temp.txt"},
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
			name:                  "Exclusion with literal and wildcard",
			mirror:                true,
			preserveSourceDirName: false,
			excludeFiles:          []string{"*.log", "temp.txt"},
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
			name:                  "Exclude Files with Suffix Pattern",
			mirror:                true,
			preserveSourceDirName: false,
			excludeFiles:          []string{"*.tmp"},
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
			name:                  "Exclude Dirs",
			mirror:                true,
			preserveSourceDirName: false,
			excludeDirs:           []string{"node_modules", "tmp"},
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
			name:                  "Exclude Dirs with Prefix Pattern",
			mirror:                true,
			preserveSourceDirName: false,
			excludeDirs:           []string{"build/"},
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
			name:                  "Exclude Dirs without Trailing Slash",
			mirror:                true,
			preserveSourceDirName: false,
			excludeDirs:           []string{"dist"},
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
			name:                  "Mirror Deletion - Keep Excluded File in Dest",
			mirror:                true,
			preserveSourceDirName: false,
			excludeFiles:          []string{"*.log"},
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
			name:                  "Dry Run - No Copy",
			mirror:                false,
			preserveSourceDirName: false,
			dryRun:                true,
			srcFiles: []testFile{
				{path: "file1.txt", content: "hello", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"file1.txt"},
		},
		{
			name:                  "Dry Run - No Deletion",
			mirror:                true,
			preserveSourceDirName: false,
			dryRun:                true,
			dstFiles: []testFile{
				{path: "obsolete.txt", content: "do not delete", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"obsolete.txt": {path: "obsolete.txt", content: "do not delete", modTime: baseTime},
			},
		},
		{
			name:                  "Directory Permission Sync",
			mirror:                false,
			preserveSourceDirName: false,
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
				expectedPerm := util.WithUserWritePermission(srcDirInfo.Mode().Perm())
				if expectedPerm != dstDirInfo.Mode().Perm() {
					t.Errorf("expected destination dir permissions to be %v, but got %v", expectedPerm, dstDirInfo.Mode().Perm())
				}
			},
		},
		{
			name:                  "Exclusion with Backslashes on Windows",
			mirror:                true,
			preserveSourceDirName: false,
			excludeFiles:          []string{`logs\app.log`}, // Use backslash in pattern
			excludeDirs:           []string{`vendor\`},      // Use backslash in pattern
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
			name:                  "Exclude Files Case Insensitive",
			mirror:                true,
			preserveSourceDirName: false,
			excludeFiles:          []string{"*.JPG"},
			srcFiles: []testFile{
				{path: "keep.png", content: "data", modTime: baseTime},
				{path: "ignore.jpg", content: "data", modTime: baseTime},
				{path: "IGNORE.JPG", content: "data", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"keep.png": {path: "keep.png", content: "data", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"ignore.jpg", "IGNORE.JPG"},
		},
		{
			name:                  "Case-Sensitive Mirror - Deletes Mismatched Case",
			mirror:                true,
			preserveSourceDirName: false,
			srcFiles: []testFile{
				// Source has only the upper-case version.
				{path: "Image.PNG", content: "new content", modTime: baseTime.Add(time.Hour)},
			},
			dstFiles: []testFile{
				// Destination has only the lower-case version.
				{path: "image.png", content: "old content", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				// The new file "Image.PNG" should be copied from the source.
				"Image.PNG": {path: "Image.PNG", content: "new content", modTime: baseTime.Add(time.Hour)},
			},
			// The old file "image.png" should be deleted by the mirror because it's not in the source.
			expectedMissingDstFiles: []string{"image.png"},
			verify: func(t *testing.T, src, dst string) {
				// The test framework already checks for existence/absence via the expected maps.
				// This is an explicit check for clarity.
				if !pathExists(t, filepath.Join(dst, "Image.PNG")) {
					t.Error("expected file 'Image.PNG' to exist in destination, but it does not")
				}
			},
		},
		{
			name:                  "Overwrite Destination Directory with File",
			mirror:                false,
			preserveSourceDirName: false,
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
			name:                  "Overwrite Destination File with Directory",
			mirror:                false,
			preserveSourceDirName: false,
			srcDirs: []testDir{
				{path: "conflict_dir", perm: 0755, modTime: baseTime},
			},
			srcFiles: []testFile{
				{path: "conflict_dir/file.txt", content: "content", modTime: baseTime},
			},
			dstFiles: []testFile{
				// Pre-create a FILE in the destination where a directory is expected.
				{path: "conflict_dir", content: "blocking file", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"conflict_dir/file.txt": {path: "conflict_dir/file.txt", content: "content", modTime: baseTime},
			},
			verify: func(t *testing.T, src, dst string) {
				info := getPathInfo(t, filepath.Join(dst, "conflict_dir"))
				if !info.IsDir() {
					t.Errorf("expected 'conflict_dir' to be a directory, but got mode %v", info.Mode())
				}
			},
		},
		{
			name:                  "Overwrite Destination Symlink with File",
			mirror:                false,
			preserveSourceDirName: false,
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
			name:                  "Overwrite Destination File with Symlink",
			mirror:                false,
			preserveSourceDirName: false,
			srcFiles: []testFile{
				{path: "link_to_create.txt", symlinkTarget: "target.txt", modTime: baseTime},
				{path: "target.txt", content: "target content", modTime: baseTime},
			},
			dstFiles: []testFile{
				// Pre-create a regular file in the destination.
				{path: "link_to_create.txt", content: "I am a regular file", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"link_to_create.txt": {path: "link_to_create.txt", symlinkTarget: "target.txt", modTime: baseTime},
				"target.txt":         {path: "target.txt", content: "target content", modTime: baseTime},
			},
			verify: func(t *testing.T, src, dst string) {
				// Verify that the destination item is now a symlink.
				info, err := os.Lstat(filepath.Join(dst, "link_to_create.txt"))
				if err != nil {
					t.Fatalf("failed to lstat destination item: %v", err)
				}
				if info.Mode()&os.ModeSymlink == 0 {
					t.Errorf("expected destination item to be a symlink, but it is %v", info.Mode())
				}
			},
		},
		{
			name:                  "Overwrite Destination File with Directory (Multiple)",
			preserveSourceDirName: false,
			srcFiles: []testFile{
				{path: "unwritable_dir/file1.txt", content: "content1", modTime: baseTime},
				{path: "unwritable_dir/file2.txt", content: "content2", modTime: baseTime},
				{path: "writable_dir/file3.txt", content: "content3", modTime: baseTime},
			},
			dstFiles: []testFile{
				// Pre-create a FILE in the destination where a directory is expected. The engine should fix this.
				{path: "unwritable_dir", content: "i am a file, not a directory", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"unwritable_dir/file1.txt": {path: "unwritable_dir/file1.txt", content: "content1", modTime: baseTime},
				"unwritable_dir/file2.txt": {path: "unwritable_dir/file2.txt", content: "content2", modTime: baseTime},
				// The sync for file3.txt should succeed.
				"writable_dir/file3.txt": {path: "writable_dir/file3.txt", content: "content3", modTime: baseTime},
			},
		},
		{
			name:                  "Overwrite Destination File with Directory (Fail-Fast Mode)",
			preserveSourceDirName: false,
			failFast:              true, // Should still succeed because conflict resolution handles this
			srcFiles: []testFile{
				// This file will cause the error.
				{path: filepath.Join("unwritable_dir", "file1.txt"), content: "content1", modTime: baseTime},
				// This file should NOT be processed because the creation of its parent dir will fail.
				{path: filepath.Join("unwritable_dir", "file2.txt"), content: "content2", modTime: baseTime},
			},
			dstFiles: []testFile{
				// Pre-create a FILE in the destination where a directory is expected.
				{path: "unwritable_dir", content: "i am a file, not a directory", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"unwritable_dir/file1.txt": {path: "unwritable_dir/file1.txt", content: "content1", modTime: baseTime},
				"unwritable_dir/file2.txt": {path: "unwritable_dir/file2.txt", content: "content2", modTime: baseTime},
			},
		},
		{
			name:                  "Metrics Counting",
			enableMetrics:         true,
			mirror:                true,
			preserveSourceDirName: false,
			excludeFiles:          []string{"*.log", "config.json"},
			excludeDirs:           []string{"ignored_dir"},
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
				copied:           2,  // dir1/new_file.txt, updated.txt
				deleted:          1,  // obsolete.txt
				excluded:         2,  // app.log, config.json
				upToDate:         1,  // uptodate.txt
				bytesWritten:     14, // "new" (3) + "new content" (11)
				dirsCreated:      1,  // dir1
				dirsDeleted:      1,  // obsolete_dir
				dirsExcluded:     1,  // ignored_dir
				entriesProcessed: 13, // 7 from sync + 6 from mirror
			},
		},
		{
			name:                  "Noop Metrics When Disabled",
			enableMetrics:         false, // Explicitly disable metrics
			mirror:                true,
			preserveSourceDirName: false,
			srcFiles: []testFile{
				{path: "file1.txt", content: "hello", modTime: baseTime},
			},
			dstFiles: []testFile{
				{path: "obsolete.txt", content: "delete me", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				"file1.txt": {path: "file1.txt", content: "hello", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"obsolete.txt"},
			verify: func(t *testing.T, src, dst string) {
				// This is the key assertion for this test.
				// We need to get the runner instance to inspect its state.
				// This is a bit of a test smell, but necessary for this kind of check.
				// The runner is populated by the test case loop below.
			},
			// Expect all metrics to be zero because NoopMetrics was used.
			expectedMetrics: &expectedMetrics{
				copied: 0, deleted: 0, excluded: 0, upToDate: 0,
				bytesWritten: 0,
				dirsCreated:  0, dirsDeleted: 0, dirsExcluded: 0,
				entriesProcessed: 0,
			},
		},
		{
			name:                  "No Mirror - Extra Files Preserved",
			mirror:                false, // Explicitly false
			preserveSourceDirName: false,
			srcFiles: []testFile{
				{path: "file1.txt", content: "hello", modTime: baseTime},
			},
			dstFiles: []testFile{
				// This file exists only in the destination.
				{path: "extra_file.txt", content: "should be preserved", modTime: baseTime},
			},
			expectedDstFiles: map[string]testFile{
				// The file from the source should be copied.
				"file1.txt": {path: "file1.txt", content: "hello", modTime: baseTime},
				// The extra file in the destination should remain untouched.
				"extra_file.txt": {path: "extra_file.txt", content: "should be preserved", modTime: baseTime},
			},
			// No files should be missing.
			expectedMissingDstFiles: nil,
		},
		{
			name:                  "Disabled Sync - Safety Check",
			disabled:              true,
			mirror:                false,
			preserveSourceDirName: false,
			srcFiles: []testFile{
				{path: "file1.txt", content: "hello", modTime: baseTime},
			},
			expectedMissingDstFiles: []string{"file1.txt"},
			expectedErrorContains:   "sync is disabled",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// --- Test-specific setup ---
			if tc.name == "Case-Sensitive Mirror - Deletes Mismatched Case" && util.IsHostCaseInsensitiveFS() {
				t.Skip("Skipping test that deletes mismatched case on a case-insensitive filesystem")
			}

			runner := &nativeSyncTestRunner{
				t:                     t,
				preserveSourceDirName: tc.preserveSourceDirName,
				mirror:                tc.mirror,
				dryRun:                tc.dryRun,
				failFast:              tc.failFast,
				enableMetrics:         tc.enableMetrics,
				excludeFiles:          tc.excludeFiles,
				excludeDirs:           tc.excludeDirs,
				srcFiles:              tc.srcFiles,
				srcDirs:               tc.srcDirs,
				dstFiles:              tc.dstFiles,
				dstDirs:               tc.dstDirs,
				modTimeWin:            tc.modTimeWindow,
			}
			runner.setup()

			plan := &Plan{
				Enabled:               !tc.disabled,
				Engine:                Native,
				PreserveSourceDirName: tc.preserveSourceDirName,
				Mirror:                tc.mirror,
				DisableSafeCopy:       tc.disableSafeCopy,
				RetryCount:            3,
				RetryWait:             5 * time.Second,
				ExcludeFiles:          tc.excludeFiles,
				ExcludeDirs:           tc.excludeDirs,
				DryRun:                tc.dryRun,
				FailFast:              tc.failFast,
				Metrics:               tc.enableMetrics,
				OverwriteBehavior:     tc.overwriteBehavior,
			}
			if tc.modTimeWindow != nil {
				plan.ModTimeWindow = time.Duration(*tc.modTimeWindow) * time.Second
			} else {
				plan.ModTimeWindow = 1 * time.Second
			}
			syncer := NewPathSyncer(256, 4, 4)
			_, err := syncer.Sync(context.Background(), runner.baseDir, runner.srcDir, "", "", plan, time.Now())

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

			// HACK: To inspect the metrics instance, we need to get the last run from the syncer.
			// This is a test-only pattern.
			var lastRunMetrics pathsyncmetrics.Metrics
			if !tc.disabled {
				if syncer.lastNativeTask == nil {
					t.Fatal("syncer.lastNativeTask was nil, cannot inspect test state")
				}
				lastRunMetrics = syncer.lastNativeTask.metrics
			}

			// Assert
			for relPathKey, expectedFile := range tc.expectedDstFiles {
				fullPath := filepath.Join(runner.baseDir, expectedFile.path)

				if expectedFile.symlinkTarget != "" {
					info, err := os.Lstat(fullPath)
					if err != nil {
						if os.IsNotExist(err) {
							t.Errorf("expected symlink to exist in destination: %s", expectedFile.path)
						} else {
							t.Fatalf("failed to lstat destination link: %v", err)
						}
						continue
					}
					if info.Mode()&os.ModeSymlink == 0 {
						t.Errorf("expected %s to be a symlink", relPathKey)
					}
					target, err := os.Readlink(fullPath)
					if err != nil {
						t.Fatalf("failed to read link target: %v", err)
					}
					if target != expectedFile.symlinkTarget {
						t.Errorf("expected link target %q, got %q", expectedFile.symlinkTarget, target)
					}
					continue
				}

				if !pathExists(t, fullPath) {
					t.Errorf("expected file to exist in destination: %s", expectedFile.path)
					continue
				}
				if content := getFileContent(t, fullPath); content != expectedFile.content {
					t.Errorf("expected content for %s to be %q, but got %q", relPathKey, expectedFile.content, content)
				}
				// For mod time comparison, use the same window as the syncer.
				window := plan.ModTimeWindow
				if tc.modTimeWindow != nil {
					window = time.Duration(*tc.modTimeWindow) * time.Second
				}

				modTime := getFileModTime(t, fullPath)
				expectedModTime := expectedFile.modTime

				if window > 0 && !modTime.Truncate(window).Equal(expectedModTime.Truncate(window)) || window == 0 && !modTime.Equal(expectedModTime) {
					t.Errorf("expected modTime for %s to be %v, but got %v", relPathKey, expectedFile.modTime, modTime)
				}
			}
			for _, p := range tc.expectedMissingDstFiles {
				if pathExists(t, filepath.Join(runner.baseDir, p)) {
					t.Errorf("expected path to be missing from destination: %s", p)
				}
			}
			// Allow for additional custom verification
			if tc.verify != nil {
				tc.verify(t, runner.srcDir, runner.baseDir)
			}

			// Verify metrics if provided
			if tc.expectedMetrics != nil {
				if lastRunMetrics == nil {
					t.Fatal("lastRunMetrics was nil, cannot verify metrics")
				}

				// If metrics were disabled, assert we got the NoopMetrics type.
				if !tc.enableMetrics {
					if _, ok := lastRunMetrics.(*pathsyncmetrics.NoopMetrics); !ok {
						t.Fatalf("expected metrics to be *metrics.NoopMetrics when disabled, but got %T", lastRunMetrics)
					}
				}

				// To check the values, we must have a *SyncMetrics instance.
				// This will be nil if metrics were disabled, and the checks will correctly fail.
				m, _ := lastRunMetrics.(*pathsyncmetrics.SyncMetrics)
				if m == nil && tc.enableMetrics {
					t.Fatalf("metrics were not of expected type *metrics.SyncMetrics, but %T", lastRunMetrics)
				}

				// If m is nil (because metrics were disabled), all .Load() calls will panic.
				// We need to handle this case.
				var copied, deleted, excluded, upToDate, bytesWritten, dirsCreated, dirsDeleted, dirsExcluded, entriesProcessed int64
				if m != nil {
					copied = m.FilesCopied.Load()
					deleted = m.FilesDeleted.Load()
					excluded = m.FilesExcluded.Load()
					upToDate = m.FilesUpToDate.Load()
					bytesWritten = m.BytesWritten.Load()
					dirsCreated = m.DirsCreated.Load()
					dirsDeleted = m.DirsDeleted.Load()
					dirsExcluded = m.DirsExcluded.Load()
					entriesProcessed = m.EntriesProcessed.Load()
				}

				if got := copied; got != tc.expectedMetrics.copied {
					t.Errorf("metric 'copied': expected %d, got %d", tc.expectedMetrics.copied, got)
				}
				if got := bytesWritten; got != tc.expectedMetrics.bytesWritten {
					t.Errorf("metric 'bytesWritten': expected %d, got %d", tc.expectedMetrics.bytesWritten, got)
				}
				if got := deleted; got != tc.expectedMetrics.deleted {
					t.Errorf("metric 'deleted': expected %d, got %d", tc.expectedMetrics.deleted, got)
				}
				if got := excluded; got != tc.expectedMetrics.excluded {
					t.Errorf("metric 'excluded': expected %d, got %d", tc.expectedMetrics.excluded, got)
				}
				if got := upToDate; got != tc.expectedMetrics.upToDate {
					t.Errorf("metric 'upToDate': expected %d, got %d", tc.expectedMetrics.upToDate, got)
				}
				if got := dirsCreated; got != tc.expectedMetrics.dirsCreated {
					t.Errorf("metric 'dirsCreated': expected %d, got %d", tc.expectedMetrics.dirsCreated, got)
				}
				if got := dirsDeleted; got != tc.expectedMetrics.dirsDeleted {
					t.Errorf("metric 'dirsDeleted': expected %d, got %d", tc.expectedMetrics.dirsDeleted, got)
				}
				if got := dirsExcluded; got != tc.expectedMetrics.dirsExcluded {
					t.Errorf("metric 'dirsExcluded': expected %d, got %d", tc.expectedMetrics.dirsExcluded, got)
				}
				if got := entriesProcessed; got != tc.expectedMetrics.entriesProcessed {
					t.Errorf("metric 'entriesProcessed': expected %d, got %d", tc.expectedMetrics.entriesProcessed, got)
				}
			}
		})
	}
}

func TestNativeSync_WorkerCancellation(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	srcDir := filepath.Join(tempDir, "src")
	baseDir := filepath.Join(tempDir, "base")

	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatalf("failed to create src dir: %v", err)
	}
	// Create many files to ensure the sync takes some time
	for i := 0; i < 1000; i++ {
		fname := filepath.Join(srcDir, fmt.Sprintf("file_%d.txt", i))
		if err := os.WriteFile(fname, []byte("content"), 0644); err != nil {
			t.Fatalf("failed to create file: %v", err)
		}
	}

	plan := &Plan{
		Enabled: true,
		Engine:  Native,
		Mirror:  true,
	}
	syncer := NewPathSyncer(256, 1, 1)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel shortly after starting
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	// Act
	_, err := syncer.Sync(ctx, baseDir, srcDir, "", "", plan, time.Now())

	// Assert
	if err != nil && !strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "critical sync error") {
		t.Errorf("expected context canceled error, got: %v", err)
	}
}

func TestNativeSync_MirrorCancellation(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	srcDir := filepath.Join(tempDir, "src")
	baseDir := filepath.Join(tempDir, "base")

	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatalf("failed to create src dir: %v", err)
	}
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		t.Fatalf("failed to create dst dir: %v", err)
	}

	// Create many files in DST that need to be deleted (Mirror phase work)
	// Src is empty, so Sync phase is fast.
	for i := 0; i < 1000; i++ {
		fname := filepath.Join(baseDir, fmt.Sprintf("file_to_delete_%d.txt", i))
		if err := os.WriteFile(fname, []byte("content"), 0644); err != nil {
			t.Fatalf("failed to create file: %v", err)
		}
	}

	plan := &Plan{
		Enabled: true,
		Engine:  Native,
		Mirror:  true,
	}
	syncer := NewPathSyncer(256, 1, 1)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel shortly after starting
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	// Act
	_, err := syncer.Sync(ctx, baseDir, srcDir, "", "", plan, time.Now())

	// Assert
	if err != nil && !strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "critical mirror error") {
		t.Errorf("expected context canceled error, got: %v", err)
	}
}

func TestNormalizeExclusionPattern(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple path",
			input:    "path/to/File.TXT",
			expected: "path/to/file.txt",
		},
		{
			name:     "Windows path",
			input:    `Path\To\Another\File.JPEG`,
			expected: "path/to/another/file.jpeg",
		},
		{
			name:     "Already normalized",
			input:    "already/normalized.log",
			expected: "already/normalized.log",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeExclusionPattern(tc.input); got != tc.expected {
				t.Errorf("normalizeExclusionPattern(%q) = %q; want %q", tc.input, got, tc.expected)
			}
		})
	}
}
