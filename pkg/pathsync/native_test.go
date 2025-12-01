package pathsync

import (
	"context"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
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
	path    string
	content string
	modTime time.Time
}

// testDir defines a directory to be created for a test case.
type testDir struct {
	path    string
	perm    os.FileMode
	modTime time.Time
}

func TestNativeSync_EndToEnd(t *testing.T) {
	baseTime := time.Now().Add(-24 * time.Hour).Truncate(time.Second)

	// --- Test Cases ---
	testCases := []struct {
		name                    string
		mirror                  bool
		dryRun                  bool
		excludeFiles            []string
		excludeDirs             []string
		srcFiles                []testFile                          // Files to create in the source directory.
		srcDirs                 []testDir                           // Dirs with special metadata to create in source.
		dstFiles                []testFile                          // Files to create in the destination directory.
		expectedDstFiles        []testFile                          // Files that must exist in the destination after sync.
		expectedMissingDstFiles []string                            // Paths that must NOT exist in the destination after sync.
		verify                  func(t *testing.T, src, dst string) // Optional custom verification.
	}{
		{
			name:   "Simple Copy",
			mirror: false,
			srcFiles: []testFile{
				{path: "file1.txt", content: "hello", modTime: baseTime},
				{path: "subdir/file2.txt", content: "world", modTime: baseTime},
			},
			expectedDstFiles: []testFile{
				{path: "file1.txt", content: "hello", modTime: baseTime},
				{path: "subdir/file2.txt", content: "world", modTime: baseTime},
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
			expectedDstFiles: []testFile{
				{path: "file1.txt", content: "new content", modTime: baseTime.Add(time.Hour)},
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
			expectedDstFiles: []testFile{
				{path: "file1.txt", content: "same", modTime: baseTime},
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
			expectedDstFiles: []testFile{
				{path: "important.dat", content: "data", modTime: baseTime},
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
			expectedDstFiles: []testFile{
				{path: "important.dat", content: "data", modTime: baseTime},
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
			expectedDstFiles: []testFile{
				{path: "document.txt", content: "content", modTime: baseTime},
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
			expectedDstFiles: []testFile{
				{path: "index.js", content: "code", modTime: baseTime},
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
			expectedDstFiles: []testFile{
				{path: "index.html", content: "root file", modTime: baseTime},
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
			expectedDstFiles: []testFile{
				{path: "index.html", content: "root file", modTime: baseTime},
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
			expectedDstFiles: []testFile{
				{path: "app.log", content: "existing log", modTime: baseTime},
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
			expectedDstFiles: []testFile{
				{path: "obsolete.txt", content: "do not delete", modTime: baseTime},
			},
		},
		{
			name:   "Directory Metadata Sync",
			mirror: false,
			srcDirs: []testDir{
				// Create a source dir with non-default permissions and a specific time.
				{path: "special_dir", perm: 0700, modTime: baseTime.Add(-time.Hour)},
			},
			srcFiles: []testFile{
				// This file will be created inside the special dir, which in a naive implementation
				// would overwrite the parent's modTime.
				{path: "special_dir/file.txt", content: "content", modTime: baseTime},
			},
			expectedDstFiles: []testFile{
				{path: "special_dir/file.txt", content: "content", modTime: baseTime},
			},
			verify: func(t *testing.T, src, dst string) {
				// This is the key assertion: verify the destination directory's metadata
				// matches the source, proving the two-phase sync worked.
				srcDirInfo := getPathInfo(t, filepath.Join(src, "special_dir"))
				dstDirInfo := getPathInfo(t, filepath.Join(dst, "special_dir"))

				// Skip permission check if running as non-root on Unix, as setting 0700 might not be possible
				// for a directory owned by another user in the temp space.
				u, err := user.Current()
				if err == nil && u.Uid == "0" {
					if srcDirInfo.Mode().Perm() != dstDirInfo.Mode().Perm() {
						t.Errorf("expected destination dir permissions to be %v, but got %v", srcDirInfo.Mode().Perm(), dstDirInfo.Mode().Perm())
					}
				}
				if !srcDirInfo.ModTime().Truncate(time.Second).Equal(dstDirInfo.ModTime().Truncate(time.Second)) {
					t.Errorf("expected destination dir modTime to be %v, but got %v", srcDirInfo.ModTime(), dstDirInfo.ModTime())
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
			expectedDstFiles: []testFile{
				{path: "main.go", content: "package main", modTime: baseTime},
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
			expectedDstFiles: []testFile{
				// On case-insensitive systems, the destination file should be updated, not deleted and recreated.
				// The final casing might depend on the OS, but the content and time must match the source.
				// We check against the original destination path `image.png`.
				{path: "image.png", content: "new content", modTime: baseTime.Add(time.Hour)},
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
				for _, entry := range entries {
					// Check if a file with the source's casing was created.
					// We expect only "image.png" to exist.
					if entry.Name() == "Image.PNG" {
						t.Error("expected file 'Image.PNG' not to be created in destination, but it was")
					}
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// --- Test-specific setup ---
			if tc.name == "Exclusion with Backslashes on Windows" {
				if runtime.GOOS != "windows" {
					t.Skip("Skipping Windows-specific backslash test on non-windows OS")
				}
			}
			// --- Test-specific setup ---
			if tc.name == "Case Insensitive Mirror - Keep Mismatched Case" {
				if runtime.GOOS != "windows" && runtime.GOOS != "darwin" {
					t.Skip("Skipping case-insensitive test on case-sensitive OS")
				}
			}

			// Arrange
			srcDir := t.TempDir()
			dstDir := t.TempDir()

			// The destination directory might be created by the sync process,
			// so we remove it first to ensure a clean state for tests that need it.
			if err := os.RemoveAll(dstDir); err != nil {
				t.Fatalf("failed to clean up dst dir before test: %v", err)
			}

			// Setup initial state from file matrices
			for _, f := range tc.srcFiles {
				createFile(t, filepath.Join(srcDir, f.path), f.content, f.modTime)
			}
			for _, d := range tc.srcDirs {
				createDir(t, filepath.Join(srcDir, d.path), d.perm, d.modTime)
			}
			for _, f := range tc.dstFiles {
				createFile(t, filepath.Join(dstDir, f.path), f.content, f.modTime)
			}

			// In the real app, `validateSyncPaths` creates the destination directory
			// before `handleNative` is called. We must simulate that here to prevent
			// a race condition in the test environment.
			if !tc.dryRun {
				os.MkdirAll(dstDir, 0755)
			}

			// Create the syncer
			cfg := config.NewDefault()
			cfg.DryRun = tc.dryRun
			cfg.Engine.NativeEngineWorkers = 2 // Use a small number of workers for tests
			cfg.Engine.NativeEngineRetryCount = 0
			syncer := NewPathSyncer(cfg)

			// Act
			err := syncer.handleNative(context.Background(), srcDir, dstDir, tc.mirror, tc.excludeFiles, tc.excludeDirs)
			if err != nil {
				t.Fatalf("handleNative failed: %v", err)
			}

			// Assert
			for _, f := range tc.expectedDstFiles {
				fullPath := filepath.Join(dstDir, f.path)
				if !pathExists(t, fullPath) {
					t.Errorf("expected file to exist in destination: %s", f.path)
					continue
				}
				if content := getFileContent(t, fullPath); content != f.content {
					t.Errorf("expected content for %s to be %q, but got %q", f.path, f.content, content)
				}
				if modTime := getFileModTime(t, fullPath).Truncate(time.Second); !modTime.Equal(f.modTime.Truncate(time.Second)) {
					t.Errorf("expected modTime for %s to be %v, but got %v", f.path, f.modTime, modTime)
				}
			}
			for _, p := range tc.expectedMissingDstFiles {
				if pathExists(t, filepath.Join(dstDir, p)) {
					t.Errorf("expected path to be missing from destination: %s", p)
				}
			}
			// Allow for additional custom verification
			if tc.verify != nil {
				tc.verify(t, srcDir, dstDir)
			}
		})
	}
}
