package pathsync

import (
	"context"
	"os"
	"path/filepath"
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

func TestNativeSync_EndToEnd(t *testing.T) {
	baseTime := time.Now().Add(-24 * time.Hour).Truncate(time.Second)

	// --- Test Cases ---
	testCases := []struct {
		name         string
		mirror       bool
		dryRun       bool
		excludeFiles []string
		excludeDirs  []string
		setup        func(t *testing.T, src, dst string) // Setup source and destination dirs
		verify       func(t *testing.T, src, dst string) // Verify state of destination dir
	}{
		{
			name:   "Simple Copy",
			mirror: false,
			setup: func(t *testing.T, src, dst string) {
				createFile(t, filepath.Join(src, "file1.txt"), "hello", baseTime)
				createFile(t, filepath.Join(src, "subdir", "file2.txt"), "world", baseTime)
			},
			verify: func(t *testing.T, src, dst string) {
				if !pathExists(t, filepath.Join(dst, "file1.txt")) {
					t.Error("expected file1.txt to be copied")
				}
				if !pathExists(t, filepath.Join(dst, "subdir", "file2.txt")) {
					t.Error("expected subdir/file2.txt to be copied")
				}
			},
		},
		{
			name:   "Update File",
			mirror: false,
			setup: func(t *testing.T, src, dst string) {
				// Source is newer
				createFile(t, filepath.Join(src, "file1.txt"), "new content", baseTime.Add(time.Hour))
				// Destination is older
				createFile(t, filepath.Join(dst, "file1.txt"), "old content", baseTime)
			},
			verify: func(t *testing.T, src, dst string) {
				dstFile := filepath.Join(dst, "file1.txt")
				if content := getFileContent(t, dstFile); content != "new content" {
					t.Errorf("expected file content to be updated, got %q", content)
				}
				// Truncate to second for comparison as some filesystems have lower precision.
				if modTime := getFileModTime(t, dstFile).Truncate(time.Second); !modTime.Equal(baseTime.Add(time.Hour)) {
					t.Errorf("expected file mod time to be updated, got %v", modTime)
				}
			},
		},
		{
			name:   "Skip Unchanged File",
			mirror: false,
			setup: func(t *testing.T, src, dst string) {
				// Both files are identical
				createFile(t, filepath.Join(src, "file1.txt"), "same", baseTime)
				createFile(t, filepath.Join(dst, "file1.txt"), "same", baseTime)
			},
			verify: func(t *testing.T, src, dst string) {
				// The mod time should not have changed if the file was correctly skipped.
				dstFile := filepath.Join(dst, "file1.txt")
				if modTime := getFileModTime(t, dstFile); !modTime.Equal(baseTime) {
					t.Errorf("expected file to be skipped, but mod time changed to %v", modTime)
				}
			},
		},
		{
			name:   "Mirror Deletion",
			mirror: true,
			setup: func(t *testing.T, src, dst string) {
				// Source is empty
				// Destination has files to be deleted
				createFile(t, filepath.Join(dst, "obsolete.txt"), "delete me", baseTime)
				createFile(t, filepath.Join(dst, "obsolete_dir", "file.txt"), "delete me too", baseTime)
			},
			verify: func(t *testing.T, src, dst string) {
				if pathExists(t, filepath.Join(dst, "obsolete.txt")) {
					t.Error("expected obsolete.txt to be deleted")
				}
				if pathExists(t, filepath.Join(dst, "obsolete_dir")) {
					t.Error("expected obsolete_dir to be deleted")
				}
			},
		},
		{
			name:         "Exclude Files",
			mirror:       true,
			excludeFiles: []string{"*.log", "temp.txt"},
			setup: func(t *testing.T, src, dst string) {
				createFile(t, filepath.Join(src, "important.dat"), "data", baseTime)
				createFile(t, filepath.Join(src, "app.log"), "logging", baseTime)
				createFile(t, filepath.Join(src, "temp.txt"), "temporary", baseTime)
			},
			verify: func(t *testing.T, src, dst string) {
				if !pathExists(t, filepath.Join(dst, "important.dat")) {
					t.Error("expected important.dat to be copied")
				}
				if pathExists(t, filepath.Join(dst, "app.log")) {
					t.Error("expected app.log to be excluded")
				}
				if pathExists(t, filepath.Join(dst, "temp.txt")) {
					t.Error("expected temp.txt to be excluded")
				}
			},
		},
		{
			name:        "Exclude Dirs",
			mirror:      true,
			excludeDirs: []string{"node_modules", "tmp"},
			setup: func(t *testing.T, src, dst string) {
				createFile(t, filepath.Join(src, "index.js"), "code", baseTime)
				createFile(t, filepath.Join(src, "node_modules", "lib.js"), "library", baseTime)
				createFile(t, filepath.Join(src, "tmp", "cache.dat"), "cache", baseTime)
			},
			verify: func(t *testing.T, src, dst string) {
				if !pathExists(t, filepath.Join(dst, "index.js")) {
					t.Error("expected index.js to be copied")
				}
				if pathExists(t, filepath.Join(dst, "node_modules")) {
					t.Error("expected node_modules to be excluded")
				}
				if pathExists(t, filepath.Join(dst, "tmp")) {
					t.Error("expected tmp to be excluded")
				}
			},
		},
		{
			name:         "Mirror Deletion - Keep Excluded File in Dest",
			mirror:       true,
			excludeFiles: []string{"*.log"},
			setup: func(t *testing.T, src, dst string) {
				// This file exists only in the destination and should be preserved due to exclusion.
				createFile(t, filepath.Join(dst, "app.log"), "existing log", baseTime)
				// This file exists only in the destination and should be deleted.
				createFile(t, filepath.Join(dst, "obsolete.txt"), "delete me", baseTime)
			},
			verify: func(t *testing.T, src, dst string) {
				// Verify the excluded file was NOT deleted from destination
				if !pathExists(t, filepath.Join(dst, "app.log")) {
					t.Error("expected excluded file app.log to be preserved in destination")
				}
				// Verify the non-excluded, obsolete file WAS deleted
				if pathExists(t, filepath.Join(dst, "obsolete.txt")) {
					t.Error("expected obsolete.txt to be deleted from destination")
				}
			},
		},
		{
			name:   "Dry Run - No Copy",
			mirror: false,
			dryRun: true,
			setup: func(t *testing.T, src, dst string) {
				createFile(t, filepath.Join(src, "file1.txt"), "hello", baseTime)
			},
			verify: func(t *testing.T, src, dst string) {
				if pathExists(t, dst) {
					// We check if the whole dst dir exists, because it shouldn't even be created.
					files, _ := os.ReadDir(dst)
					if len(files) > 0 {
						t.Error("expected destination to be empty in dry run")
					}
				}
			},
		},
		{
			name:   "Dry Run - No Deletion",
			mirror: true,
			dryRun: true,
			setup: func(t *testing.T, src, dst string) {
				// Source is empty
				createFile(t, filepath.Join(dst, "obsolete.txt"), "do not delete", baseTime)
			},
			verify: func(t *testing.T, src, dst string) {
				if !pathExists(t, filepath.Join(dst, "obsolete.txt")) {
					t.Error("expected obsolete.txt to NOT be deleted in dry run")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			srcDir := t.TempDir()
			dstDir := t.TempDir()

			// The destination directory might be created by the sync process,
			// so we remove it first to ensure a clean state for tests that need it.
			if err := os.RemoveAll(dstDir); err != nil {
				t.Fatalf("failed to clean up dst dir before test: %v", err)
			}

			// Setup initial state
			tc.setup(t, srcDir, dstDir)

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
			tc.verify(t, srcDir, dstDir)
		})
	}
}
