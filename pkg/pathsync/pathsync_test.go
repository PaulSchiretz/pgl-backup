package pathsync

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
)

func TestSync_Dispatch(t *testing.T) {
	t.Run("Unknown Engine", func(t *testing.T) {
		srcDir := t.TempDir()
		baseDir := t.TempDir()

		plan := &Plan{
			Enabled: true,
			Engine:  Engine(99),
		}

		syncer := NewPathSyncer(256, 1, 1)
		_, err := syncer.Sync(context.Background(), baseDir, srcDir, "current", "content", plan, time.Now())

		if err == nil {
			t.Fatal("expected an error for unknown sync engine, but got nil")
		}
	})

	t.Run("Robocopy on Non-Windows OS", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("this test is for non-windows platforms only")
		}

		srcDir := t.TempDir()
		baseDir := t.TempDir()

		plan := &Plan{
			Engine: Robocopy,
		}

		syncer := NewPathSyncer(256, 1, 1)
		_, err := syncer.Sync(context.Background(), baseDir, srcDir, "current", "content", plan, time.Now())

		if err == nil {
			t.Fatal("expected an error when using robocopy on a non-windows OS, but got nil")
		}

		expectedError := "robocopy is not supported"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error to contain %q, but got: %v", expectedError, err)
		}
	})

	t.Run("Success Native Stores Result", func(t *testing.T) {
		srcDir := t.TempDir()
		baseDir := t.TempDir()

		// Create a dummy file in src so there is something to sync
		if err := os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("content"), 0644); err != nil {
			t.Fatal(err)
		}

		plan := &Plan{
			Enabled: true,
			Engine:  Native,
		}

		syncer := NewPathSyncer(256, 1, 1)
		relCurrent := "current"
		relContent := "content"
		timestamp := time.Now().UTC()

		result, err := syncer.Sync(context.Background(), baseDir, srcDir, relCurrent, relContent, plan, timestamp)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if result.RelPathKey != relCurrent {
			t.Errorf("expected ResultInfo.RelPathKey to be %q, got %q", relCurrent, result.RelPathKey)
		}
		if !result.Metadata.TimestampUTC.Equal(timestamp) {
			t.Errorf("expected ResultInfo.Metadata.TimestampUTC to be %v, got %v", timestamp, result.Metadata.TimestampUTC)
		}
	})
}

func TestSync_PreserveSourceDirName(t *testing.T) {
	srcDir := t.TempDir()
	baseDir := t.TempDir()

	// Create a dummy file in src so there is something to sync
	if err := os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}

	plan := &Plan{
		Enabled:               true,
		Engine:                Native,
		PreserveSourceDirName: true,
	}

	syncer := NewPathSyncer(256, 1, 1)
	relCurrent := "current"
	relContent := "content"
	timestamp := time.Now().UTC()

	_, err := syncer.Sync(context.Background(), baseDir, srcDir, relCurrent, relContent, plan, timestamp)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Calculate expected path: baseDir/current/content/<srcDirName>/test.txt
	srcDirName := filepath.Base(srcDir)
	expectedPath := filepath.Join(baseDir, relCurrent, relContent, srcDirName, "test.txt")

	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("expected file to exist at %s when PreserveSourceDirName is true", expectedPath)
	}
}

func TestRestore_NoMetafile(t *testing.T) {
	srcBase := t.TempDir()
	restoreTarget := t.TempDir()

	// Setup a mock backup structure: base/backup_name/content/file.txt
	backupName := "my_backup"
	relContentPath := "content"

	contentDir := filepath.Join(srcBase, backupName, relContentPath)
	if err := os.MkdirAll(contentDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(contentDir, "file.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	toRestore := metafile.MetafileInfo{
		RelPathKey: backupName,
	}

	plan := &Plan{
		Enabled:               true,
		Engine:                Native,
		PreserveSourceDirName: false, // Standard restore behavior
	}

	syncer := NewPathSyncer(256, 1, 1)

	// Act
	err := syncer.Restore(context.Background(), srcBase, relContentPath, toRestore, restoreTarget, plan)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Assert
	// 1. Content restored
	restoredFile := filepath.Join(restoreTarget, "file.txt")
	if _, err := os.Stat(restoredFile); os.IsNotExist(err) {
		t.Errorf("Restored file not found at %s", restoredFile)
	}

	// 2. No metafile in restore target
	metaPath := filepath.Join(restoreTarget, metafile.MetaFileName)
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Errorf("Metafile should NOT exist in restore target, but found at %s", metaPath)
	}
}

func TestResolveTargetDirectory(t *testing.T) {
	// Define a generic root for constructing absolute paths in tests
	root := "/tmp"
	if runtime.GOOS == "windows" {
		root = `C:\tmp`
	}
	targetBase := filepath.Join(root, "target")

	testCases := []struct {
		name                        string
		preserveSourceDirectoryName bool
		source                      string
		expectedTarget              string
	}{
		{
			name:                        "PreserveSourceDirectoryName is true",
			preserveSourceDirectoryName: true,
			source:                      filepath.Join(root, "src"),
			expectedTarget:              filepath.Join(targetBase, "src"),
		},
		{
			name:                        "PreserveSourceDirectoryName is false",
			preserveSourceDirectoryName: false,
			source:                      filepath.Join(root, "src"),
			expectedTarget:              targetBase,
		},
		{
			name:                        "PreserveSourceDirectoryName is true - Windows Drive Root",
			preserveSourceDirectoryName: true,
			source:                      "C:\\",
			expectedTarget:              filepath.Join(targetBase, "C"),
		},
		{
			name:                        "PreserveSourceDirectoryName is true - Unix Root",
			preserveSourceDirectoryName: true,
			source:                      "/",
			expectedTarget:              targetBase,
		},
		{
			name:                        "PreserveSourceDirectoryName is true - Relative Path",
			preserveSourceDirectoryName: true,
			source:                      "./my_relative_dir",
			expectedTarget:              filepath.Join(targetBase, "my_relative_dir"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Skip platform-specific tests on the wrong OS.
			if (runtime.GOOS != "windows" && strings.HasPrefix(tc.source, "C:\\")) ||
				(runtime.GOOS == "windows" && tc.source == "/") {
				t.Skipf("Skipping platform-specific test case %q on %s", tc.name, runtime.GOOS)
			}

			syncer := &PathSyncer{}
			got := syncer.resolveTargetDirectory(tc.source, targetBase, tc.preserveSourceDirectoryName)
			if got != tc.expectedTarget {
				t.Errorf("resolveTargetDirectory(%q, %q, %v) = %q; want %q", tc.source, targetBase, tc.preserveSourceDirectoryName, got, tc.expectedTarget)
			}
		})
	}
}
