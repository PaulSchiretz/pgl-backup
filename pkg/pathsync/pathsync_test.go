package pathsync

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSync_Dispatch(t *testing.T) {
	t.Run("Unknown Engine", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := t.TempDir()

		plan := &Plan{
			Engine: Engine(99),
		}

		syncer := NewPathSyncer(256, 1, 1)
		err := syncer.Sync(context.Background(), srcDir, dstDir, "current", "content", plan, time.Now())

		if err == nil {
			t.Fatal("expected an error for unknown sync engine, but got nil")
		}
	})

	t.Run("Robocopy on Non-Windows OS", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("this test is for non-windows platforms only")
		}

		srcDir := t.TempDir()
		dstDir := t.TempDir()

		plan := &Plan{
			Engine: Robocopy,
		}

		syncer := NewPathSyncer(256, 1, 1)
		err := syncer.Sync(context.Background(), srcDir, dstDir, "current", "content", plan, time.Now())

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
		dstDir := t.TempDir()

		// Create a dummy file in src so there is something to sync
		if err := os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("content"), 0644); err != nil {
			t.Fatal(err)
		}

		plan := &Plan{
			Engine: Native,
		}

		syncer := NewPathSyncer(256, 1, 1)
		relCurrent := "current"
		relContent := "content"
		timestamp := time.Now().UTC()

		err := syncer.Sync(context.Background(), srcDir, dstDir, relCurrent, relContent, plan, timestamp)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if plan.ResultInfo.RelPathKey != relContent {
			t.Errorf("expected ResultInfo.RelPathKey to be %q, got %q", relContent, plan.ResultInfo.RelPathKey)
		}
		if !plan.ResultInfo.Metadata.TimestampUTC.Equal(timestamp) {
			t.Errorf("expected ResultInfo.Metadata.TimestampUTC to be %v, got %v", timestamp, plan.ResultInfo.Metadata.TimestampUTC)
		}
		if plan.ResultInfo.Metadata.Source != srcDir {
			t.Errorf("expected ResultInfo.Metadata.Source to be %q, got %q", srcDir, plan.ResultInfo.Metadata.Source)
		}
	})
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
