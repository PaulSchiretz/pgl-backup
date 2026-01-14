package pathsync

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/paulschiretz/pgl-backup/pkg/config"
)

func TestSync_Dispatch(t *testing.T) {
	t.Run("Unknown Engine", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := t.TempDir()

		cfg := config.NewDefault()
		// Set an invalid engine type
		cfg.Engine.Type = config.SyncEngine(99)

		syncer := NewPathSyncer(cfg)
		err := syncer.Sync(context.Background(), srcDir, dstDir, false, false, nil, nil, true)

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

		cfg := config.NewDefault()
		cfg.Engine.Type = config.RobocopyEngine

		syncer := NewPathSyncer(cfg)
		err := syncer.Sync(context.Background(), srcDir, dstDir, false, false, nil, nil, true)

		if err == nil {
			t.Fatal("expected an error when using robocopy on a non-windows OS, but got nil")
		}

		expectedError := "robocopy is not supported"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error to contain %q, but got: %v", expectedError, err)
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

			got := resolveTargetDirectory(tc.source, targetBase, tc.preserveSourceDirectoryName)
			if got != tc.expectedTarget {
				t.Errorf("resolveTargetDirectory(%q, %q, %v) = %q; want %q", tc.source, targetBase, tc.preserveSourceDirectoryName, got, tc.expectedTarget)
			}
		})
	}
}
