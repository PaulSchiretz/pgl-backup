package pathsync

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
)

func TestValidateSyncPaths(t *testing.T) {
	t.Run("Happy Path", func(t *testing.T) {
		srcDir := t.TempDir()
		dstParent := t.TempDir()
		dstDir := filepath.Join(dstParent, "dest") // Don't create it yet

		syncer := NewPathSyncer(config.NewDefault())
		err := syncer.validateSyncPaths(srcDir, dstDir)

		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}

		// Check that the destination directory was created
		if _, err := os.Stat(dstDir); os.IsNotExist(err) {
			t.Error("expected destination directory to be created, but it was not")
		}
	})

	t.Run("Source Does Not Exist", func(t *testing.T) {
		srcDir := filepath.Join(t.TempDir(), "nonexistent")
		dstDir := t.TempDir()

		syncer := NewPathSyncer(config.NewDefault())
		err := syncer.validateSyncPaths(srcDir, dstDir)

		if err == nil {
			t.Fatal("expected an error for non-existent source, but got nil")
		}
	})

	t.Run("Source Is a File", func(t *testing.T) {
		srcFile := filepath.Join(t.TempDir(), "source.txt")
		if err := os.WriteFile(srcFile, []byte("I am a file"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		dstDir := t.TempDir()

		syncer := NewPathSyncer(config.NewDefault())
		err := syncer.validateSyncPaths(srcFile, dstDir)

		if err == nil {
			t.Fatal("expected an error for source being a file, but got nil")
		}
	})

	t.Run("Destination Not Writable", func(t *testing.T) {
		srcDir := t.TempDir()

		dstParent := t.TempDir()
		dstDir := filepath.Join(dstParent, "dest")
		makePathUnwritable(t, dstDir)

		syncer := NewPathSyncer(config.NewDefault())
		err := syncer.validateSyncPaths(srcDir, dstDir)

		if err == nil {
			t.Fatal("expected an error for non-writable destination, but got nil")
		}
	})

	t.Run("Dry Run Skips Write Check", func(t *testing.T) {
		srcDir := t.TempDir()

		dstParent := t.TempDir()
		dstDir := filepath.Join(dstParent, "dest")
		makePathUnwritable(t, dstDir)

		cfg := config.NewDefault()
		cfg.DryRun = true
		syncer := NewPathSyncer(cfg)
		err := syncer.validateSyncPaths(srcDir, dstDir)

		if err != nil {
			t.Fatalf("expected no error in dry run, but got: %v", err)
		}
	})
}

// makePathUnwritable simulates a non-writable path in a cross-platform way.
// On Unix-like systems, it makes the parent directory read-only.
// On Windows, it creates a file where a directory is expected, as Chmod is ineffective.
func makePathUnwritable(t *testing.T, dstPath string) {
	t.Helper()

	if runtime.GOOS == "windows" {
		// On Windows, Chmod doesn't prevent directory creation by a user with permissions.
		// The most reliable way to block MkdirAll is to create a file at the target path.
		createFile(t, dstPath, "i am a file, not a directory", time.Now())
	} else {
		// On Unix-like systems, making the parent directory read-only is the correct test.
		dstParent := filepath.Dir(dstPath)
		if err := os.Chmod(dstParent, 0555); err != nil { // r-x r-x r-x
			t.Fatalf("failed to set parent dir to read-only: %v", err)
		}
		// On test cleanup, make it writable again so the TempDir can be removed.
		t.Cleanup(func() {
			os.Chmod(dstParent, 0755)
		})
	}
}

func TestSync_Dispatch(t *testing.T) {
	t.Run("Unknown Engine", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := t.TempDir()

		cfg := config.NewDefault()
		// Set an invalid engine type
		cfg.Engine.Type = config.SyncEngine(99)

		syncer := NewPathSyncer(cfg)
		err := syncer.Sync(context.Background(), srcDir, dstDir, false, nil, nil)

		if err == nil {
			t.Fatal("expected an error for unknown sync engine, but got nil")
		}
	})
}
