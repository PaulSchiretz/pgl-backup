package pathsync

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
)

func TestSync_Dispatch(t *testing.T) {
	t.Run("Unknown Engine", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := t.TempDir()

		cfg := config.NewDefault()
		// Set an invalid engine type
		cfg.Engine.Type = config.SyncEngine(99)

		syncer := NewPathSyncer(cfg)
		err := syncer.Sync(context.Background(), srcDir, dstDir, false, nil, nil, true)

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
		err := syncer.Sync(context.Background(), srcDir, dstDir, false, nil, nil, true)

		if err == nil {
			t.Fatal("expected an error when using robocopy on a non-windows OS, but got nil")
		}

		expectedError := "robocopy is not supported"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error to contain %q, but got: %v", expectedError, err)
		}
	})
}
