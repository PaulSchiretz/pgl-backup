//go:build windows

package pathsync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// createExitError is a helper to generate an exec.ExitError with a specific exit code.
// This is necessary because robocopy uses non-zero exit codes for success.
func createExitError(code int) error {
	// On Windows, `cmd /c exit <code>` is a reliable way to get a specific exit code.
	cmd := exec.Command("cmd", "/c", fmt.Sprintf("exit %d", code))
	err := cmd.Run()
	// We expect an error here, which will be of type *exec.ExitError.
	return err
}

func TestIsRobocopySuccessHelper(t *testing.T) {
	testCases := []struct {
		name          string
		err           error
		expectSuccess bool
	}{
		{
			name:          "Non-exit error",
			err:           errors.New("some other error"),
			expectSuccess: false,
		},
		{
			name: "Exit code 0 (no changes)",
			// This is a special case. cmd.Run() returns nil for exit code 0, not an ExitError.
			// The main handleRobocopy function handles the nil case before calling the helper.
			// The helper itself should return false for a nil error.
			err:           nil,
			expectSuccess: true,
		},
		{
			name:          "Exit code 1 (files copied)",
			err:           createExitError(1),
			expectSuccess: true,
		},
		{
			name:          "Exit code 3 (files copied and extra files)",
			err:           createExitError(3),
			expectSuccess: true,
		},
		{
			name:          "Exit code 7 (success with mismatch)",
			err:           createExitError(7),
			expectSuccess: true,
		},
		{
			name:          "Exit code 8 (some failures)",
			err:           createExitError(8),
			expectSuccess: false,
		},
		{
			name:          "Exit code 16 (fatal error)",
			err:           createExitError(16),
			expectSuccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Simulate the logic from handleRobocopy: a nil error is success,
			// otherwise, check the helper.
			isSuccess := (tc.err == nil) || isRobocopySuccessHelper(tc.err)
			if isSuccess != tc.expectSuccess {
				t.Errorf("expected success=%v, but got %v for error: %v", tc.expectSuccess, isSuccess, tc.err)
			}
		})
	}
}

func TestRobocopySync_Integration(t *testing.T) {
	// This is an integration test that requires robocopy.exe to be in the system's PATH.
	if _, err := exec.LookPath("robocopy"); err != nil {
		t.Skip("robocopy.exe not found in PATH, skipping integration test")
	}

	t.Run("Simple copy", func(t *testing.T) {
		srcDir := t.TempDir()
		baseDir := t.TempDir()

		// Create a source file
		srcFile := filepath.Join(srcDir, "file.txt")
		if err := os.WriteFile(srcFile, []byte("robocopy test"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		// Create the syncer
		plog.SetLevel(plog.LevelWarn) // Keep test logs clean
		syncer := NewPathSyncer(256, 1, 1)

		plan := &Plan{
			Enabled: true,
			Engine:  Robocopy,
		}

		// Act
		_, err := syncer.Sync(context.Background(), baseDir, srcDir, "", "", plan, time.Now())
		if err != nil {
			t.Fatalf("Sync failed: %v", err)
		}

		// Assert
		dstFile := filepath.Join(baseDir, "file.txt")
		content, err := os.ReadFile(dstFile)
		if err != nil {
			t.Fatalf("failed to read destination file: %v", err)
		}

		if string(content) != "robocopy test" {
			t.Errorf("expected destination file content to be 'robocopy test', but got %q", string(content))
		}
	})

	t.Run("No Mirror - Extra files preserved", func(t *testing.T) {
		srcDir := t.TempDir()
		baseDir := t.TempDir()

		// Create a source file
		srcFile := filepath.Join(srcDir, "file.txt")
		if err := os.WriteFile(srcFile, []byte("source content"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		// Create an extra file in the destination
		extraDstFile := filepath.Join(baseDir, "extra.txt")
		if err := os.WriteFile(extraDstFile, []byte("extra content"), 0644); err != nil {
			t.Fatalf("failed to create extra destination file: %v", err)
		}

		// Create the syncer
		plog.SetLevel(plog.LevelWarn)
		syncer := NewPathSyncer(256, 1, 1)

		plan := &Plan{
			Enabled: true,
			Engine:  Robocopy,
			Mirror:  false, // Explicitly disable mirror
		}

		// Act
		_, err := syncer.Sync(context.Background(), baseDir, srcDir, "", "", plan, time.Now())
		if err != nil {
			t.Fatalf("Sync failed: %v", err)
		}

		// Assert
		// 1. Check that the source file was copied
		dstFile := filepath.Join(baseDir, "file.txt")
		if _, err := os.Stat(dstFile); os.IsNotExist(err) {
			t.Errorf("expected destination file %q to exist, but it doesn't", dstFile)
		}

		// 2. Check that the extra destination file was NOT deleted
		if _, err := os.Stat(extraDstFile); os.IsNotExist(err) {
			t.Errorf("expected extra destination file %q to be preserved, but it was deleted", extraDstFile)
		}
	})

	t.Run("OverwriteNever - Skips newer source", func(t *testing.T) {
		srcDir := t.TempDir()
		baseDir := t.TempDir()

		// Create a source file with "newer" content
		srcFile := filepath.Join(srcDir, "file.txt")
		if err := os.WriteFile(srcFile, []byte("newer content"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		// Ensure source is newer
		futureTime := time.Now().Add(time.Hour)
		if err := os.Chtimes(srcFile, futureTime, futureTime); err != nil {
			t.Fatalf("failed to set source time: %v", err)
		}

		// Create a destination file with "older" content
		dstFile := filepath.Join(baseDir, "file.txt")
		if err := os.WriteFile(dstFile, []byte("older content"), 0644); err != nil {
			t.Fatalf("failed to create destination file: %v", err)
		}

		// Create the syncer
		plog.SetLevel(plog.LevelWarn)
		syncer := NewPathSyncer(256, 1, 1)

		plan := &Plan{
			Enabled:           true,
			Engine:            Robocopy,
			OverwriteBehavior: OverwriteNever,
		}

		// Act
		if _, err := syncer.Sync(context.Background(), baseDir, srcDir, "", "", plan, time.Now()); err != nil {
			t.Fatalf("Sync failed: %v", err)
		}

		// Assert: Content should still be "older content"
		if content, _ := os.ReadFile(dstFile); string(content) != "older content" {
			t.Errorf("expected destination content to be preserved, but got %q", string(content))
		}
	})

	t.Run("OverwriteIfNewer - Updates older destination", func(t *testing.T) {
		srcDir := t.TempDir()
		baseDir := t.TempDir()

		// Create a source file with "newer" content
		srcFile := filepath.Join(srcDir, "file.txt")
		if err := os.WriteFile(srcFile, []byte("newer content"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		// Ensure source is newer
		futureTime := time.Now().Add(time.Hour)
		if err := os.Chtimes(srcFile, futureTime, futureTime); err != nil {
			t.Fatalf("failed to set source time: %v", err)
		}

		// Create a destination file with "older" content
		dstFile := filepath.Join(baseDir, "file.txt")
		if err := os.WriteFile(dstFile, []byte("older content"), 0644); err != nil {
			t.Fatalf("failed to create destination file: %v", err)
		}
		// Ensure destination is older
		pastTime := time.Now().Add(-time.Hour)
		if err := os.Chtimes(dstFile, pastTime, pastTime); err != nil {
			t.Fatalf("failed to set destination time: %v", err)
		}

		// Create the syncer
		plog.SetLevel(plog.LevelWarn)
		syncer := NewPathSyncer(256, 1, 1)

		plan := &Plan{
			Enabled:           true,
			Engine:            Robocopy,
			OverwriteBehavior: OverwriteIfNewer,
		}

		// Act
		if _, err := syncer.Sync(context.Background(), baseDir, srcDir, "", "", plan, time.Now()); err != nil {
			t.Fatalf("Sync failed: %v", err)
		}

		// Assert: Content should be updated to "newer content"
		if content, _ := os.ReadFile(dstFile); string(content) != "newer content" {
			t.Errorf("expected destination content to be updated, but got %q", string(content))
		}
	})
}
