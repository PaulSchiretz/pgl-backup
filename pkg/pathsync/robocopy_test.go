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

	"pixelgardenlabs.io/pgl-backup/pkg/config"
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

func TestHandleRobocopy_Integration(t *testing.T) {
	// This is an integration test that requires robocopy.exe to be in the system's PATH.
	if _, err := exec.LookPath("robocopy"); err != nil {
		t.Skip("robocopy.exe not found in PATH, skipping integration test")
	}

	t.Run("Simple copy", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := t.TempDir()

		// Create a source file
		srcFile := filepath.Join(srcDir, "file.txt")
		if err := os.WriteFile(srcFile, []byte("robocopy test"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		// Create the syncer
		cfg := config.NewDefault()
		cfg.Quiet = true // Keep test logs clean
		syncer := NewPathSyncer(cfg)

		// Act
		err := syncer.handleRobocopy(context.Background(), srcDir, dstDir, false, nil, nil)
		if err != nil {
			t.Fatalf("handleRobocopy failed: %v", err)
		}

		// Assert
		dstFile := filepath.Join(dstDir, "file.txt")
		content, err := os.ReadFile(dstFile)
		if err != nil {
			t.Fatalf("failed to read destination file: %v", err)
		}

		if string(content) != "robocopy test" {
			t.Errorf("expected destination file content to be 'robocopy test', but got %q", string(content))
		}
	})
}
