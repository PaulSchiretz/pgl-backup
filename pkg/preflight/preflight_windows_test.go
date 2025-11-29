//go:build windows

package preflight

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestCheckBackupTargetAccessible_Windows(t *testing.T) {
	t.Run("Windows - Error on Non-Existent Drive", func(t *testing.T) {
		// Helper to find a drive letter that is guaranteed not to exist on this system.
		findFirstNonExistentDrive := func() string {
			drives, err := windows.GetLogicalDrives()
			if err != nil {
				t.Fatalf("Failed to get logical drives: %v", err)
			}

			for letter := 'A'; letter <= 'Z'; letter++ {
				driveBit := uint32(1) << (letter - 'A')
				if (drives & driveBit) == 0 {
					return string(letter) + `:\`
				}
			}
			return "" // All drive letters are in use.
		}

		nonExistentDrive := findFirstNonExistentDrive()
		if nonExistentDrive == "" {
			t.Skip("could not find a non-existent drive letter; all letters A-Z are in use")
		}
		nonExistentPath := filepath.Join(nonExistentDrive, "nonexistent", "backup", "path")

		err := CheckBackupTargetAccessible(nonExistentPath)
		if err == nil {
			t.Fatal("expected an error for a non-existent drive, but got nil")
		}

		expectedError := "volume root does not exist"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error to contain %q, but got: %v", expectedError, err)
		}
	})
}

func TestPlatformValidateMountPoint_Windows(t *testing.T) {
	// Get a known existing volume, like C:
	tempDir := t.TempDir()
	existingVolume := filepath.VolumeName(tempDir)
	if existingVolume == "" {
		t.Skip("could not determine an existing volume for testing")
	}

	testCases := []struct {
		name          string
		path          string
		expectAnError bool
	}{
		{
			name:          "Happy Path - Existing drive",
			path:          filepath.Join(existingVolume, "Users", "Test"),
			expectAnError: false,
		},
		{
			name:          "Happy Path - Relative path",
			path:          `some\relative\path`,
			expectAnError: false, // Should do nothing and not error
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := platformValidateMountPoint(tc.path)
			if tc.expectAnError && err == nil {
				t.Errorf("expected an error for path %q but got nil", tc.path)
			} else if !tc.expectAnError && err != nil {
				t.Errorf("expected no error for path %q but got: %v", tc.path, err)
			}
		})
	}
}
