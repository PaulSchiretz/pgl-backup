//go:build windows

package preflight

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestCheckTargetAccessible_Windows(t *testing.T) {
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

		err := checkTargetAccessible(nonExistentPath)
		if err == nil {
			t.Fatal("expected an error for a non-existent drive, but got nil")
		}

		expectedError := "volume root does not exist"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error to contain %q, but got: %v", expectedError, err)
		}
	})

	t.Run("Error - Target Path is Bare Drive Letter", func(t *testing.T) {
		err := checkTargetAccessible(`C:`)
		if err == nil {
			t.Error("expected error for target path being a bare drive letter, but got nil")
		}
		if !strings.Contains(err.Error(), "target path cannot be the current directory") {
			t.Errorf("expected error about unsafe root, but got: %v", err)
		}
	})

	t.Run("Happy Path - Target Path is Windows Volume Root", func(t *testing.T) {
		// This test verifies that an explicit volume root like "C:\" is considered a SAFE target
		// by CheckTargetAccessible. This function does not perform a write check,
		// so it should pass as the path is not an unsafe root and it exists.
		err := checkTargetAccessible(`C:\`)
		if err != nil {
			t.Errorf("expected no error for target path being a volume root, but got: %v", err)
		}
	})

	t.Run("Happy Path - Target Path is UNC Path", func(t *testing.T) {
		// The initial `isUnsafeRoot` check should pass. The function will then fail later
		// because the path doesn't exist. We verify that the error is the expected
		// "volume root does not exist" and NOT the "unsafe root" error.
		err := checkTargetAccessible(`\\server\share`)
		if err == nil {
			t.Fatal("expected an error for a non-existent UNC path, but got nil")
		}
		if !strings.Contains(err.Error(), "volume root does not exist") {
			t.Errorf("expected error to be about non-existent volume, but got: %v", err)
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
