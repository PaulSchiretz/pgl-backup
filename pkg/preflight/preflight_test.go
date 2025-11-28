package preflight

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCheckBackupTargetAccessible(t *testing.T) {
	t.Run("Happy Path - Target Exists", func(t *testing.T) {
		targetDir := t.TempDir()
		err := CheckBackupTargetAccessible(targetDir)
		if err != nil {
			t.Errorf("expected no error for existing directory, but got: %v", err)
		}
	})

	t.Run("Happy Path - Target Does Not Exist, Parent Exists", func(t *testing.T) {
		parentDir := t.TempDir()
		targetDir := filepath.Join(parentDir, "new_dir")

		err := CheckBackupTargetAccessible(targetDir)
		if err != nil {
			t.Errorf("expected no error when parent exists, but got: %v", err)
		}
	})

	t.Run("Error - Target Is a File", func(t *testing.T) {
		targetFile := filepath.Join(t.TempDir(), "target.txt")
		if err := os.WriteFile(targetFile, []byte("i am a file"), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		err := CheckBackupTargetAccessible(targetFile)
		if err == nil {
			t.Fatal("expected an error when target is a file, but got nil")
		}
		if !strings.Contains(err.Error(), "is not a directory") {
			t.Errorf("expected error to be about 'not a directory', but got: %v", err)
		}
	})

	t.Run("Error - Target and Parent Do Not Exist", func(t *testing.T) {
		nonExistentPath := filepath.Join(t.TempDir(), "nonexistent", "target")

		err := CheckBackupTargetAccessible(nonExistentPath)
		if err == nil {
			t.Fatal("expected an error when target and parent do not exist, but got nil")
		}
		if !strings.Contains(err.Error(), "target path and parent directory do not exist") {
			t.Errorf("expected error about non-existent parent, but got: %v", err)
		}
	})

	t.Run("Error - No Permission to Stat Parent", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("permission tests are not reliable on Windows")
		}

		// Setup: Create a grandparent directory we can enter, but a parent directory
		// that we cannot search (no 'x' permission). This allows the ancestor
		// search to succeed but the final parent check to fail with a permission error.
		readableGrandparent := t.TempDir()
		unsearchableParent := filepath.Join(readableGrandparent, "unsearchable")
		// Mode 0666 (rw-rw-rw-) allows stat but not listing contents for non-root users.
		if err := os.Mkdir(unsearchableParent, 0666); err != nil {
			t.Fatalf("failed to create unsearchable dir: %v", err)
		}
		t.Cleanup(func() { os.Chmod(unsearchableParent, 0755) }) // Clean up permissions

		targetDir := filepath.Join(unsearchableParent, "target")

		err := CheckBackupTargetAccessible(targetDir)
		if err == nil {
			t.Fatal("expected a permission error, but got nil")
		}
		expectedError := "cannot access parent directory"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error to contain %q, but got: %v", expectedError, err)
		}
	})

	t.Run("Unix - Ghost Directory Check", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("ghost directory check is for Unix-like systems only")
		}

		// This test simulates a "ghost" directory.
		// We create /tmp/pgl-test-mnt/backup, where /tmp/pgl-test-mnt is intended
		// to be a mount point but isn't.
		mountPointBase := filepath.Join(os.TempDir(), "pgl-test-mnt")
		targetDir := filepath.Join(mountPointBase, "backup")

		if err := os.MkdirAll(targetDir, 0755); err != nil {
			t.Fatalf("failed to create test directories: %v", err)
		}
		t.Cleanup(func() { os.RemoveAll(mountPointBase) })

		err := CheckBackupTargetAccessible(targetDir)
		if err == nil {
			t.Fatal("expected an error for a non-mounted 'ghost' directory, but got nil")
		}

		expectedError := "appears to be on the system disk but is expected to be a mount point"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error to contain %q, but got: %v", expectedError, err)
		}
	})

	t.Run("Unix - Ghost Directory Check Skipped for Home Dir", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("ghost directory check is for Unix-like systems only")
		}

		homeDir, err := os.UserHomeDir()
		if err != nil {
			t.Fatalf("could not get user home directory: %v", err)
		}

		// Create a path inside the user's home directory.
		targetDir := filepath.Join(homeDir, "pgl-test-backup")
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			// It might fail if we don't have permissions, but we try.
			t.Logf("could not create test dir in home, skipping: %v", err)
			t.SkipNow()
		}
		t.Cleanup(func() { os.RemoveAll(targetDir) })

		// This check should pass because the heuristic skips the mount point check
		// for paths inside the home directory.
		err = CheckBackupTargetAccessible(targetDir)
		if err != nil {
			t.Errorf("expected no error for a path in the home directory, but got: %v", err)
		}
	})
}

func TestCheckVolumeExists_Windows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("this test is for windows platforms only")
	}

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
		{
			name:          "Error - Non-existent drive",
			path:          `Z:\nonexistent\path`, // Assuming Z: does not exist
			expectAnError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkVolumeExists(tc.path)
			if tc.expectAnError && err == nil {
				t.Errorf("expected an error for path %q but got nil", tc.path)
			} else if !tc.expectAnError && err != nil {
				t.Errorf("expected no error for path %q but got: %v", tc.path, err)
			}
		})
	}
}

func TestCheckBackupSourceAccessible(t *testing.T) {
	t.Run("Happy Path - Source is a directory", func(t *testing.T) {
		srcDir := t.TempDir()
		err := CheckBackupSourceAccessible(srcDir)
		if err != nil {
			t.Errorf("expected no error for existing directory, but got: %v", err)
		}
	})

	t.Run("Error - Source does not exist", func(t *testing.T) {
		nonExistentPath := filepath.Join(t.TempDir(), "nonexistent")
		err := CheckBackupSourceAccessible(nonExistentPath)
		if err == nil {
			t.Fatal("expected an error for non-existent source, but got nil")
		}
		if !strings.Contains(err.Error(), "does not exist") {
			t.Errorf("expected error about non-existent source, but got: %v", err)
		}
	})

	t.Run("Error - Source is a file", func(t *testing.T) {
		srcFile := filepath.Join(t.TempDir(), "source.txt")
		if err := os.WriteFile(srcFile, []byte("i am a file"), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
		err := CheckBackupSourceAccessible(srcFile)
		if err == nil {
			t.Fatal("expected an error when source is a file, but got nil")
		}
		if !strings.Contains(err.Error(), "is not a directory") {
			t.Errorf("expected error about source not being a directory, but got: %v", err)
		}
	})

	t.Run("Error - No permission to stat source", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("permission tests are not reliable on Windows")
		}

		unreadableDir := filepath.Join(t.TempDir(), "unreadable")
		if err := os.Mkdir(unreadableDir, 0000); err != nil { // no permissions
			t.Fatalf("failed to create unreadable dir: %v", err)
		}
		t.Cleanup(func() { os.Chmod(unreadableDir, 0755) }) // Clean up

		err := CheckBackupSourceAccessible(unreadableDir)
		// Note: The error comes from os.Stat, which might not be a permission error itself
		// but a consequence of it. We just check that an error is returned.
		if err == nil {
			t.Fatal("expected a permission error, but got nil")
		}
	})
}

func TestCheckBackupTargetWritable(t *testing.T) {
	t.Run("Happy Path - Creates and is writable", func(t *testing.T) {
		parentDir := t.TempDir()
		targetDir := filepath.Join(parentDir, "new_backup_target")
		err := CheckBackupTargetWritable(targetDir)
		if err != nil {
			t.Errorf("expected no error, but got: %v", err)
		}
		// Verify directory was created
		if _, err := os.Stat(targetDir); os.IsNotExist(err) {
			t.Error("expected target directory to be created, but it was not")
		}
	})

	t.Run("Error - Destination not writable", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("permission tests are not reliable on Windows")
		}

		// Create a directory that we can't write into
		unwritableDir := filepath.Join(t.TempDir(), "unwritable")
		if err := os.Mkdir(unwritableDir, 0555); err != nil { // r-x r-x r-x
			t.Fatalf("failed to create unwritable dir: %v", err)
		}
		t.Cleanup(func() { os.Chmod(unwritableDir, 0755) }) // Clean up

		// Try to make a subdirectory in the unwritable one
		targetDir := filepath.Join(unwritableDir, "sub_target")

		err := CheckBackupTargetWritable(targetDir)
		if err == nil {
			t.Fatal("expected an error for unwritable destination, but got nil")
		}
		if !strings.Contains(err.Error(), "not writable") && !os.IsPermission(err) {
			t.Errorf("expected error about 'not writable' or permission denied, but got: %v", err)
		}
	})

	t.Run("Error - Target is a file", func(t *testing.T) {
		targetFile := filepath.Join(t.TempDir(), "target.txt")
		os.WriteFile(targetFile, []byte("i am a file"), 0644)
		err := CheckBackupTargetWritable(targetFile)
		if err == nil || !strings.Contains(err.Error(), "failed to create target directory") {
			t.Errorf("expected error about creating directory over a file, but got: %v", err)
		}
	})
}
