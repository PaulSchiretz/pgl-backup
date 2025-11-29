//go:build !windows

package preflight

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckBackupTargetAccessible_Unix(t *testing.T) {
	t.Run("Error - No Permission on Deepest Existing Ancestor", func(t *testing.T) {
		// Setup: Create a directory structure where the deepest existing ancestor
		// of the target path is not accessible.
		// e.g., /tmp/grandparent/unreadable_ancestor/non_existent_child/target
		// The check should fail on "unreadable_ancestor".
		grandparent := t.TempDir()
		unreadableAncestor := filepath.Join(grandparent, "unreadable_ancestor")

		// Create the ancestor with no permissions.
		if err := os.Mkdir(unreadableAncestor, 0000); err != nil {
			t.Fatalf("failed to create unreadable ancestor dir: %v", err)
		}
		// Make sure we can clean it up later.
		t.Cleanup(func() { os.Chmod(unreadableAncestor, 0755) })

		// The target path is several levels deep, and does not exist.
		targetDir := filepath.Join(unreadableAncestor, "non_existent_child", "target")

		err := CheckBackupTargetAccessible(targetDir)
		if err == nil {
			t.Fatal("expected a permission error, but got nil")
		}
		expectedError := "cannot access ancestor directory"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error to contain %q, but got: %v", expectedError, err)
		}
	})

	t.Run("Ghost Directory Check", func(t *testing.T) {
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

		expectedError := "is on the root filesystem (system disk)"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error to contain %q, but got: %v", expectedError, err)
		}
	})

	t.Run("Ghost Directory Check Skipped for Home Dir", func(t *testing.T) {
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

func TestCheckBackupTargetWritable_Unix(t *testing.T) {
	t.Run("Error - Destination not writable", func(t *testing.T) {
		// Create a directory that we can't write into
		unwritableDir := filepath.Join(t.TempDir(), "unwritable")
		if err := os.Mkdir(unwritableDir, 0555); err != nil { // r-x r-x r-x
			t.Fatalf("failed to create unwritable dir: %v", err)
		}
		t.Cleanup(func() { os.Chmod(unwritableDir, 0755) }) // Clean up

		err := CheckBackupTargetWritable(unwritableDir)
		if err == nil {
			t.Fatal("expected an error for unwritable destination, but got nil")
		}
		if !strings.Contains(err.Error(), "not writable") {
			t.Errorf("expected error about 'not writable' or permission denied, but got: %v", err)
		}
	})
}
