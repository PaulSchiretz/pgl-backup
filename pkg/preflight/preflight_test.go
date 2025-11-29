package preflight

import (
	"os"
	"path/filepath"
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
}

func TestCheckBackupTargetWritable(t *testing.T) {
	t.Run("Happy Path - Directory is writable", func(t *testing.T) {
		// The function expects the directory to exist, so we create it.
		targetDir := t.TempDir()

		err := CheckBackupTargetWritable(targetDir)
		if err != nil {
			t.Errorf("expected no error, but got: %v", err)
		}
	})

	t.Run("Error - Target is a file", func(t *testing.T) {
		targetFile := filepath.Join(t.TempDir(), "target.txt")
		os.WriteFile(targetFile, []byte("i am a file"), 0644)
		err := CheckBackupTargetWritable(targetFile)
		if err == nil || !strings.Contains(err.Error(), "target path exists but is not a directory") {
			t.Errorf("expected error about target being a file, but got: %v", err)
		}
	})

	t.Run("Error - Target does not exist", func(t *testing.T) {
		nonExistentPath := filepath.Join(t.TempDir(), "nonexistent")
		err := CheckBackupTargetWritable(nonExistentPath)
		if err == nil {
			t.Fatal("expected an error for non-existent target, but got nil")
		}
		if !strings.Contains(err.Error(), "target directory does not exist") {
			t.Errorf("expected error about non-existent target, but got: %v", err)
		}
	})
}
