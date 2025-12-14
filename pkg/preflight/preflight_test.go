package preflight

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
)

func TestCheckBackupTargetAccessible(t *testing.T) {
	t.Run("Happy Path - Target Exists", func(t *testing.T) {
		targetDir := t.TempDir()
		err := checkBackupTargetAccessible(targetDir)
		if err != nil {
			t.Errorf("expected no error for existing directory, but got: %v", err)
		}
	})

	t.Run("Happy Path - Target Does Not Exist, Parent Exists", func(t *testing.T) {
		parentDir := t.TempDir()
		targetDir := filepath.Join(parentDir, "new_dir")

		err := checkBackupTargetAccessible(targetDir)
		if err != nil {
			t.Errorf("expected no error when parent exists, but got: %v", err)
		}
	})

	t.Run("Error - Target Is a File", func(t *testing.T) {
		targetFile := filepath.Join(t.TempDir(), "target.txt")
		if err := os.WriteFile(targetFile, []byte("i am a file"), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		err := checkBackupTargetAccessible(targetFile)
		if err == nil {
			t.Fatal("expected an error when target is a file, but got nil")
		}
		if !strings.Contains(err.Error(), "is not a directory") {
			t.Errorf("expected error to be about 'not a directory', but got: %v", err)
		}
	})

	t.Run("Error - Target Path is Current Directory", func(t *testing.T) {
		err := checkBackupTargetAccessible(".")
		if err == nil {
			t.Error("expected error for target path being current directory, but got nil")
		}
	})

	t.Run("Error - Target Path is Root Directory", func(t *testing.T) {
		err := checkBackupTargetAccessible(string(filepath.Separator))
		if err == nil {
			t.Error("expected error for target path being root directory, but got nil")
		}
	})

}

func TestCheckBackupSourceAccessible(t *testing.T) {
	t.Run("Happy Path - Source is a directory", func(t *testing.T) {
		srcDir := t.TempDir()
		err := checkBackupSourceAccessible(srcDir)
		if err != nil {
			t.Errorf("expected no error for existing directory, but got: %v", err)
		}
	})

	t.Run("Error - Source does not exist", func(t *testing.T) {
		nonExistentPath := filepath.Join(t.TempDir(), "nonexistent")
		err := checkBackupSourceAccessible(nonExistentPath)
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
		err := checkBackupSourceAccessible(srcFile)
		if err == nil {
			t.Fatal("expected an error when source is a file, but got nil")
		}
		if !strings.Contains(err.Error(), "is not a directory") {
			t.Errorf("expected error about source not being a directory, but got: %v", err)
		}
	})
}

func TestRunChecks(t *testing.T) {
	// Helper to create a valid config for testing purposes.
	newValidConfig := func(t *testing.T) *config.Config {
		cfg := config.NewDefault()
		cfg.Paths.Source = t.TempDir()
		cfg.Paths.TargetBase = filepath.Join(t.TempDir(), "target")
		return &cfg
	}

	t.Run("Happy Path - Normal Run", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.DryRun = false

		err := RunChecks(cfg)
		if err != nil {
			t.Fatalf("RunChecks() failed on a valid config: %v", err)
		}

		// Verify that the target directory was created.
		if _, err := os.Stat(cfg.Paths.TargetBase); os.IsNotExist(err) {
			t.Error("expected target directory to be created, but it was not")
		}
	})

	t.Run("Happy Path - Dry Run", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.DryRun = true

		err := RunChecks(cfg)
		if err != nil {
			t.Fatalf("RunChecks() failed on a valid config during a dry run: %v", err)
		}

		// Verify that the target directory was NOT created.
		if _, err := os.Stat(cfg.Paths.TargetBase); !os.IsNotExist(err) {
			t.Error("expected target directory NOT to be created on a dry run, but it was")
		}
	})

	t.Run("Failure - Invalid Config", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Paths.Source = "" // Make the config invalid.

		err := RunChecks(cfg)
		if err == nil {
			t.Fatal("expected RunChecks() to fail with an invalid config, but it passed")
		}
		if !strings.Contains(err.Error(), "invalid configuration") {
			t.Errorf("expected error to be about invalid configuration, but got: %v", err)
		}
	})

	t.Run("Failure - Inaccessible Target", func(t *testing.T) {
		cfg := newValidConfig(t)
		// Use a file as the target path to make it inaccessible as a directory.
		targetFile := filepath.Join(t.TempDir(), "file.txt")
		if err := os.WriteFile(targetFile, []byte("not a dir"), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
		cfg.Paths.TargetBase = targetFile

		err := RunChecks(cfg)
		if err == nil {
			t.Fatal("expected RunChecks() to fail with an inaccessible target, but it passed")
		}
		if !strings.Contains(err.Error(), "target path accessibility check failed") {
			t.Errorf("expected error to be about target accessibility, but got: %v", err)
		}
	})

	t.Run("Failure - Inaccessible Source", func(t *testing.T) {
		cfg := newValidConfig(t)
		// Use a file as the source path to make it fail the "is directory" check.
		sourceFile := filepath.Join(t.TempDir(), "file.txt")
		if err := os.WriteFile(sourceFile, []byte("not a dir"), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
		cfg.Paths.Source = sourceFile

		err := RunChecks(cfg)
		if err == nil {
			t.Fatal("expected RunChecks() to fail with an inaccessible source, but it passed")
		}
		if !strings.Contains(err.Error(), "source path validation failed") {
			t.Errorf("expected error to be about source validation, but got: %v", err)
		}
	})
}

func TestCheckBackupTargetWritable(t *testing.T) {
	t.Run("Happy Path - Directory is writable", func(t *testing.T) {
		// The function expects the directory to exist, so we create it.
		targetDir := t.TempDir()

		err := checkBackupTargetWritable(targetDir)
		if err != nil {
			t.Errorf("expected no error, but got: %v", err)
		}
	})

	t.Run("Error - Target is a file", func(t *testing.T) {
		targetFile := filepath.Join(t.TempDir(), "target.txt")
		os.WriteFile(targetFile, []byte("i am a file"), 0644)
		err := checkBackupTargetWritable(targetFile)
		if err == nil || !strings.Contains(err.Error(), "target path exists but is not a directory") {
			t.Errorf("expected error about target being a file, but got: %v", err)
		}
	})

	t.Run("Error - Target does not exist", func(t *testing.T) {
		nonExistentPath := filepath.Join(t.TempDir(), "nonexistent")
		err := checkBackupTargetWritable(nonExistentPath)
		if err == nil {
			t.Fatal("expected an error for non-existent target, but got nil")
		}
		if !strings.Contains(err.Error(), "target directory does not exist") {
			t.Errorf("expected error about non-existent target, but got: %v", err)
		}
	})
}

func TestCheckCaseSensitivityMismatch(t *testing.T) {
	t.Run("Detects no mismatch on same-sensitivity filesystems", func(t *testing.T) {
		// This test validates the behavior of the check on the filesystem it's currently running on.
		// - On Windows/macOS, it tests the "safe" path where the source is also case-insensitive.
		// - On Linux, it tests the path where the check is correctly skipped.
		srcDir := t.TempDir()

		err := checkCaseSensitivityMismatch(srcDir)

		if err != nil {
			t.Fatalf("checkCaseSensitivityMismatch() returned an unexpected error: %v", err)
		}
	})
}
