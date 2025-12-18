package pathcompression

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/metafile"
	"pixelgardenlabs.io/pgl-backup/pkg/util"
)

// newTestCompressionManager creates a manager with a default config for testing.
func newTestCompressionManager(t *testing.T, cfg config.Config) *PathCompressionManager {
	t.Helper()
	return NewPathCompressionManager(cfg)
}

// createTestBackupDir creates a directory with a metafile and some content files.
func createTestBackupDir(t *testing.T, baseDir, name string, timestampUTC time.Time, isCompressed bool, attempts int) string {
	t.Helper()
	backupPath := filepath.Join(baseDir, name)
	if err := os.MkdirAll(backupPath, util.UserWritableDirPerms); err != nil {
		t.Fatalf("failed to create test backup dir: %v", err)
	}

	// Create a metafile
	metadata := metafile.MetafileContent{
		TimestampUTC:        timestampUTC,
		IsCompressed:        isCompressed,
		CompressionAttempts: attempts,
	}
	if err := metafile.Write(backupPath, metadata); err != nil {
		t.Fatalf("failed to write metafile: %v", err)
	}

	contentPath := filepath.Join(backupPath, "PGL_Backup_Content") // Use literal for test setup
	if err := os.Mkdir(contentPath, util.UserWritableDirPerms); err != nil {
		t.Fatalf("failed to create test content dir: %v", err)
	}

	// Create some content
	if err := os.WriteFile(filepath.Join(contentPath, "file1.txt"), []byte("hello"), util.UserWritableFilePerms); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	return backupPath
}

func TestCompress(t *testing.T) {
	testCases := []struct {
		name   string
		format config.CompressionFormat
	}{
		{"Zip", config.ZipFormat},
		{"TarGz", config.TarGzFormat},
		{"TarZst", config.TarZstFormat},
	}

	for _, tc := range testCases {
		t.Run("Happy Path - "+tc.name, func(t *testing.T) {
			// Arrange
			tempDir := t.TempDir()
			cfg := config.NewDefault()
			cfg.Naming.Prefix = "backup_"
			manager := newTestCompressionManager(t, cfg)

			backupName := "backup_to_compress"
			backupDir := createTestBackupDir(t, tempDir, backupName, time.Now(), false, 0)

			policy := config.CompressionPolicyConfig{
				Format:     tc.format,
				MaxRetries: 3,
			}

			// Act
			err := manager.Compress(context.Background(), "test", tempDir, "", policy)
			if err != nil {
				t.Fatalf("Compress failed: %v", err)
			}

			// Assert
			// 1. Original directory should still exist.
			if _, err := os.Stat(backupDir); os.IsNotExist(err) {
				t.Errorf("expected original backup directory to exist, but it was deleted")
			}

			// 2. Archive file should exist inside the original directory.
			archiveName := backupName + "." + tc.format.String()
			archivePath := filepath.Join(backupDir, archiveName)
			if _, err := os.Stat(archivePath); os.IsNotExist(err) {
				t.Errorf("expected archive file %s to be created, but it was not", archivePath)
			}

			// 3. Metafile should be updated to show compressed.
			metadata, err := metafile.Read(backupDir)
			if err != nil {
				t.Fatalf("Failed to read metafile after compression: %v", err)
			}
			if !metadata.IsCompressed {
				t.Error("expected metafile to be marked as compressed, but it was not")
			}

			// 4. Original content (except metafile) should be gone.
			if _, err := os.Stat(filepath.Join(backupDir, cfg.Paths.ContentSubDir)); !os.IsNotExist(err) {
				t.Errorf("expected original content directory to be deleted, but it still exists")
			}
			if _, err := os.Stat(filepath.Join(backupDir, config.MetaFileName)); os.IsNotExist(err) {
				t.Errorf("expected metafile to be preserved, but it was deleted")
			}

			// 5. Verify archive content (simple check for zip)
			AssertArchiveContains(t, archivePath, tc.format, []string{"file1.txt"})
		})
	}

	t.Run("Cancellation", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Naming.Prefix = "backup_"
		manager := newTestCompressionManager(t, cfg)

		backupName := "backup_to_cancel"
		backupDir := createTestBackupDir(t, tempDir, backupName, time.Now(), false, 0)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		policy := config.CompressionPolicyConfig{
			Format:     config.ZipFormat,
			MaxRetries: 3,
		}

		// Act
		manager.Compress(ctx, "test", tempDir, "", policy)

		// Original directory should still exist since compression was aborted.
		if _, err := os.Stat(backupDir); os.IsNotExist(err) {
			t.Error("original backup directory was deleted despite cancellation")
		}

		// No archive file should be left over inside the directory.
		archivePath := filepath.Join(backupDir, backupName+".zip")
		if _, err := os.Stat(archivePath); !os.IsNotExist(err) {
			t.Error("archive file was left over after cancellation")
		}

		// Metafile should NOT be modified.
		metadata, err := metafile.Read(backupDir)
		if err != nil {
			t.Fatalf("Failed to read metafile after cancellation: %v", err)
		}
		if metadata.CompressionAttempts != 0 {
			t.Errorf("expected compression attempts to be 0 after cancellation, but got %d", metadata.CompressionAttempts)
		}
	})

	t.Run("Dry Run", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Naming.Prefix = "backup_"
		cfg.DryRun = true
		manager := newTestCompressionManager(t, cfg)

		backupName := "backup_dry_run"
		backupDir := createTestBackupDir(t, tempDir, backupName, time.Now(), false, 0)

		policy := config.CompressionPolicyConfig{
			Format:     config.ZipFormat,
			MaxRetries: 3,
		}

		// Act
		err := manager.Compress(context.Background(), "test", tempDir, "", policy)
		if err != nil {
			t.Fatalf("Compress in dry run mode failed: %v", err)
		}

		// Assert
		// Original directory should NOT be deleted.
		if _, err := os.Stat(backupDir); os.IsNotExist(err) {
			t.Error("original backup directory was deleted in dry run mode")
		}

		// Archive file should NOT be created inside the directory.
		archivePath := filepath.Join(backupDir, backupName+".zip")
		if _, err := os.Stat(archivePath); !os.IsNotExist(err) {
			t.Error("archive file was created in dry run mode")
		}
	})

	t.Run("No backups to compress", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Naming.Prefix = "backup_"
		manager := newTestCompressionManager(t, cfg)

		backupDir := createTestBackupDir(t, tempDir, "backup_already_compressed", time.Now(), true, 0)

		policy := config.CompressionPolicyConfig{
			Format:     config.ZipFormat,
			MaxRetries: 3,
		}

		// Act
		err := manager.Compress(context.Background(), "test", tempDir, "", policy)
		if err != nil {
			t.Fatalf("Compress failed: %v", err)
		}

		// Assert: The original directory should still be there, untouched.
		if _, err := os.Stat(backupDir); os.IsNotExist(err) {
			t.Error("backup directory was deleted even though it was already compressed")
		}
	})

	t.Run("Skips backup after max retries", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Naming.Prefix = "backup_"
		manager := newTestCompressionManager(t, cfg)

		// Create a backup that has already failed 3 times.
		backupName := "backup_max_retries"
		backupDir := createTestBackupDir(t, tempDir, backupName, time.Now(), false, 3)

		policy := config.CompressionPolicyConfig{
			Format:     config.ZipFormat,
			MaxRetries: 3, // The policy has max 3 retries.
		}

		// Act
		err := manager.Compress(context.Background(), "test", tempDir, "", policy)
		if err != nil {
			t.Fatalf("Compress failed: %v", err)
		}

		// Assert
		// The backup should be untouched because its attempt count (3) is >= maxRetries (3).
		// 1. Content directory should still exist.
		if _, err := os.Stat(filepath.Join(backupDir, cfg.Paths.ContentSubDir)); os.IsNotExist(err) {
			t.Error("content directory was deleted even though max retries was reached")
		}
		// 2. No archive should have been created.
		archivePath := filepath.Join(backupDir, backupName+".zip")
		if _, err := os.Stat(archivePath); !os.IsNotExist(err) {
			t.Error("archive file was created even though max retries was reached")
		}
	})

	t.Run("Success even if final cleanup fails", func(t *testing.T) {
		// This test simulates a failure during the final cleanup step (os.RemoveAll).
		// This can happen if a file inside the original content directory is locked.
		// The expected behavior is that compression is still considered successful,
		// the metadata is marked as compressed, but the original content remains alongside the new archive.

		// Arrange
		archivesDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Naming.Prefix = "backup_"
		manager := newTestCompressionManager(t, cfg)

		backupName := cfg.Naming.Prefix + "cleanup_fail"
		backupDir := createTestBackupDir(t, archivesDir, backupName, time.Now(), false, 0)

		// Lock a file inside the content directory to make os.RemoveAll fail.
		lockedFilePath := filepath.Join(backupDir, cfg.Paths.ContentSubDir, "locked-file.txt")
		lockedFile, err := os.Create(lockedFilePath)
		if err != nil {
			t.Fatalf("Failed to create locked file for test: %v", err)
		}
		defer lockedFile.Close()

		policy := config.CompressionPolicyConfig{
			Format:     config.ZipFormat,
			MaxRetries: 3,
		}

		// Act
		err = manager.Compress(context.Background(), "test", archivesDir, "", policy)

		// Assert
		if err != nil {
			t.Fatalf("Compress should not return an error for a cleanup failure, but got: %v", err)
		}

		// 1. The archive should have been created successfully.
		archivePath := filepath.Join(backupDir, backupName+".zip")
		if _, statErr := os.Stat(archivePath); os.IsNotExist(statErr) {
			t.Error("The archive was not created even though compression succeeded before cleanup.")
		}

		// 2. The metadata should be marked as compressed, with no attempts incremented.
		time.Sleep(100 * time.Millisecond) // Give fs time to sync
		finalMeta, metaErr := metafile.Read(backupDir)
		if metaErr != nil {
			t.Fatalf("Could not read metafile of backup: %v", metaErr)
		}
		if !finalMeta.IsCompressed {
			t.Error("Expected IsCompressed to be true, but it was false.")
		}
		if finalMeta.CompressionAttempts != 0 {
			t.Errorf("Expected compression attempts to be 0, but got %d", finalMeta.CompressionAttempts)
		}

		// 3. The original content directory should still exist because cleanup failed.
		if _, statErr := os.Stat(filepath.Join(backupDir, cfg.Paths.ContentSubDir)); os.IsNotExist(statErr) {
			t.Error("Original content directory was deleted, but it should have remained due to the locked file.")
		}
	})
}

// AssertArchiveContains checks if a given archive file contains all the expected file names.
func AssertArchiveContains(t *testing.T, archivePath string, format config.CompressionFormat, expectedFiles []string) {
	t.Helper()

	foundFiles := make(map[string]bool)
	for _, f := range expectedFiles {
		foundFiles[f] = false
	}

	switch format {
	case config.ZipFormat:
		r, err := zip.OpenReader(archivePath)
		if err != nil {
			t.Fatalf("failed to open created zip file %s: %v", archivePath, err)
		}
		defer r.Close()
		for _, f := range r.File {
			if _, ok := foundFiles[f.Name]; ok {
				foundFiles[f.Name] = true
			}
		}

	case config.TarGzFormat:
		file, err := os.Open(archivePath)
		if err != nil {
			t.Fatalf("failed to open created tar.gz file %s: %v", archivePath, err)
		}
		defer file.Close()
		gzr, err := gzip.NewReader(file)
		if err != nil {
			t.Fatalf("failed to create gzip reader for %s: %v", archivePath, err)
		}
		defer gzr.Close()
		tr := tar.NewReader(gzr)
		for {
			header, err := tr.Next()
			if err == io.EOF {
				break
			}
			if _, ok := foundFiles[header.Name]; ok {
				foundFiles[header.Name] = true
			}
		}

	case config.TarZstFormat:
		file, err := os.Open(archivePath)
		if err != nil {
			t.Fatalf("failed to open created tar.zst file %s: %v", archivePath, err)
		}
		defer file.Close()
		zstdr, err := zstd.NewReader(file)
		if err != nil {
			t.Fatalf("failed to create zstd reader for %s: %v", archivePath, err)
		}
		defer zstdr.Close()
		tr := tar.NewReader(zstdr)
		for {
			header, err := tr.Next()
			if err == io.EOF {
				break
			}
			if _, ok := foundFiles[header.Name]; ok {
				foundFiles[header.Name] = true
			}
		}
	}

	for file, found := range foundFiles {
		if !found {
			t.Errorf("archive %s is missing expected file '%s'", archivePath, file)
		}
	}
}
