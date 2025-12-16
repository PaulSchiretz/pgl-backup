package pathcompression

import (
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
func createTestBackupDir(t *testing.T, baseDir, name string, timestampUTC time.Time) string {
	t.Helper()
	backupPath := filepath.Join(baseDir, name)
	if err := os.MkdirAll(backupPath, util.UserWritableDirPerms); err != nil {
		t.Fatalf("failed to create test backup dir: %v", err)
	}

	// Create a metafile
	metadata := metafile.MetafileContent{TimestampUTC: timestampUTC}
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
	}

	for _, tc := range testCases {
		t.Run("Happy Path - "+tc.name, func(t *testing.T) {
			// Arrange
			tempDir := t.TempDir()
			cfg := config.NewDefault()
			cfg.Naming.Prefix = "backup_"
			manager := newTestCompressionManager(t, cfg)

			backupName := "backup_to_compress"
			backupDir := createTestBackupDir(t, tempDir, backupName, time.Now())

			// Act
			err := manager.Compress(context.Background(), "test", tempDir, "", tc.format)
			if err != nil {
				t.Fatalf("Compress failed: %v", err)
			}

			// Assert
			// 1. Original directory should still exist.
			if _, err := os.Stat(backupDir); os.IsNotExist(err) {
				t.Errorf("expected original backup directory to exist, but it was deleted")
			}

			// 2. Archive file should exist inside the original directory.
			archiveName := cfg.Paths.ContentSubDir + "." + tc.format.String()
			archivePath := filepath.Join(backupDir, archiveName)
			if _, err := os.Stat(archivePath); os.IsNotExist(err) {
				t.Errorf("expected archive file %s to be created, but it was not", archivePath)
			}

			// 3. Marker file should exist.
			markerPath := filepath.Join(backupDir, config.CompressedFileName)
			if _, err := os.Stat(markerPath); os.IsNotExist(err) {
				t.Errorf("expected compression marker file to be created, but it was not")
			}

			// 4. Original content (except metafile) should be gone.
			if _, err := os.Stat(filepath.Join(backupDir, cfg.Paths.ContentSubDir)); !os.IsNotExist(err) {
				t.Errorf("expected original content directory to be deleted, but it still exists")
			}
			if _, err := os.Stat(filepath.Join(backupDir, config.MetaFileName)); os.IsNotExist(err) {
				t.Errorf("expected metafile to be preserved, but it was deleted")
			}

			// 5. Verify archive content (simple check for zip)
			if tc.format == config.ZipFormat {
				r, err := zip.OpenReader(archivePath)
				if err != nil {
					t.Fatalf("failed to open created zip file: %v", err)
				}
				defer r.Close()

				foundFile1 := false
				for _, f := range r.File {
					if f.Name == "file1.txt" {
						foundFile1 = true
					}
				}
				if !foundFile1 {
					t.Errorf("zip archive is missing expected file 'file1.txt'")
				}
			}
		})
	}

	t.Run("Cancellation", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Naming.Prefix = "backup_"
		manager := newTestCompressionManager(t, cfg)

		backupName := "backup_to_cancel"
		backupDir := createTestBackupDir(t, tempDir, backupName, time.Now())

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		// Act
		err := manager.Compress(ctx, "test", tempDir, "", config.ZipFormat)

		// Assert
		if err != context.Canceled {
			t.Errorf("expected context.Canceled error, but got: %v", err)
		}

		// Original directory should still exist since compression was aborted.
		if _, err := os.Stat(backupDir); os.IsNotExist(err) {
			t.Error("original backup directory was deleted despite cancellation")
		}

		// No archive file should be left over inside the directory.
		archivePath := filepath.Join(backupDir, cfg.Paths.ContentSubDir+".zip")
		if _, err := os.Stat(archivePath); !os.IsNotExist(err) {
			t.Error("archive file was left over after cancellation")
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
		backupDir := createTestBackupDir(t, tempDir, backupName, time.Now())

		// Act
		err := manager.Compress(context.Background(), "test", tempDir, "", config.ZipFormat)
		if err != nil {
			t.Fatalf("Compress in dry run mode failed: %v", err)
		}

		// Assert
		// Original directory should NOT be deleted.
		if _, err := os.Stat(backupDir); os.IsNotExist(err) {
			t.Error("original backup directory was deleted in dry run mode")
		}

		// Archive file should NOT be created inside the directory.
		archivePath := filepath.Join(backupDir, cfg.Paths.ContentSubDir+".zip")
		if _, err := os.Stat(archivePath); !os.IsNotExist(err) {
			t.Error("archive file was created in dry run mode")
		}
	})

	t.Run("Cleanup on Failure", func(t *testing.T) {
		// This test simulates a failure during compression by making a file unreadable.
		// It calls the public Compress method and verifies that when a single backup
		// fails to compress, the original directory is left untouched and no partial
		// or temporary files remain.

		// Arrange
		archivesDir := t.TempDir() // This is the directory containing the backups to be compressed.
		cfg := config.NewDefault()
		cfg.Naming.Prefix = "backup_"
		manager := newTestCompressionManager(t, cfg)

		// Create one backup that will succeed and one that will fail.
		goodBackupName := cfg.Naming.Prefix + "good"
		goodBackupDir := createTestBackupDir(t, archivesDir, goodBackupName, time.Now())

		failBackupName := cfg.Naming.Prefix + "to_fail"
		failBackupDir := createTestBackupDir(t, archivesDir, failBackupName, time.Now())

		// To reliably cause a read error on all platforms, we create a file and
		// keep it open with an exclusive lock (on Windows). When the archiver
		// tries to open it, it will fail.
		lockedFilePath := filepath.Join(failBackupDir, cfg.Paths.ContentSubDir, "locked-file.txt")
		lockedFile, err := os.Create(lockedFilePath)
		if err != nil {
			t.Fatalf("Failed to create locked file for test: %v", err)
		}
		// Defer closing the file handle to release the lock after the test completes.
		// This is crucial for cleanup.
		defer lockedFile.Close()

		// Act
		// Call the public Compress method. It should not return an error for a single worker failure.
		err = manager.Compress(context.Background(), "test", archivesDir, "", config.ZipFormat)

		// Assert
		if err != nil {
			t.Fatalf("Compress should not return an error for a single worker failure, but got: %v", err)
		}

		// 1. The directory that FAILED to compress should still exist and be unmodified.
		if _, statErr := os.Stat(failBackupDir); os.IsNotExist(statErr) {
			t.Errorf("The directory that failed to compress was deleted, but it should have been left untouched.")
		}
		// Check that its original content is still there.
		if _, statErr := os.Stat(filepath.Join(failBackupDir, cfg.Paths.ContentSubDir)); os.IsNotExist(statErr) {
			t.Error("Original content directory of failed backup was deleted.")
		}

		// Check that no temporary files are left behind inside the failed directory.
		files, _ := filepath.Glob(filepath.Join(failBackupDir, "*.tmp"))
		for _, f := range files {
			if strings.Contains(f, "pgl-backup-") {
				t.Errorf("Found leftover archive/temp file after failure: %s", f)
			}
		}

		// 2. The directory that SUCCEEDED should be compressed.
		// It should still exist.
		if _, statErr := os.Stat(goodBackupDir); os.IsNotExist(statErr) {
			t.Error("The successfully compressed directory was deleted.")
		}
		// It should contain the archive.
		if _, statErr := os.Stat(filepath.Join(goodBackupDir, cfg.Paths.ContentSubDir+".zip")); os.IsNotExist(statErr) {
			t.Error("The archive for the successfully compressed directory was not found.")
		}
		// Its original content should be gone.
		if _, statErr := os.Stat(filepath.Join(goodBackupDir, cfg.Paths.ContentSubDir)); !os.IsNotExist(statErr) {
			t.Error("Original content of successfully compressed directory was not deleted.")
		}
	})

	t.Run("No backups to compress", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Naming.Prefix = "backup_"
		manager := newTestCompressionManager(t, cfg)

		backupDir := createTestBackupDir(t, tempDir, "backup_already_compressed", time.Now())
		// Create the marker file
		markerPath := filepath.Join(backupDir, config.CompressedFileName)
		if err := os.WriteFile(markerPath, []byte{}, util.UserWritableFilePerms); err != nil {
			t.Fatalf("failed to create compression marker: %v", err)
		}

		// Act
		err := manager.Compress(context.Background(), "test", tempDir, "", config.ZipFormat)
		if err != nil {
			t.Fatalf("Compress failed: %v", err)
		}

		// Assert: The original directory should still be there, untouched.
		if _, err := os.Stat(backupDir); os.IsNotExist(err) {
			t.Error("backup directory was deleted even though it was already compressed")
		}
	})
}
