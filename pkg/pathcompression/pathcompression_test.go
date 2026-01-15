package pathcompression_test

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// newTestCompressionManager creates a manager with a default config for testing.
func newTestCompressionManager(t *testing.T, cfg pathcompression.Config) *pathcompression.PathCompressionManager {
	t.Helper()
	return pathcompression.NewPathCompressionManager(cfg)
}

// createTestBackupDir creates a directory with a metafile and some content files.
func createTestBackupDir(t *testing.T, baseDir, name string, timestampUTC time.Time, isCompressed bool) string {
	t.Helper()
	backupPath := filepath.Join(baseDir, name)
	if err := os.MkdirAll(backupPath, util.UserWritableDirPerms); err != nil {
		t.Fatalf("failed to create test backup dir: %v", err)
	}

	// Create a metafile
	metadata := metafile.MetafileContent{
		TimestampUTC: timestampUTC,
		IsCompressed: isCompressed,
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

	// Create a symlink
	err := os.Symlink("file1.txt", filepath.Join(contentPath, "link1.txt"))
	if err != nil {
		if runtime.GOOS == "windows" && strings.Contains(err.Error(), "A required privilege is not held by the client") {
			t.Skip("Skipping test: creating symlinks on Windows requires administrator privileges or Developer Mode.")
		}
		// For other errors, fail the test.
		t.Fatalf("failed to create symlink: %v", err)
	}

	// Create a broken symlink
	err = os.Symlink("missing_target.txt", filepath.Join(contentPath, "broken_link.txt"))
	if err != nil {
		t.Fatalf("failed to create broken symlink: %v", err)
	}

	return backupPath
}

func TestCompress(t *testing.T) {
	testCases := []struct {
		name   string
		format pathcompression.Format
	}{
		{"Zip", pathcompression.Zip},
		{"TarGz", pathcompression.TarGz},
		{"TarZst", pathcompression.TarZst},
	}

	for _, tc := range testCases {
		t.Run("Happy Path - "+tc.name, func(t *testing.T) {
			// Arrange
			tempDir := t.TempDir()
			cfg := pathcompression.Config{
				MetricsEnabled: false,
				ContentSubDir:  "PGL_Backup_Content",
				DryRun:         false,
				BufferSizeKB:   256,
				NumWorkers:     4,
			}
			manager := newTestCompressionManager(t, cfg)

			backupName := "backup_to_compress"
			backupDir := createTestBackupDir(t, tempDir, backupName, time.Now(), false)

			// Act
			err := manager.Compress(context.Background(), []string{backupDir}, tc.format)
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
			if _, err := os.Stat(filepath.Join(backupDir, cfg.ContentSubDir)); !os.IsNotExist(err) {
				t.Errorf("expected original content directory to be deleted, but it still exists")
			}
			if _, err := os.Stat(filepath.Join(backupDir, metafile.MetaFileName)); os.IsNotExist(err) {
				t.Errorf("expected metafile to be preserved, but it was deleted")
			}

			// 5. Verify archive content (simple check for zip)
			AssertArchiveContains(t, archivePath, tc.format, []string{"file1.txt", "link1.txt", "broken_link.txt"})
		})
	}

	t.Run("Cancellation", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := pathcompression.Config{
			MetricsEnabled: false,
			ContentSubDir:  "PGL_Backup_Content",
			DryRun:         false,
			BufferSizeKB:   256,
			NumWorkers:     4,
		}
		manager := newTestCompressionManager(t, cfg)

		backupName := "backup_to_cancel"
		backupDir := createTestBackupDir(t, tempDir, backupName, time.Now(), false)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		// Act
		manager.Compress(ctx, []string{backupDir}, pathcompression.Zip)

		// Original directory should still exist since compression was aborted.
		if _, err := os.Stat(backupDir); os.IsNotExist(err) {
			t.Error("original backup directory was deleted despite cancellation")
		}

		// No archive file should be left over inside the directory.
		archivePath := filepath.Join(backupDir, backupName+".zip")
		if _, err := os.Stat(archivePath); !os.IsNotExist(err) {
			t.Error("archive file was left over after cancellation")
		}
	})

	t.Run("Worker Cancellation During Processing", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := pathcompression.Config{
			MetricsEnabled: false,
			ContentSubDir:  "PGL_Backup_Content",
			DryRun:         false,
			BufferSizeKB:   256,
			NumWorkers:     1, // Use 1 worker to serialize execution
		}
		manager := newTestCompressionManager(t, cfg)

		// Create a backup manually to populate it with many files
		backupName := "backup_heavy"
		backupPath := filepath.Join(tempDir, backupName)
		if err := os.MkdirAll(backupPath, util.UserWritableDirPerms); err != nil {
			t.Fatalf("failed to create backup dir: %v", err)
		}
		metadata := metafile.MetafileContent{TimestampUTC: time.Now(), IsCompressed: false}
		if err := metafile.Write(backupPath, metadata); err != nil {
			t.Fatalf("failed to write metafile: %v", err)
		}
		contentPath := filepath.Join(backupPath, cfg.ContentSubDir)
		if err := os.Mkdir(contentPath, util.UserWritableDirPerms); err != nil {
			t.Fatalf("failed to create content dir: %v", err)
		}
		// Create enough files to likely span across the 1ms sleep
		for i := 0; i < 500; i++ {
			fname := filepath.Join(contentPath, fmt.Sprintf("file_%d.txt", i))
			if err := os.WriteFile(fname, []byte("some content"), util.UserWritableFilePerms); err != nil {
				t.Fatalf("failed to create file: %v", err)
			}
		}

		ctx, cancel := context.WithCancel(context.Background())

		// Cancel shortly after starting
		go func() {
			time.Sleep(1 * time.Millisecond)
			cancel()
		}()

		// Act
		err := manager.Compress(ctx, []string{backupPath}, pathcompression.Zip)

		// Assert
		if err != nil {
			t.Errorf("Compress returned error on cancellation: %v", err)
		}

		// Verify consistency
		archivePath := filepath.Join(backupPath, backupName+".zip")
		_, errStat := os.Stat(archivePath)
		archiveExists := errStat == nil

		meta, err := metafile.Read(backupPath)
		if err != nil {
			t.Fatalf("failed to read metafile: %v", err)
		}

		if archiveExists {
			// If it finished before cancellation
			if !meta.IsCompressed {
				t.Error("Archive exists but metadata says not compressed")
			}
		} else {
			// If it was cancelled
			if meta.IsCompressed {
				t.Error("Archive does not exist but metadata says compressed")
			}
			// Check for leftover temp files
			entries, _ := os.ReadDir(backupPath)
			for _, e := range entries {
				if strings.HasSuffix(e.Name(), ".tmp") {
					t.Errorf("Found temp file left behind: %s", e.Name())
				}
			}
			// Content directory should still exist
			if _, err := os.Stat(contentPath); os.IsNotExist(err) {
				t.Error("Content directory missing after cancellation")
			}
		}
	})

	t.Run("Dry Run", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := pathcompression.Config{
			MetricsEnabled: false,
			ContentSubDir:  "PGL_Backup_Content",
			DryRun:         true,
			BufferSizeKB:   256,
			NumWorkers:     4,
		}
		manager := newTestCompressionManager(t, cfg)

		backupName := "backup_dry_run"
		backupDir := createTestBackupDir(t, tempDir, backupName, time.Now(), false)

		// Act
		err := manager.Compress(context.Background(), []string{backupDir}, pathcompression.Zip)
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
		cfg := pathcompression.Config{
			MetricsEnabled: false,
			ContentSubDir:  "PGL_Backup_Content",
			DryRun:         false,
			BufferSizeKB:   256,
			NumWorkers:     4,
		}
		manager := newTestCompressionManager(t, cfg)

		backupDir := createTestBackupDir(t, tempDir, "backup_already_compressed", time.Now(), true)

		// Act
		err := manager.Compress(context.Background(), []string{backupDir}, pathcompression.Zip)
		if err != nil {
			t.Fatalf("Compress failed: %v", err)
		}

		// Assert: The original directory should still be there, untouched.
		if _, err := os.Stat(backupDir); os.IsNotExist(err) {
			t.Error("backup directory was deleted even though it was already compressed")
		}
	})

	t.Run("Success even if final cleanup fails", func(t *testing.T) {
		// This test simulates a failure during the final cleanup step (os.RemoveAll).
		// This can happen if a file inside the original content directory is locked.
		// The expected behavior is that compression is still considered successful,
		// the metadata is marked as compressed, but the original content remains alongside the new archive.

		// Arrange
		archivesDir := t.TempDir()
		cfg := pathcompression.Config{
			MetricsEnabled: false,
			ContentSubDir:  "PGL_Backup_Content",
			DryRun:         false,
			BufferSizeKB:   256,
			NumWorkers:     4,
		}
		manager := newTestCompressionManager(t, cfg)

		backupName := "backup_cleanup_fail"
		backupDir := createTestBackupDir(t, archivesDir, backupName, time.Now(), false)

		// Lock a file inside the content directory to make os.RemoveAll fail.
		lockedFilePath := filepath.Join(backupDir, cfg.ContentSubDir, "locked-file.txt")
		lockedFile, err := os.Create(lockedFilePath)
		if err != nil {
			t.Fatalf("Failed to create locked file for test: %v", err)
		}
		defer lockedFile.Close()

		// Act
		err = manager.Compress(context.Background(), []string{backupDir}, pathcompression.Zip)

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

		// 3. The original content directory should still exist because cleanup failed.
		if _, statErr := os.Stat(filepath.Join(backupDir, cfg.ContentSubDir)); os.IsNotExist(statErr) {
			t.Error("Original content directory was deleted, but it should have remained due to the locked file.")
		}
	})
}

func TestIdentifyEligibleBackups(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	cfg := pathcompression.Config{
		MetricsEnabled: false,
		ContentSubDir:  "PGL_Backup_Content",
		DryRun:         false,
		BufferSizeKB:   256,
		NumWorkers:     4,
	}
	manager := newTestCompressionManager(t, cfg)

	// 1. Compressed Backup (Should be ignored)
	compressedDir := createTestBackupDir(t, tempDir, "backup_compressed", time.Now(), true)

	// 2. Uncompressed Backup (Should be selected)
	uncompressedDir := createTestBackupDir(t, tempDir, "backup_uncompressed", time.Now(), false)

	// 3. Backup with missing metafile (Should be ignored/logged)
	missingMetaDir := filepath.Join(tempDir, "backup_missing_meta")
	if err := os.MkdirAll(missingMetaDir, util.UserWritableDirPerms); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// 4. Backup with corrupt metafile (Should be ignored/logged)
	corruptMetaDir := filepath.Join(tempDir, "backup_corrupt_meta")
	if err := os.MkdirAll(corruptMetaDir, util.UserWritableDirPerms); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	// Write invalid JSON
	if err := os.WriteFile(filepath.Join(corruptMetaDir, metafile.MetaFileName), []byte("{ invalid json"), util.UserWritableFilePerms); err != nil {
		t.Fatalf("failed to write corrupt metafile: %v", err)
	}

	inputBackups := []string{compressedDir, uncompressedDir, missingMetaDir, corruptMetaDir}

	// Act
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eligible := manager.IdentifyEligibleBackups(ctx, inputBackups)

	// Assert
	if len(eligible) != 1 {
		t.Errorf("expected 1 eligible backup, got %d", len(eligible))
	} else if eligible[0].RelPathKey != uncompressedDir {
		t.Errorf("expected eligible backup to be %s, got %s", uncompressedDir, eligible[0].RelPathKey)
	}
}

// AssertArchiveContains checks if a given archive file contains all the expected file names.
func AssertArchiveContains(t *testing.T, archivePath string, format pathcompression.Format, expectedFiles []string) {
	t.Helper()

	foundFiles := make(map[string]bool)
	for _, f := range expectedFiles {
		foundFiles[f] = false
	}

	switch format {
	case pathcompression.Zip:
		r, err := zip.OpenReader(archivePath)
		if err != nil {
			t.Fatalf("failed to open created zip file %s: %v", archivePath, err)
		}
		defer r.Close()
		for _, f := range r.File {
			if _, ok := foundFiles[f.Name]; ok {
				foundFiles[f.Name] = true
				if f.Name == "link1.txt" {
					if f.Mode()&os.ModeSymlink == 0 {
						t.Errorf("expected link1.txt to be a symlink in zip")
					}
					rc, err := f.Open()
					if err != nil {
						t.Fatalf("failed to open zip entry link1.txt: %v", err)
					}
					content, err := io.ReadAll(rc)
					rc.Close()
					if err != nil {
						t.Fatalf("failed to read zip entry link1.txt: %v", err)
					}
					if string(content) != "file1.txt" {
						t.Errorf("expected link1.txt to point to 'file1.txt', got %q", string(content))
					}
				}
				if f.Name == "broken_link.txt" {
					if f.Mode()&os.ModeSymlink == 0 {
						t.Errorf("expected broken_link.txt to be a symlink in zip")
					}
					rc, err := f.Open()
					if err != nil {
						t.Fatalf("failed to open zip entry broken_link.txt: %v", err)
					}
					content, err := io.ReadAll(rc)
					rc.Close()
					if err != nil {
						t.Fatalf("failed to read zip entry broken_link.txt: %v", err)
					}
					if string(content) != "missing_target.txt" {
						t.Errorf("expected broken_link.txt to point to 'missing_target.txt', got %q", string(content))
					}
				}
			}
		}

	case pathcompression.TarGz:
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
				if header.Name == "link1.txt" {
					if header.Typeflag != tar.TypeSymlink {
						t.Errorf("expected link1.txt to be a symlink in tar.gz")
					}
					if header.Linkname != "file1.txt" {
						t.Errorf("expected link1.txt to point to 'file1.txt', got %q", header.Linkname)
					}
				}
				if header.Name == "broken_link.txt" {
					if header.Typeflag != tar.TypeSymlink {
						t.Errorf("expected broken_link.txt to be a symlink in tar.gz")
					}
					if header.Linkname != "missing_target.txt" {
						t.Errorf("expected broken_link.txt to point to 'missing_target.txt', got %q", header.Linkname)
					}
				}
			}
		}

	case pathcompression.TarZst:
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
				if header.Name == "link1.txt" {
					if header.Typeflag != tar.TypeSymlink {
						t.Errorf("expected link1.txt to be a symlink in tar.zst")
					}
					if header.Linkname != "file1.txt" {
						t.Errorf("expected link1.txt to point to 'file1.txt', got %q", header.Linkname)
					}
				}
				if header.Name == "broken_link.txt" {
					if header.Typeflag != tar.TypeSymlink {
						t.Errorf("expected broken_link.txt to be a symlink in tar.zst")
					}
					if header.Linkname != "missing_target.txt" {
						t.Errorf("expected broken_link.txt to point to 'missing_target.txt', got %q", header.Linkname)
					}
				}
			}
		}
	}

	for file, found := range foundFiles {
		if !found {
			t.Errorf("archive %s is missing expected file '%s'", archivePath, file)
		}
	}
}
