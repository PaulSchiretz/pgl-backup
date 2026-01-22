package pathcompression_test

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"errors"
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
)

const testContentDir = "PGL_Backup_Content"

func TestCompress(t *testing.T) {
	tests := []struct {
		name        string
		plan        pathcompression.Plan
		setup       func(t *testing.T, targetBase string) metafile.MetafileInfo
		expectError error
		validate    func(t *testing.T, targetBase string, backup metafile.MetafileInfo)
	}{
		{
			name: "Happy Path - Zip",
			plan: pathcompression.Plan{
				Enabled: true,
				Format:  pathcompression.Zip,
				Metrics: true,
			},
			setup: func(t *testing.T, targetBase string) metafile.MetafileInfo {
				return createTestBackup(t, targetBase, "backup_zip", false)
			},
			validate: func(t *testing.T, targetBase string, backup metafile.MetafileInfo) {
				backupPath := filepath.Join(targetBase, backup.RelPathKey)
				archivePath := filepath.Join(backupPath, "backup_zip.zip")

				if _, err := os.Stat(archivePath); os.IsNotExist(err) {
					t.Errorf("Archive not found at %s", archivePath)
				}

				assertContentDeleted(t, backupPath)
				assertMetaCompressed(t, backupPath)
				assertArchiveContains(t, archivePath, pathcompression.Zip, []string{"file.txt"})
			},
		},
		{
			name: "Happy Path - TarGz",
			plan: pathcompression.Plan{
				Enabled: true,
				Format:  pathcompression.TarGz,
			},
			setup: func(t *testing.T, targetBase string) metafile.MetafileInfo {
				return createTestBackup(t, targetBase, "backup_targz", false)
			},
			validate: func(t *testing.T, targetBase string, backup metafile.MetafileInfo) {
				backupPath := filepath.Join(targetBase, backup.RelPathKey)
				archivePath := filepath.Join(backupPath, "backup_targz.tar.gz")

				if _, err := os.Stat(archivePath); os.IsNotExist(err) {
					t.Errorf("Archive not found at %s", archivePath)
				}
				assertContentDeleted(t, backupPath)
				assertMetaCompressed(t, backupPath)
				assertArchiveContains(t, archivePath, pathcompression.TarGz, []string{"file.txt"})
			},
		},
		{
			name: "Happy Path - TarZst",
			plan: pathcompression.Plan{
				Enabled: true,
				Format:  pathcompression.TarZst,
			},
			setup: func(t *testing.T, targetBase string) metafile.MetafileInfo {
				return createTestBackup(t, targetBase, "backup_tarzst", false)
			},
			validate: func(t *testing.T, targetBase string, backup metafile.MetafileInfo) {
				backupPath := filepath.Join(targetBase, backup.RelPathKey)
				archivePath := filepath.Join(backupPath, "backup_tarzst.tar.zst")

				if _, err := os.Stat(archivePath); os.IsNotExist(err) {
					t.Errorf("Archive not found at %s", archivePath)
				}
				assertContentDeleted(t, backupPath)
				assertMetaCompressed(t, backupPath)
				assertArchiveContains(t, archivePath, pathcompression.TarZst, []string{"file.txt"})
			},
		},
		{
			name: "Dry Run",
			plan: pathcompression.Plan{
				Enabled: true,
				Format:  pathcompression.Zip,
				DryRun:  true,
			},
			setup: func(t *testing.T, targetBase string) metafile.MetafileInfo {
				return createTestBackup(t, targetBase, "backup_dryrun", false)
			},
			validate: func(t *testing.T, targetBase string, backup metafile.MetafileInfo) {
				backupPath := filepath.Join(targetBase, backup.RelPathKey)
				archivePath := filepath.Join(backupPath, "backup_dryrun.zip")

				if _, err := os.Stat(archivePath); !os.IsNotExist(err) {
					t.Errorf("Archive should NOT exist in dry run: %s", archivePath)
				}
				assertContentExists(t, backupPath)
				assertMetaNotCompressed(t, backupPath)
			},
		},
		{
			name:        "Empty Metafile Info",
			plan:        pathcompression.Plan{Enabled: true, Format: pathcompression.Zip},
			expectError: pathcompression.ErrNothingToCompress,
			setup: func(t *testing.T, targetBase string) metafile.MetafileInfo {
				return metafile.MetafileInfo{}
			},
			validate: func(t *testing.T, targetBase string, backup metafile.MetafileInfo) {
				// Nothing to validate
			},
		},
		{
			name: "Symlinks",
			plan: pathcompression.Plan{Enabled: true, Format: pathcompression.Zip},
			setup: func(t *testing.T, targetBase string) metafile.MetafileInfo {
				info := createTestBackup(t, targetBase, "backup_symlink", false)
				// Add symlink
				contentDir := filepath.Join(targetBase, info.RelPathKey, testContentDir)
				if err := os.Symlink("file.txt", filepath.Join(contentDir, "link.txt")); err != nil {
					// Skip if windows privilege error
					if runtime.GOOS == "windows" && strings.Contains(err.Error(), "privilege") {
						t.Skip("Skipping symlink test on Windows (requires admin)")
					}
					t.Fatalf("Failed to create symlink: %v", err)
				}
				return info
			},
			validate: func(t *testing.T, targetBase string, backup metafile.MetafileInfo) {
				backupPath := filepath.Join(targetBase, backup.RelPathKey)
				archivePath := filepath.Join(backupPath, "backup_symlink.zip")
				assertArchiveContains(t, archivePath, pathcompression.Zip, []string{"file.txt", "link.txt"})
			},
		},
		{
			name: "Disabled - No Compression",
			plan: pathcompression.Plan{
				Enabled: false,
				Format:  pathcompression.Zip,
			},
			expectError: pathcompression.ErrDisabled,
			setup: func(t *testing.T, targetBase string) metafile.MetafileInfo {
				return createTestBackup(t, targetBase, "backup_disabled", false)
			},
			validate: func(t *testing.T, targetBase string, backup metafile.MetafileInfo) {
				backupPath := filepath.Join(targetBase, backup.RelPathKey)
				archivePath := filepath.Join(backupPath, "backup_disabled.zip")

				if _, err := os.Stat(archivePath); !os.IsNotExist(err) {
					t.Errorf("Archive should NOT exist when disabled: %s", archivePath)
				}
				assertContentExists(t, backupPath)
				assertMetaNotCompressed(t, backupPath)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			targetBase := t.TempDir()
			toCompress := tc.setup(t, targetBase)

			compressor := pathcompression.NewPathCompressor(256)

			err := compressor.Compress(context.Background(), targetBase, testContentDir, toCompress, &tc.plan, time.Now().UTC())

			if tc.expectError != nil {
				if !errors.Is(err, tc.expectError) {
					t.Errorf("Expected error %v, got %v", tc.expectError, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}

			if tc.validate != nil {
				tc.validate(t, targetBase, toCompress)
			}
		})
	}
}

func TestExtract(t *testing.T) {
	tests := []struct {
		name     string
		format   pathcompression.Format
		setup    func(t *testing.T, targetBase string) metafile.MetafileInfo
		validate func(t *testing.T, extractPath string)
	}{
		{
			name:   "Extract Zip",
			format: pathcompression.Zip,
			setup: func(t *testing.T, targetBase string) metafile.MetafileInfo {
				return createTestBackup(t, targetBase, "extract_zip", false)
			},
			validate: func(t *testing.T, extractPath string) {
				assertFileContent(t, filepath.Join(extractPath, "file.txt"), "content")
			},
		},
		{
			name:   "Extract TarGz",
			format: pathcompression.TarGz,
			setup: func(t *testing.T, targetBase string) metafile.MetafileInfo {
				return createTestBackup(t, targetBase, "extract_targz", false)
			},
			validate: func(t *testing.T, extractPath string) {
				assertFileContent(t, filepath.Join(extractPath, "file.txt"), "content")
			},
		},
		{
			name:   "Extract TarZst",
			format: pathcompression.TarZst,
			setup: func(t *testing.T, targetBase string) metafile.MetafileInfo {
				return createTestBackup(t, targetBase, "extract_tarzst", false)
			},
			validate: func(t *testing.T, extractPath string) {
				assertFileContent(t, filepath.Join(extractPath, "file.txt"), "content")
			},
		},
		{
			name:   "Extract Symlinks (TarZst)",
			format: pathcompression.TarZst,
			setup: func(t *testing.T, targetBase string) metafile.MetafileInfo {
				info := createTestBackup(t, targetBase, "extract_symlink", false)
				contentDir := filepath.Join(targetBase, info.RelPathKey, testContentDir)
				if err := os.Symlink("file.txt", filepath.Join(contentDir, "link.txt")); err != nil {
					if runtime.GOOS == "windows" && strings.Contains(err.Error(), "privilege") {
						t.Skip("Skipping symlink test on Windows (requires admin)")
					}
					t.Fatalf("Failed to create symlink: %v", err)
				}
				return info
			},
			validate: func(t *testing.T, extractPath string) {
				assertFileContent(t, filepath.Join(extractPath, "file.txt"), "content")

				linkPath := filepath.Join(extractPath, "link.txt")
				info, err := os.Lstat(linkPath)
				if err != nil {
					t.Fatalf("Failed to lstat extracted link: %v", err)
				}
				if info.Mode()&os.ModeSymlink == 0 {
					t.Errorf("Expected link.txt to be a symlink")
				}
				target, err := os.Readlink(linkPath)
				if err != nil {
					t.Fatalf("Failed to read link target: %v", err)
				}
				if target != "file.txt" {
					t.Errorf("Expected link target 'file.txt', got %q", target)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			targetBase := t.TempDir()
			toCompress := tc.setup(t, targetBase)

			// 1. Compress first to generate the archive
			compressPlan := &pathcompression.Plan{
				Enabled: true,
				Format:  tc.format,
			}
			compressor := pathcompression.NewPathCompressor(256)

			err := compressor.Compress(context.Background(), targetBase, testContentDir, toCompress, compressPlan, time.Now().UTC())
			if err != nil {
				t.Fatalf("Setup failed: failed to compress: %v", err)
			}

			// 2. Extract
			extractPath := filepath.Join(targetBase, "extracted")
			extractPlan := &pathcompression.Plan{
				Enabled: true,
				Format:  tc.format,
			}

			err = compressor.Extract(context.Background(), targetBase, toCompress, extractPath, extractPlan, time.Now().UTC())
			if err != nil {
				t.Fatalf("Extract failed: %v", err)
			}

			if tc.validate != nil {
				tc.validate(t, extractPath)
			}
		})
	}
}

// Helpers

func createTestBackup(t *testing.T, targetBase, relPath string, compressed bool) metafile.MetafileInfo {
	absPath := filepath.Join(targetBase, relPath)
	if err := os.MkdirAll(absPath, 0755); err != nil {
		t.Fatalf("Failed to create backup dir: %v", err)
	}

	meta := metafile.MetafileContent{
		TimestampUTC: time.Now().UTC(),
		IsCompressed: compressed,
	}
	if err := metafile.Write(absPath, meta); err != nil {
		t.Fatalf("Failed to write metafile: %v", err)
	}

	contentDir := filepath.Join(absPath, testContentDir)
	if err := os.MkdirAll(contentDir, 0755); err != nil {
		t.Fatalf("Failed to create content dir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(contentDir, "file.txt"), []byte("content"), 0644); err != nil {
		t.Fatalf("Failed to create content file: %v", err)
	}

	return metafile.MetafileInfo{
		RelPathKey: relPath,
		Metadata:   meta,
	}
}

func assertContentDeleted(t *testing.T, backupPath string) {
	t.Helper()
	contentPath := filepath.Join(backupPath, testContentDir)
	if _, err := os.Stat(contentPath); !os.IsNotExist(err) {
		t.Errorf("Content directory %s should have been deleted", contentPath)
	}
}

func assertContentExists(t *testing.T, backupPath string) {
	t.Helper()
	contentPath := filepath.Join(backupPath, testContentDir)
	if _, err := os.Stat(contentPath); os.IsNotExist(err) {
		t.Errorf("Content directory %s should exist", contentPath)
	}
}

func assertMetaCompressed(t *testing.T, backupPath string) {
	t.Helper()
	m, err := metafile.Read(backupPath)
	if err != nil {
		t.Fatalf("Failed to read metafile: %v", err)
	}
	if !m.IsCompressed {
		t.Error("Metafile should be marked as compressed")
	}
}

func assertMetaNotCompressed(t *testing.T, backupPath string) {
	t.Helper()
	m, err := metafile.Read(backupPath)
	if err != nil {
		t.Fatalf("Failed to read metafile: %v", err)
	}
	if m.IsCompressed {
		t.Error("Metafile should NOT be marked as compressed")
	}
}

func assertArchiveContains(t *testing.T, archivePath string, format pathcompression.Format, expectedFiles []string) {
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

func assertFileContent(t *testing.T, path, expected string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file %s: %v", path, err)
	}
	if string(content) != expected {
		t.Errorf("File content mismatch for %s. Expected %q, got %q", path, expected, string(content))
	}
}
