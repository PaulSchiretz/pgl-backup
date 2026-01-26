package metafile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteAndReadMetafile(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Test Write
	testContent := MetafileContent{
		Version:      "1.0.0",
		TimestampUTC: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		Mode:         "incremental",
		UUID:         "test-uuid-1234",
	}

	err := Write(tempDir, &testContent)
	if err != nil {
		t.Fatalf("Write() failed: %v", err)
	}

	metaFilePath := filepath.Join(tempDir, MetaFileName)
	if _, err := os.Stat(metaFilePath); os.IsNotExist(err) {
		t.Fatalf("Metafile was not created at %s", metaFilePath)
	}

	// 2. Test Read
	readContent, err := Read(tempDir)
	if err != nil {
		t.Fatalf("Read() failed: %v", err)
	}

	if readContent.Version != testContent.Version {
		t.Errorf("Expected version %q, got %q", testContent.Version, readContent.Version)
	}
	if !readContent.TimestampUTC.Equal(testContent.TimestampUTC) {
		t.Errorf("Expected timestamp %v, got %v", testContent.TimestampUTC, readContent.TimestampUTC)
	}
	if readContent.Mode != testContent.Mode {
		t.Errorf("Expected mode %q, got %q", testContent.Mode, readContent.Mode)
	}
	if readContent.UUID != testContent.UUID {
		t.Errorf("Expected UUID %q, got %q", testContent.UUID, readContent.UUID)
	}
}

func TestReadNonExistentMetafile(t *testing.T) {
	tempDir := t.TempDir()
	_, err := Read(tempDir)
	if err == nil {
		t.Fatal("Expected an error when reading a non-existent metafile, but got nil")
	}
	if !os.IsNotExist(err) {
		t.Errorf("Expected os.IsNotExist error, got %v", err)
	}
}

func TestReadCorruptMetafile(t *testing.T) {
	tempDir := t.TempDir()
	metaFilePath := filepath.Join(tempDir, MetaFileName)
	// Write some invalid JSON to simulate corruption
	if err := os.WriteFile(metaFilePath, []byte("{invalid json"), 0644); err != nil {
		t.Fatalf("Failed to write corrupt metafile: %v", err)
	}

	_, err := Read(tempDir)
	if err == nil {
		t.Fatal("Expected an error when reading a corrupt metafile, but got nil")
	}
	if !strings.Contains(err.Error(), "could not parse metafile") {
		t.Errorf("Expected error about parsing metafile, got %v", err)
	}
}
