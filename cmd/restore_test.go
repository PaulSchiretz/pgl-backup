package cmd_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/cmd"
	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

func TestRunRestore_Interactive(t *testing.T) {
	// Setup directories
	baseDir := t.TempDir()
	targetDir := t.TempDir()

	// 1. Create Config
	cfg := config.NewDefault()
	if err := config.Generate(baseDir, cfg); err != nil {
		t.Fatalf("Failed to generate config: %v", err)
	}

	// 2. Create Dummy Backups
	// Backup A: Older
	pathA := filepath.Join(baseDir, "PGL_Backup_Incremental_Archive", "PGL_Backup_2023-01-01")
	if err := os.MkdirAll(filepath.Join(pathA, "PGL_Backup_Content"), 0755); err != nil {
		t.Fatal(err)
	}
	metafile.Write(pathA, &metafile.MetafileContent{
		TimestampUTC: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		Mode:         "incremental",
	})

	// Backup B: Newer (Should be listed first)
	pathB := filepath.Join(baseDir, "PGL_Backup_Incremental_Archive", "PGL_Backup_2023-02-01")
	if err := os.MkdirAll(filepath.Join(pathB, "PGL_Backup_Content"), 0755); err != nil {
		t.Fatal(err)
	}
	metafile.Write(pathB, &metafile.MetafileContent{
		TimestampUTC: time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC),
		Mode:         "incremental",
	})

	// 3. Mock Stdin/Stdout
	rIn, wIn, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	// Mock Stdout (to silence the menu output during test)
	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	origStdin := os.Stdin
	origStdout := os.Stdout

	defer func() {
		os.Stdin = origStdin
		os.Stdout = origStdout
	}()

	os.Stdin = rIn
	os.Stdout = wOut

	// Write "1\n" to stdin to select the first (newest) backup
	go func() {
		defer wIn.Close()
		io.WriteString(wIn, "1\n")
	}()

	// Drain stdout so the print calls don't block
	go func() {
		defer rOut.Close()
		io.Copy(io.Discard, rOut)
	}()

	// 4. Run Restore
	plog.SetOutput(io.Discard)

	flags := map[string]interface{}{
		"base":   baseDir,
		"target": targetDir,
		// "backup-name" is intentionally omitted to trigger interactive mode
	}

	err = cmd.RunRestore(context.Background(), flags)
	wOut.Close() // Close write end to finish copy goroutine

	if err != nil {
		t.Fatalf("RunRestore failed: %v", err)
	}
}

func TestRunRestore_Interactive_DefaultCancel(t *testing.T) {
	// Setup directories
	baseDir := t.TempDir()
	targetDir := t.TempDir()

	// 1. Create Config
	cfg := config.NewDefault()
	if err := config.Generate(baseDir, cfg); err != nil {
		t.Fatalf("Failed to generate config: %v", err)
	}

	// 2. Create Dummy Backup (at least one needed for list)
	pathA := filepath.Join(baseDir, "PGL_Backup_Incremental_Archive", "PGL_Backup_2023-01-01")
	if err := os.MkdirAll(filepath.Join(pathA, "PGL_Backup_Content"), 0755); err != nil {
		t.Fatal(err)
	}
	metafile.Write(pathA, &metafile.MetafileContent{
		TimestampUTC: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		Mode:         "incremental",
	})

	// 3. Mock Stdin/Stdout
	rIn, wIn, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	// Mock Stdout
	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	origStdin := os.Stdin
	origStdout := os.Stdout

	defer func() {
		os.Stdin = origStdin
		os.Stdout = origStdout
	}()

	os.Stdin = rIn
	os.Stdout = wOut

	// Write just a newline to stdin to trigger default (Cancel)
	go func() {
		defer wIn.Close()
		io.WriteString(wIn, "\n")
	}()

	// Drain stdout
	go func() {
		defer rOut.Close()
		io.Copy(io.Discard, rOut)
	}()

	// 4. Run Restore
	plog.SetOutput(io.Discard)

	flags := map[string]interface{}{
		"base":   baseDir,
		"target": targetDir,
		// "backup-name" is intentionally omitted to trigger interactive mode
	}

	err = cmd.RunRestore(context.Background(), flags)
	wOut.Close()

	if err != nil {
		t.Fatalf("RunRestore failed (expected nil for cancel): %v", err)
	}
}
