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
	"github.com/paulschiretz/pgl-backup/pkg/hints"
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

func TestPromptBackupSelection(t *testing.T) {
	// Silence logs
	plog.SetOutput(io.Discard)

	now := time.Now()
	backups := []metafile.MetafileInfo{
		{
			RelPathKey: "path/to/backup_1",
			Metadata: metafile.MetafileContent{
				TimestampUTC: now,
				Mode:         "incremental",
			},
		},
		{
			RelPathKey: "path/to/backup_2",
			Metadata: metafile.MetafileContent{
				TimestampUTC: now.Add(-1 * time.Hour),
				Mode:         "snapshot",
			},
		},
	}

	tests := []struct {
		name           string
		input          string
		expectedResult string
		expectHint     bool
	}{
		{
			name:           "Select First",
			input:          "1\n",
			expectedResult: "backup_1",
		},
		{
			name:           "Select Second",
			input:          "2\n",
			expectedResult: "backup_2",
		},
		{
			name:           "Cancel via Option",
			input:          "3\n",
			expectedResult: "",
			expectHint:     true,
		},
		{
			name:           "Cancel via Default (Enter)",
			input:          "\n",
			expectedResult: "",
			expectHint:     true,
		},
		{
			name:           "Invalid Input Retry",
			input:          "invalid\n1\n",
			expectedResult: "backup_1",
		},
		{
			name:           "Out of Range Retry",
			input:          "0\n4\n1\n",
			expectedResult: "backup_1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Mock Stdin/Stdout
			rIn, wIn, _ := os.Pipe()
			rOut, wOut, _ := os.Pipe()

			origStdin := os.Stdin
			origStdout := os.Stdout
			defer func() {
				os.Stdin = origStdin
				os.Stdout = origStdout
			}()
			os.Stdin = rIn
			os.Stdout = wOut

			// Write input in a goroutine
			go func() {
				defer wIn.Close()
				io.WriteString(wIn, tc.input)
			}()

			// Consume output in a goroutine to prevent blocking
			go func() {
				defer rOut.Close()
				io.Copy(io.Discard, rOut)
			}()

			result, err := cmd.PromptBackupSelection(backups)
			wOut.Close() // Ensure stdout is closed

			if tc.expectHint {
				if err == nil {
					t.Fatal("Expected hint error, got nil")
				}
				if !hints.IsHint(err) {
					t.Fatalf("Expected hint error, got: %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}

			if result != tc.expectedResult {
				t.Errorf("Expected result %q, got %q", tc.expectedResult, result)
			}
		})
	}
}
