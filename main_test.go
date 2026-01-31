package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

func TestMain(m *testing.M) {
	// Silence logs during tests to keep output clean
	plog.SetOutput(io.Discard)
	os.Exit(m.Run())
}

func TestRun(t *testing.T) {
	// Save original args and restore after test
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	tmpDir := t.TempDir()

	tests := []struct {
		name          string
		args          []string
		setup         func(*testing.T)
		expectedError string
	}{
		{
			name: "No Arguments (Help)",
			args: []string{"pgl-backup"},
		},
		{
			name: "Help Flag",
			args: []string{"pgl-backup", "--help"},
		},
		{
			name:          "Invalid Command",
			args:          []string{"pgl-backup", "invalid"},
			expectedError: "invalid command",
		},
		{
			name: "Init Existing Empty Directory",
			setup: func(t *testing.T) {
				if err := os.MkdirAll(filepath.Join(tmpDir, "src"), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Join(tmpDir, "existing_empty"), 0755); err != nil {
					t.Fatal(err)
				}
			},
			args: []string{"pgl-backup", "init", "-base", filepath.Join(tmpDir, "existing_empty"), "-source", filepath.Join(tmpDir, "src")},
		},
		{
			name:          "Prune Missing Base",
			args:          []string{"pgl-backup", "prune"},
			expectedError: "the -base flag is required",
		},
		{
			name:          "Prune Non-Existent Base",
			args:          []string{"pgl-backup", "prune", "-base", filepath.Join(os.TempDir(), "pgl_nonexistent_target_12345")},
			expectedError: "base path",
		},
		{
			name:          "Backup Missing Base",
			args:          []string{"pgl-backup", "backup", "-source", tmpDir},
			expectedError: "the -base flag is required",
		},
		{
			name:          "Backup Missing Source",
			args:          []string{"pgl-backup", "backup", "-base", tmpDir},
			expectedError: "the -source flag is required",
		},
		{
			name:          "Backup Non-Existent Source",
			args:          []string{"pgl-backup", "backup", "-base", tmpDir, "-source", filepath.Join(tmpDir, "nonexistent")},
			expectedError: "source path",
		},
		{
			name:          "Restore Missing Base",
			args:          []string{"pgl-backup", "restore", "-target", tmpDir, "-backup-name", "current", "-mode", "incremental"},
			expectedError: "the -base flag is required",
		},
		{
			name:          "Restore Missing Target",
			args:          []string{"pgl-backup", "restore", "-base", tmpDir, "-backup-name", "current", "-mode", "incremental"},
			expectedError: "the -target flag is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != nil {
				tc.setup(t)
			}
			os.Args = tc.args

			err := run(context.Background())

			if tc.expectedError != "" {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tc.expectedError)
				} else if !strings.Contains(err.Error(), tc.expectedError) {
					t.Errorf("expected error containing %q, got %v", tc.expectedError, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			}
		})
	}
}
