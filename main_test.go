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

	tests := []struct {
		name          string
		args          []string
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
			name:          "Prune Missing Target",
			args:          []string{"pgl-backup", "prune"},
			expectedError: "the -target flag is required",
		},
		{
			name:          "Prune Non-Existent Target",
			args:          []string{"pgl-backup", "prune", "-target", filepath.Join(os.TempDir(), "pgl_nonexistent_target_12345")},
			expectedError: "target directory does not exist",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
