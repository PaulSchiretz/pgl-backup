package main

import (
	"context"
	"io"
	"os"
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
			name:          "Invalid Command",
			args:          []string{"pgl-backup", "invalid"},
			expectedError: "unknown command: invalid",
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
