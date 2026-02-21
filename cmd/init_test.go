package cmd_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/paulschiretz/pgl-backup/cmd"
	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/lockfile"
)

func TestPromptForConfirmation(t *testing.T) {
	// Helper to mock stdin/stdout and run the function
	mockPrompt := func(input string, prompt string, defaultYes bool) (bool, string) {
		// Pipe for stdin
		rIn, wIn, _ := os.Pipe()
		// Pipe for stdout
		rOut, wOut, _ := os.Pipe()

		// Save original stdin/stdout
		origStdin := os.Stdin
		origStdout := os.Stdout
		defer func() {
			os.Stdin = origStdin
			os.Stdout = origStdout
		}()

		// Redirect
		os.Stdin = rIn
		os.Stdout = wOut

		// Write input
		go func() {
			_, _ = wIn.WriteString(input)
			_ = wIn.Close()
		}()

		// Run the function
		result := cmd.PromptForConfirmation(prompt, defaultYes)

		// Close writer to read output
		_ = wOut.Close()
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, rOut)

		return result, buf.String()
	}

	tests := []struct {
		name       string
		input      string
		prompt     string
		defaultYes bool
		want       bool
		wantPrompt string
	}{
		{"Explicit Yes", "y\n", "Continue?", false, true, "Continue? [y/N]: "},
		{"Explicit No", "n\n", "Continue?", true, false, "Continue? [Y/n]: "},
		{"Default Yes (Empty)", "\n", "Sure?", true, true, "Sure? [Y/n]: "},
		{"Default No (Empty)", "\n", "Sure?", false, false, "Sure? [y/N]: "},
		{"Case Insensitive", "YES\n", "Go?", false, true, "Go? [y/N]: "},
		{"Whitespace Handling", "   y   \n", "Clean?", false, true, "Clean? [y/N]: "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, output := mockPrompt(tt.input, tt.prompt, tt.defaultYes)
			if got != tt.want {
				t.Errorf("promptForConfirmation() = %v, want %v", got, tt.want)
			}
			if !strings.Contains(output, tt.wantPrompt) {
				t.Errorf("Output = %q, want substring %q", output, tt.wantPrompt)
			}
		})
	}
}

func TestRunInit(t *testing.T) {
	// Create a valid source directory for all tests to use
	srcDir := t.TempDir()

	tests := []struct {
		name          string
		flags         map[string]any
		preSetup      func(t *testing.T, baseDir string) context.CancelFunc
		stdinInput    string
		expectError   bool
		errorContains string
		validate      func(t *testing.T, baseDir string)
	}{
		{
			name: "Happy Path - Success",
			flags: map[string]any{
				"source": srcDir,
			},
			validate: func(t *testing.T, baseDir string) {
				// Verify config file created
				confPath := filepath.Join(baseDir, config.ConfigFileName)
				if _, err := os.Stat(confPath); os.IsNotExist(err) {
					t.Error("expected config file to be created, but it was not")
				}
				// Verify lock file removed
				lockPath := filepath.Join(baseDir, lockfile.LockFileName)
				if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
					t.Error("expected lock file to be removed, but it exists")
				}
			},
		},
		{
			name: "Failure - Locked Directory",
			flags: map[string]any{
				"source": srcDir,
			},
			preSetup: func(t *testing.T, targetDir string) context.CancelFunc {
				// Ensure target exists
				if err := os.MkdirAll(targetDir, 0755); err != nil {
					t.Fatalf("failed to create target dir: %v", err)
				}
				// Acquire lock externally to simulate another process
				ctx, cancel := context.WithCancel(context.Background())
				_, err := lockfile.Acquire(ctx, targetDir, "external-process")
				if err != nil {
					cancel()
					t.Fatalf("failed to acquire setup lock: %v", err)
				}
				return cancel
			},
			expectError:   true,
			errorContains: "failed to acquire lock",
		},
		{
			name: "Dry Run - No Changes",
			flags: map[string]any{
				"source":  srcDir,
				"dry-run": true,
			},
			validate: func(t *testing.T, targetDir string) {
				// Verify config file NOT created
				confPath := filepath.Join(targetDir, config.ConfigFileName)
				if _, err := os.Stat(confPath); !os.IsNotExist(err) {
					t.Error("expected config file NOT to be created in dry run")
				}
				// Verify lock file NOT present
				lockPath := filepath.Join(targetDir, lockfile.LockFileName)
				if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
					t.Error("expected lock file not to exist")
				}
			},
		},
		{
			name: "Failure - Source Does Not Exist",
			flags: map[string]any{
				"source": filepath.Join(srcDir, "nonexistent"),
			},
			expectError:   true,
			errorContains: "does not exist",
		},
		{
			name: "Overwrite with Default + Force",
			flags: map[string]any{
				"source":  srcDir,
				"default": true,
				"force":   true,
			},
			preSetup: func(t *testing.T, baseDir string) context.CancelFunc {
				if err := os.MkdirAll(baseDir, 0755); err != nil {
					t.Fatalf("failed to create target dir: %v", err)
				}
				cfg := config.Default()
				cfg.LogLevel = "debug"
				if err := config.Generate(baseDir, cfg); err != nil {
					t.Fatalf("failed to create existing config: %v", err)
				}
				return nil
			},
			validate: func(t *testing.T, baseDir string) {
				cfg, err := config.Load(baseDir)
				if err != nil {
					t.Fatalf("failed to load config: %v", err)
				}
				if cfg.LogLevel != "info" {
					t.Errorf("expected LogLevel to be 'info' (default), got '%s'", cfg.LogLevel)
				}
			},
		},
		{
			name: "Overwrite with Default (Interactive Yes)",
			flags: map[string]any{
				"source":  srcDir,
				"default": true,
			},
			stdinInput: "y\n",
			preSetup: func(t *testing.T, baseDir string) context.CancelFunc {
				if err := os.MkdirAll(baseDir, 0755); err != nil {
					t.Fatalf("failed to create target dir: %v", err)
				}
				cfg := config.Default()
				cfg.LogLevel = "debug"
				if err := config.Generate(baseDir, cfg); err != nil {
					t.Fatalf("failed to create existing config: %v", err)
				}
				return nil
			},
			validate: func(t *testing.T, targetDir string) {
				cfg, err := config.Load(targetDir)
				if err != nil {
					t.Fatalf("failed to load config: %v", err)
				}
				if cfg.LogLevel != "info" {
					t.Errorf("expected LogLevel to be 'info' (overwritten), got '%s'", cfg.LogLevel)
				}
			},
		},
		{
			name: "Overwrite with Default (Interactive No)",
			flags: map[string]any{
				"source":  srcDir,
				"default": true,
			},
			stdinInput: "n\n",
			preSetup: func(t *testing.T, baseDir string) context.CancelFunc {
				if err := os.MkdirAll(baseDir, 0755); err != nil {
					t.Fatalf("failed to create target dir: %v", err)
				}
				cfg := config.Default()
				cfg.LogLevel = "debug"
				if err := config.Generate(baseDir, cfg); err != nil {
					t.Fatalf("failed to create existing config: %v", err)
				}
				return nil
			},
			validate: func(t *testing.T, targetDir string) {
				cfg, err := config.Load(targetDir)
				if err != nil {
					t.Fatalf("failed to load config: %v", err)
				}
				if cfg.LogLevel != "debug" {
					t.Errorf("expected LogLevel to remain 'debug', got '%s'", cfg.LogLevel)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			// Inject base into flags
			if tc.flags == nil {
				tc.flags = make(map[string]any)
			}
			tc.flags["base"] = baseDir

			if tc.stdinInput != "" {
				r, w, err := os.Pipe()
				if err != nil {
					t.Fatalf("failed to create pipe: %v", err)
				}
				origStdin := os.Stdin
				os.Stdin = r
				defer func() { os.Stdin = origStdin }()

				go func() {
					defer w.Close()
					io.WriteString(w, tc.stdinInput)
				}()
			}

			if tc.preSetup != nil {
				cancel := tc.preSetup(t, baseDir)
				if cancel != nil {
					defer cancel()
				}
			}

			err := cmd.RunInit(context.Background(), tc.flags)

			if tc.expectError {
				if err == nil {
					t.Error("expected error, but got nil")
				} else if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain %q, but got: %v", tc.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}

			if tc.validate != nil {
				tc.validate(t, baseDir)
			}
		})
	}
}
