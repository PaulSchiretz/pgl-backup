package main

import (
	"bytes"
	"flag"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/paulschiretz/pgl-backup/pkg/config"
)

// runTestWithFlags is a helper to safely run tests that use the global flag package.
// It backs up and restores os.Args and resets the flag package for each run.
func runTestWithFlags(t *testing.T, args []string, testFunc func()) {
	t.Helper()

	// 1. Backup original os.Args and defer restoration.
	originalArgs := os.Args
	defer func() { os.Args = originalArgs }()

	// 2. Set os.Args for this specific test case.
	// The first element must be the program name.
	os.Args = append([]string{t.Name()}, args...)

	// 3. Reset the flag package to a clean state.
	// This is crucial because the flag package is global.
	flag.CommandLine = flag.NewFlagSet(t.Name(), flag.ContinueOnError)

	// 4. Run the actual test function.
	testFunc()
}

func TestParseFlagConfig(t *testing.T) {
	t.Run("No Flags - Returns Zero-Value Config", func(t *testing.T) {
		runTestWithFlags(t, []string{}, func() {
			act, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}
			if act != actionRunBackup {
				t.Errorf("expected action to be actionRunBackup, but got %v", act)
			}
			if len(setFlags) != 0 {
				t.Errorf("expected no flags to be set, but got %d", len(setFlags))
			}
		})
	})

	t.Run("Override Source and Target", func(t *testing.T) {
		args := []string{"-source=/new/src", "-target=/new/dst"}
		runTestWithFlags(t, args, func() {
			_, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}
			if val, ok := setFlags["source"]; !ok {
				t.Error("expected 'source' flag to be in setFlags map")
			} else if val != "/new/src" {
				t.Errorf("expected source to be '/new/src', but got %v", val)
			}

			if val, ok := setFlags["target"]; !ok {
				t.Error("expected 'target' flag to be in setFlags map")
			} else if val != "/new/dst" {
				t.Errorf("expected target to be '/new/dst', but got %v", val)
			}
		})
	})

	t.Run("Set Action Flags", func(t *testing.T) {
		testCases := []struct {
			name           string
			arg            string
			expectedAction action
		}{
			{"Version Flag", "-version", actionShowVersion},
			{"Init Flag", "-init", actionInitConfig},
			{"Init Default Flag", "-init-default", actionInitConfig},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				runTestWithFlags(t, []string{tc.arg}, func() {
					act, _, err := parseFlagConfig()
					if err != nil {
						t.Fatalf("expected no error, but got: %v", err)
					}
					if act != tc.expectedAction {
						t.Errorf("expected action %v, but got %v", tc.expectedAction, act)
					}
				})
			})
		}
	})

	t.Run("Parse Exclude Flags", func(t *testing.T) {
		args := []string{"-user-exclude-files=*.tmp,*.log", "-user-exclude-dirs=node_modules,.cache"}
		runTestWithFlags(t, args, func() {
			_, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}

			expectedFiles := []string{"*.tmp", "*.log"}
			if !equalSlices(setFlags["user-exclude-files"].([]string), expectedFiles) {
				t.Errorf("expected exclude files %v, but got %v", expectedFiles, setFlags["user-exclude-files"])
			}

			expectedDirs := []string{"node_modules", ".cache"}
			if !equalSlices(setFlags["user-exclude-dirs"].([]string), expectedDirs) {
				t.Errorf("expected exclude dirs %v, but got %v", expectedDirs, setFlags["user-exclude-dirs"])
			}
		})
	})

	t.Run("Parse Hook Flags", func(t *testing.T) {
		args := []string{"-pre-backup-hooks=cmd1, 'cmd2 with space'", "-post-backup-hooks=cmd3"}
		runTestWithFlags(t, args, func() {
			_, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}

			expectedPre := []string{"cmd1", "'cmd2 with space'"}
			if !equalSlices(setFlags["pre-backup-hooks"].([]string), expectedPre) {
				t.Errorf("expected pre-backup hooks %v, but got %v", expectedPre, setFlags["pre-backup-hooks"])
			}

			expectedPost := []string{"cmd3"}
			if !equalSlices(setFlags["post-backup-hooks"].([]string), expectedPost) {
				t.Errorf("expected post-backup hooks %v, but got %v", expectedPost, setFlags["post-backup-hooks"])
			}
		})
	})

	t.Run("Set Mod Time Window Flag", func(t *testing.T) {
		args := []string{"-mod-time-window=2"}
		runTestWithFlags(t, args, func() {
			_, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}
			val, ok := setFlags["mod-time-window"]
			if !ok {
				t.Fatal("expected 'mod-time-window' flag to be in setFlags map")
			}
			if intVal, typeOK := val.(int); !typeOK || intVal != 2 {
				t.Errorf("expected mod-time-window to be 2, but got %v (type %T)", val, val)
			}
		})
	})

	t.Run("Override PreserveRoot", func(t *testing.T) {
		args := []string{"-preserve-source-name=false"}
		runTestWithFlags(t, args, func() {
			_, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}
			val, ok := setFlags["preserve-source-name"]
			if !ok {
				t.Fatal("expected 'preserve-source-name' flag to be in setFlags map")
			}
			if boolVal, typeOK := val.(bool); !typeOK || boolVal != false {
				t.Errorf("expected PreserveRoot to be false, but got %v (type %T)", val, val)
			}
		})
	})

	t.Run("Set Log Level Flag", func(t *testing.T) {
		args := []string{"-log-level=debug"}
		runTestWithFlags(t, args, func() {
			_, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}
			val, ok := setFlags["log-level"]
			if !ok {
				t.Fatal("expected 'log-level' flag to be in setFlags map")
			}
			if strVal, typeOK := val.(string); !typeOK || strVal != "debug" {
				t.Errorf("expected log-level to be 'debug', but got %v (type %T)", val, val)
			}
		})
	})

	t.Run("Set Metrics Flag", func(t *testing.T) {
		args := []string{"-metrics"}
		runTestWithFlags(t, args, func() {
			_, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}
			val, ok := setFlags["metrics"]
			if !ok {
				t.Fatal("expected 'metrics' flag to be in setFlags map")
			}
			if boolVal, typeOK := val.(bool); !typeOK || !boolVal {
				t.Errorf("expected metrics to be true, but got %v (type %T)", val, val)
			}
		})
	})

	t.Run("Set Init Default Flag", func(t *testing.T) {
		args := []string{"-init-default"}
		runTestWithFlags(t, args, func() {
			_, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}
			val, ok := setFlags["init-default"]
			if !ok {
				t.Fatal("expected 'init-default' flag to be in setFlags map")
			}
			if boolVal, typeOK := val.(bool); !typeOK || !boolVal {
				t.Errorf("expected init-default to be true, but got %v (type %T)", val, val)
			}
		})
	})

	t.Run("Set Force Flag", func(t *testing.T) {
		args := []string{"-force"}
		runTestWithFlags(t, args, func() {
			_, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}
			val, ok := setFlags["force"]
			if !ok {
				t.Fatal("expected 'force' flag to be in setFlags map")
			}
			if boolVal, typeOK := val.(bool); !typeOK || !boolVal {
				t.Errorf("expected force to be true, but got %v (type %T)", val, val)
			}
		})
	})

	t.Run("Invalid Mode Flag", func(t *testing.T) {
		args := []string{"-mode=invalid-mode"}
		runTestWithFlags(t, args, func() {
			_, _, err := parseFlagConfig()
			if err == nil {
				t.Fatal("expected an error for invalid mode, but got nil")
			}
			if !strings.Contains(err.Error(), "invalid BackupMode") {
				t.Errorf("expected error to contain 'invalid BackupMode', but got: %v", err)
			}
		})
	})

	t.Run("Invalid Sync Engine Flag", func(t *testing.T) {
		args := []string{"-sync-engine=invalid-engine"}
		runTestWithFlags(t, args, func() {
			_, _, err := parseFlagConfig()
			if err == nil {
				t.Fatal("expected an error for invalid sync engine, but got nil")
			}
			if !strings.Contains(err.Error(), "invalid SyncEngine") {
				t.Errorf("expected error to contain 'invalid SyncEngine', but got: %v", err)
			}
		})
	})

	t.Run("Parse Compression Flags", func(t *testing.T) {
		args := []string{
			"-compression",
			"-compression-format=tar.gz",
		}
		runTestWithFlags(t, args, func() {
			_, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}

			// Check compression enabled
			if val, ok := setFlags["compression"]; !ok || !val.(bool) {
				t.Errorf("expected incremental-compression to be true, but got %v", val)
			}

			// Check compression format
			if val, ok := setFlags["compression-format"]; !ok || string(val.(config.CompressionFormat)) != "tar.gz" {
				t.Errorf("expected compression-format to be 'tar.gz', but got %v", val.(config.CompressionFormat))
			}
		})
	})

	t.Run("Set Archive Interval Flag", func(t *testing.T) {
		args := []string{"-archive-interval-seconds=172800"} // 48h
		runTestWithFlags(t, args, func() {
			_, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}
			val, ok := setFlags["archive-interval-seconds"]
			if !ok {
				t.Fatal("expected 'archive-interval-seconds' flag to be in setFlags map")
			}
			expectedSeconds := 172800
			if intVal, typeOK := val.(int); !typeOK || intVal != expectedSeconds {
				t.Errorf("expected archive-interval-seconds to be %v, but got %v (type %T)", expectedSeconds, val, val)
			}
		})
	})

	t.Run("Set Archive Interval Mode Flag", func(t *testing.T) {
		args := []string{"-archive-interval-mode=manual"}
		runTestWithFlags(t, args, func() {
			_, setFlags, err := parseFlagConfig()
			if err != nil {
				t.Fatalf("expected no error, but got: %v", err)
			}
			val, ok := setFlags["archive-interval-mode"]
			if !ok {
				t.Fatal("expected 'archive-interval-mode' flag to be in setFlags map")
			}
			if modeVal, typeOK := val.(config.ArchiveIntervalMode); !typeOK || modeVal != config.ManualInterval {
				t.Errorf("expected archive-interval-mode to be ManualInterval, but got %v", val)
			}
		})
	})
}

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
		result := promptForConfirmation(prompt, defaultYes)

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

// equalSlices is a helper to compare two string slices for equality.
func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
