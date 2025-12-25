package main

import (
	"flag"
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
