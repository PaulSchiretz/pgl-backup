package flagparse_test

import (
	"strings"
	"testing"

	"github.com/paulschiretz/pgl-backup/pkg/flagparse"
)

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

func TestParseExcludeList(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []string
	}{
		{"Simple List", "a,b,c", []string{"a", "b", "c"}},
		{"List with Spaces", " a , b, c ", []string{"a", "b", "c"}},
		{"Empty String", "", nil},
		{"Quoted Item with Spaces", "'item with spaces',b", []string{"item with spaces", "b"}},
		{"Quoted Item with Comma", "'a,b',c", []string{"a,b", "c"}},
		{"Mixed Quoted and Unquoted", "a,'b,c',d", []string{"a", "b,c", "d"}},
		{"Unmatched Quote", "'a,b", []string{"a,b"}},
		{"Multiple Quoted Items", "'a b','c d'", []string{"a b", "c d"}},
		{"Double Quoted Item with Spaces", "\"item with spaces\",b", []string{"item with spaces", "b"}},
		{"Nested Quotes", "'a \"b\" c',d", []string{"a \"b\" c", "d"}},
		{"Nested Quotes 2", "\"it's a test\",d", []string{"it's a test", "d"}},
		{"Windows Path with Backslashes", `C:\Users\Test,D:\Data`, []string{`C:\Users\Test`, `D:\Data`}},
		{"Unix Path with Slashes", "/home/user/test,/var/log", []string{"/home/user/test", "/var/log"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := flagparse.ParseExcludeList(tc.input)

			// Handle the case where an empty input should result in a nil or empty slice.
			if len(tc.expected) == 0 && len(result) == 0 {
				// This is a pass, so we can return early.
				return
			}

			if !equalSlices(result, tc.expected) {
				t.Errorf("expected %v, but got %v", tc.expected, result)
			}
		})
	}
}

func TestParse(t *testing.T) {
	t.Run("No Arguments", func(t *testing.T) {
		act, _, err := flagparse.Parse([]string{})
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}
		if act != flagparse.None {
			t.Errorf("expected command to be None, but got %v", act)
		}
	})

	t.Run("Help Command", func(t *testing.T) {
		act, _, err := flagparse.Parse([]string{"help"})
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}
		if act != flagparse.None {
			t.Errorf("expected command to be None, but got %v", act)
		}
	})

	t.Run("Version Command", func(t *testing.T) {
		act, flagMap, err := flagparse.Parse([]string{"version"})
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}
		if act != flagparse.Version {
			t.Errorf("expected command to be Version, but got %v", act)
		}
		if len(flagMap) != 0 {
			t.Errorf("expected empty flag map, but got %v", flagMap)
		}
	})

	t.Run("Override Source and Target (Explicit Subcommand)", func(t *testing.T) {
		args := []string{"backup", "-source=/new/src", "-target=/new/dst"}
		_, setFlags, err := flagparse.Parse(args)
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

	t.Run("Missing Command (Flags only)", func(t *testing.T) {
		args := []string{"-source=/new/src", "-target=/new/dst"}
		_, _, err := flagparse.Parse(args)
		if err == nil {
			t.Fatal("expected error for missing command, got nil")
		}
		if !strings.Contains(err.Error(), "invalid command") {
			t.Errorf("expected error containing 'invalid command', got: %v", err)
		}
	})

	t.Run("Set Command Flags", func(t *testing.T) {
		testCases := []struct {
			name            string
			args            []string
			expectedCommand flagparse.Command
		}{
			{"Version Command", []string{"version"}, flagparse.Version},
			{"Init Command", []string{"init"}, flagparse.Init},
			{"Init Default Command", []string{"init", "-default"}, flagparse.Init},
			{"Prune Command", []string{"prune"}, flagparse.Prune},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				act, _, err := flagparse.Parse(tc.args)
				if err != nil {
					t.Fatalf("expected no error, but got: %v", err)
				}
				if act != tc.expectedCommand {
					t.Errorf("expected command %v, but got %v", tc.expectedCommand, act)
				}
			})
		}
	})

	t.Run("Case Insensitive Commands", func(t *testing.T) {
		testCases := []struct {
			name            string
			args            []string
			expectedCommand flagparse.Command
		}{
			{"Init Uppercase", []string{"INIT"}, flagparse.Init},
			{"Init Mixed", []string{"Init"}, flagparse.Init},
			{"Backup Mixed", []string{"BackUp"}, flagparse.Backup},
			{"Version Mixed", []string{"Version"}, flagparse.Version},
			{"Prune Mixed", []string{"Prune"}, flagparse.Prune},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				act, _, err := flagparse.Parse(tc.args)
				if err != nil {
					t.Fatalf("expected no error, but got: %v", err)
				}
				if act != tc.expectedCommand {
					t.Errorf("expected command %v, but got %v", tc.expectedCommand, act)
				}
			})
		}
	})

	t.Run("Parse Exclude Flags", func(t *testing.T) {
		// Using subcommand style
		args := []string{"backup", "-user-exclude-files=*.tmp,*.log", "-user-exclude-dirs=node_modules,.cache"}
		_, setFlags, err := flagparse.Parse(args)
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

	t.Run("Parse Hook Flags", func(t *testing.T) {
		args := []string{"backup", "-pre-backup-hooks=cmd1, 'cmd2 with space'", "-post-backup-hooks=cmd3"}
		_, setFlags, err := flagparse.Parse(args)
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

	t.Run("Set Sync Incremental Mod Time Window Flag", func(t *testing.T) {
		args := []string{"backup", "-sync-incremental-mod-time-window=2"}
		_, setFlags, err := flagparse.Parse(args)
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}
		val, ok := setFlags["sync-incremental-mod-time-window"]
		if !ok {
			t.Fatal("expected 'sync-incremental-mod-time-window' flag to be in setFlags map")
		}
		if intVal, typeOK := val.(int); !typeOK || intVal != 2 {
			t.Errorf("expected sync-incremental-mod-time-window to be 2, but got %v (type %T)", val, val)
		}
	})

	t.Run("Override Sync Incremental Preserve Source Dir Name", func(t *testing.T) {
		args := []string{"backup", "-sync-incremental-preserve-source-dir-name=false"}
		_, setFlags, err := flagparse.Parse(args)
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}
		val, ok := setFlags["sync-incremental-preserve-source-dir-name"]
		if !ok {
			t.Fatal("expected 'sync-incremental-preserve-source-dir-name' flag to be in setFlags map")
		}
		if boolVal, typeOK := val.(bool); !typeOK || boolVal != false {
			t.Errorf("expected SyncIncrementalPreserveSourceDirName to be false, but got %v (type %T)", val, val)
		}
	})

	t.Run("Set Log Level Flag", func(t *testing.T) {
		args := []string{"backup", "-log-level=debug"}
		_, setFlags, err := flagparse.Parse(args)
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

	t.Run("Set Metrics Flag", func(t *testing.T) {
		args := []string{"backup", "-metrics"}
		_, setFlags, err := flagparse.Parse(args)
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

	t.Run("Set Init Default Flag", func(t *testing.T) {
		// New style: init -default
		args := []string{"init", "-default"}
		act, _, err := flagparse.Parse(args)
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}
		if act != flagparse.Init {
			t.Errorf("expected command to be Init, got %v", act)
		}
	})

	t.Run("Set Force Flag", func(t *testing.T) {
		args := []string{"init", "-force"}
		_, setFlags, err := flagparse.Parse(args)
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

	t.Run("Parse Compression Flags", func(t *testing.T) {
		args := []string{
			"backup",
			"-compression-incremental",
			"-compression-incremental-format=tar.gz",
		}
		_, setFlags, err := flagparse.Parse(args)
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}

		// Check compression enabled
		if val, ok := setFlags["compression-incremental"]; !ok || !val.(bool) {
			t.Errorf("expected compression-incremental to be true, but got %v", val)
		}

		// Check compression format
		if val, ok := setFlags["compression-incremental-format"]; !ok || val.(string) != "tar.gz" {
			t.Errorf("expected compression-incremental-format to be 'tar.gz', but got %v", val.(string))
		}

		args = []string{
			"backup",
			"-compression-snapshot",
			"-compression-snapshot-format=tar.gz",
		}
		_, setFlags, err = flagparse.Parse(args)
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}

		// Check compression enabled
		if val, ok := setFlags["compression-snapshot"]; !ok || !val.(bool) {
			t.Errorf("expected compression-snapshot to be true, but got %v", val)
		}

		// Check compression format
		if val, ok := setFlags["compression-snapshot-format"]; !ok || val.(string) != "tar.gz" {
			t.Errorf("expected compression-snapshot-format to be 'tar.gz', but got %v", val)
		}
	})

	t.Run("Set Archive Interval Flag", func(t *testing.T) {
		args := []string{"backup", "-archive-incremental-interval-seconds=172800"} // 48h
		_, setFlags, err := flagparse.Parse(args)
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}
		val, ok := setFlags["archive-incremental-interval-seconds"]
		if !ok {
			t.Fatal("expected 'archive-incremental-interval-seconds' flag to be in setFlags map")
		}
		expectedSeconds := 172800
		if intVal, typeOK := val.(int); !typeOK || intVal != expectedSeconds {
			t.Errorf("expected archive-incremental-interval-seconds to be %v, but got %v (type %T)", expectedSeconds, val, val)
		}
	})

	t.Run("Set Archive Interval Mode Flag", func(t *testing.T) {
		args := []string{"backup", "-archive-incremental-interval-mode=manual"}
		_, setFlags, err := flagparse.Parse(args)
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}
		val, ok := setFlags["archive-incremental-interval-mode"]
		if !ok {
			t.Fatal("expected 'archive-incremental-interval-mode' flag to be in setFlags map")
		}
		if modeVal, typeOK := val.(string); !typeOK || modeVal != "manual" {
			t.Errorf("expected archive-incremental-interval-mode to be 'manual', but got %v", val)
		}
	})

	t.Run("Unknown Command", func(t *testing.T) {
		args := []string{"invalid-command", "-target=/tmp"}
		_, _, err := flagparse.Parse(args)
		if err == nil {
			t.Fatal("expected error for unknown command, got nil")
		}
		if !strings.Contains(err.Error(), "invalid command") {
			t.Errorf("expected error containing 'invalid command', got: %v", err)
		}
	})

	t.Run("Invalid Flag for Subcommand", func(t *testing.T) {
		// This test would exit the process if we were using flag.ExitOnError
		args := []string{"backup", "-non-existent-flag"}
		_, _, err := flagparse.Parse(args)
		if err == nil {
			t.Fatal("expected error for invalid flag, got nil")
		}
		if !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Errorf("expected error about undefined flag, got: %v", err)
		}
	})
}

func TestParseCmdList(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []string
	}{
		{"Simple List", "cmd1,cmd2", []string{"cmd1", "cmd2"}},
		{"Quoted Item with Spaces", "'echo hello',cmd2", []string{"'echo hello'", "cmd2"}},
		{"Quoted Item with Comma", "'echo a,b',c", []string{"'echo a,b'", "c"}},
		{"Unmatched Quote", "'a,b", []string{"'a,b"}},
		{"Multiple Quoted Items", "'a b','c d'", []string{"'a b'", "'c d'"}},
		{"Double Quoted Item with Spaces", "\"item with spaces\",b", []string{"\"item with spaces\"", "b"}},
		{"Mixed Single and Double Quotes", "'a b',\"c,d\",e", []string{"'a b'", "\"c,d\"", "e"}},
		{"Nested Quotes", "'a \"b\" c',d", []string{"'a \"b\" c'", "d"}},
		{"Escaped Single Quote Inside Single Quotes", "'hello\\'world',next", []string{"'hello\\'world'", "next"}},
		{"Escaped Double Quote Inside Double Quotes", "\"hello\\\"world\",next", []string{"\"hello\\\"world\"", "next"}},
		{"Escaped Comma Outside Quotes", "a\\,b,c", []string{"a\\,b", "c"}},
		{"Escaped Backslash", "'a\\\\b',c", []string{"'a\\\\b'", "c"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := flagparse.ParseCmdList(tc.input)

			// Handle the case where an empty input should result in a nil or empty slice.
			if len(tc.expected) == 0 && len(result) == 0 {
				// This is a pass, so we can return early.
				return
			}

			if !equalSlices(result, tc.expected) {
				t.Errorf("expected %v, but got %v", tc.expected, result)
			}
		})
	}
}
