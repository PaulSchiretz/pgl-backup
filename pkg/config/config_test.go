package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfig_Validate(t *testing.T) {
	// Helper to get a valid base config for testing
	newValidConfig := func(t *testing.T) Config {
		cfg := NewDefault()
		// Create a temporary source directory so validation passes
		srcDir := t.TempDir()
		cfg.Paths.Source = srcDir
		cfg.Paths.TargetBase = t.TempDir()
		return cfg
	}

	t.Run("Valid Config", func(t *testing.T) {
		cfg := newValidConfig(t)
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected valid config to pass validation, but got error: %v", err)
		}
	})

	t.Run("Empty Source Path", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Paths.Source = ""
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for empty source path, but got nil")
		}
	})

	t.Run("Non-Existent Source Path", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Paths.Source = filepath.Join(t.TempDir(), "nonexistent")
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for non-existent source path, but got nil")
		}
	})

	t.Run("Empty Target Path", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Paths.TargetBase = ""
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for empty target path, but got nil")
		}
	})

	t.Run("Invalid SyncWorkers", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Engine.Performance.SyncWorkers = 0
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for zero sync workers, but got nil")
		}
	})

	t.Run("Invalid MirrorWorkers", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Engine.Performance.MirrorWorkers = 0
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for zero mirror workers, but got nil")
		}
	})

	t.Run("Invalid CompressWorkers", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Engine.Performance.CompressWorkers = 0
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for zero compress workers, but got nil")
		}
	})

	t.Run("Invalid DeleteWorkers", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Engine.Performance.DeleteWorkers = 0
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for zero delete workers, but got nil")
		}
	})

	t.Run("Invalid RetryCount", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Engine.RetryCount = -1
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for negative retry count, but got nil")
		}
	})

	t.Run("Invalid RetryWaitSeconds", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Engine.RetryWaitSeconds = -1
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for negative retry wait, but got nil")
		}
	})

	t.Run("Invalid ArchiveInterval", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Mode = IncrementalMode

		// Test negative interval in manual mode (should error)
		cfg.Archive.Incremental.Mode = ManualInterval
		cfg.Archive.Incremental.Interval = -1 * time.Hour
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for negative archive interval in manual mode, but got nil")
		}

		// Test zero interval in manual mode (should NOT error, as it disables archive)
		cfg.Archive.Incremental.Interval = 0
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected no error for zero archive interval in manual mode (disables archive), but got: %v", err)
		}

		// In auto mode, the interval is calculated, so a user-set 0 should not error.
		cfg.Archive.Incremental.Mode = AutoInterval
		cfg.Archive.Incremental.Interval = 0
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected no error for zero archive interval in auto mode (value is ignored), but got: %v", err)
		}
	})

	t.Run("Invalid Incremental SubDirs", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Mode = IncrementalMode // This validation only applies in incremental mode

		// Test ArchivesSubDir empty
		cfg.Paths.ArchivesSubDir = ""
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for empty archivesSubDir, but got nil")
		}

		// Test ArchivesSubDir with path separators
		cfg.Paths.ArchivesSubDir = "invalid/path"
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for archivesSubDir with path separators, but got nil")
		}
		cfg.Paths.ArchivesSubDir = "valid" // Reset

		// Test empty IncrementalSubDir
		cfg.Paths.IncrementalSubDir = ""
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for empty IncrementalSubDir, but got nil")
		}

		// Test IncrementalSubDir
		cfg.Paths.IncrementalSubDir = "invalid/path"
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for incrementalSubDir with path separators, but got nil")
		}

		cfg.Paths.IncrementalSubDir = "valid" // Reset to valid
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected no error for valid archivesSubDir and incrementalSubDir, but got: %v", err)
		}
	})

	t.Run("Invalid SnapshotsSubDir", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Mode = SnapshotMode // This validation only applies in snapshot mode

		// Test empty
		cfg.Paths.SnapshotsSubDir = ""
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for empty snapshotsSubDir, but got nil")
		}

		// Test with path separators
		cfg.Paths.SnapshotsSubDir = "invalid/path"
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for snapshotsSubDir with path separators, but got nil")
		}

		cfg.Paths.SnapshotsSubDir = "valid" // Reset to valid
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected no error for valid snapshotsSubDir, but got: %v", err)
		}
	})

	t.Run("Invalid Compression MaxRetries", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Compression.Incremental.MaxRetries = -1
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for negative incremental compression max retries, but got nil")
		}

		cfg = newValidConfig(t) // reset
		cfg.Compression.Snapshot.MaxRetries = -1
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for negative snapshot compression max retries, but got nil")
		}
	})

	t.Run("Invalid Glob Pattern", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Paths.UserExcludeFiles = []string{"["} // Invalid glob pattern
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for invalid glob pattern, but got nil")
		}
	})
}

func TestMergeConfigWithFlags(t *testing.T) {
	t.Run("Flag overrides base config", func(t *testing.T) {
		base := NewDefault() // base.LogLevel is "info"
		setFlags := map[string]interface{}{"log-level": "debug"}

		merged := MergeConfigWithFlags(base, setFlags)
		if merged.LogLevel != "debug" {
			t.Error("expected flag 'log-level=debug' to override base 'info'")
		}
	})

	t.Run("Base config is used when flag is not set", func(t *testing.T) {
		base := NewDefault()
		base.LogLevel = "warn" // Set a non-default base value
		setFlags := map[string]interface{}{}

		merged := MergeConfigWithFlags(base, setFlags)
		if merged.LogLevel != "warn" {
			t.Error("expected base 'log-level=warn' to be used when flag is not set")
		}
	})

	t.Run("Flag explicitly set to default overrides base", func(t *testing.T) {
		base := NewDefault()
		base.Paths.PreserveSourceDirectoryName = false // Non-default base
		setFlags := map[string]interface{}{"preserve-source-name": true}

		merged := MergeConfigWithFlags(base, setFlags)
		if !merged.Paths.PreserveSourceDirectoryName {
			t.Error("expected flag 'preserve-source-name=true' to override base 'false'")
		}
	})

	t.Run("Slice flags override base slice", func(t *testing.T) {
		base := NewDefault()
		base.Paths.UserExcludeFiles = []string{"from_base.txt"}
		setFlags := map[string]interface{}{"user-exclude-files": []string{"from_flag.txt"}}

		merged := MergeConfigWithFlags(base, setFlags)
		finalExcludes := merged.Paths.ExcludeFiles()

		foundFlagValue := false
		foundBaseValue := false
		for _, p := range finalExcludes {
			if p == "from_flag.txt" {
				foundFlagValue = true
			}
			if p == "from_base.txt" {
				foundBaseValue = true
			}
		}

		if !foundFlagValue {
			t.Error("expected final exclusion list to contain value from flag, but it was missing")
		}
		if foundBaseValue {
			t.Error("expected final exclusion list to NOT contain value from base config (should be overridden), but it was present")
		}
	})

	t.Run("Mod time window flag overrides base", func(t *testing.T) {
		base := NewDefault()
		base.Engine.ModTimeWindowSeconds = 1 // Default base value
		setFlags := map[string]interface{}{"mod-time-window": 0}

		merged := MergeConfigWithFlags(base, setFlags)
		expectedWindow := 0
		if merged.Engine.ModTimeWindowSeconds != expectedWindow {
			t.Errorf("expected flag 'mod-time-window=%d' to override base '%d', but got '%d'", expectedWindow, 1, merged.Engine.ModTimeWindowSeconds)
		}
	})
}

func TestGenerate(t *testing.T) {
	t.Run("Generates file in target dir", func(t *testing.T) {
		targetDir := t.TempDir()
		cfg := NewDefault()
		cfg.Paths.TargetBase = targetDir

		err := Generate(cfg)
		if err != nil {
			t.Fatalf("Generate() failed: %v", err)
		}

		expectedPath := filepath.Join(targetDir, ConfigFileName)
		if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
			t.Errorf("expected config file to be created at %s, but it was not", expectedPath)
		}
	})

	t.Run("Overwrites existing file", func(t *testing.T) {
		targetDir := t.TempDir()
		configPath := filepath.Join(targetDir, ConfigFileName)

		// Create a dummy file first.
		if err := os.WriteFile(configPath, []byte("old content"), 0644); err != nil {
			t.Fatalf("failed to create dummy config file: %v", err)
		}

		cfg := NewDefault()
		cfg.Paths.TargetBase = targetDir

		err := Generate(cfg)
		if err != nil {
			t.Fatalf("Generate() should not fail when overwriting, but got: %v", err)
		}

		// Check that the file was overwritten with default JSON content.
		content, _ := os.ReadFile(configPath)
		if string(content) == "old content" {
			t.Error("config file was not overwritten, but it should have been")
		}
	})

	t.Run("Generates file with custom values", func(t *testing.T) {
		targetDir := t.TempDir()
		customCfg := NewDefault()
		customCfg.Paths.Source = "/my/custom/source" // A non-default value
		customCfg.LogLevel = "debug"                 // A non-default value
		// Set the targetBase in the config to match the directory it's being generated in.
		// This is required for the Load function's validation to pass.
		customCfg.Paths.TargetBase = targetDir

		err := Generate(customCfg)
		if err != nil {
			t.Fatalf("Generate() with custom config failed: %v", err)
		}

		// Load the generated file and check its contents
		loadedCfg, err := Load(targetDir)
		if err != nil {
			t.Fatalf("Failed to load generated config for verification: %v", err)
		}

		if loadedCfg.Paths.Source != "/my/custom/source" || loadedCfg.LogLevel != "debug" {
			t.Errorf("Generated config did not contain the custom values. Got source=%s, logLevel=%s", loadedCfg.Paths.Source, loadedCfg.LogLevel)
		}
	})
}

func TestFormatTimestampWithOffset(t *testing.T) {
	// 1. Create a fixed UTC time for the test.
	testTime := time.Date(2023, 10, 27, 14, 30, 15, 123456789, time.UTC)

	// 2. Call the function under test.
	actualResult := FormatTimestampWithOffset(testTime)

	// 3. Construct the expected result.
	// The UTC part is based on the fixed time and the package constant.
	expectedUTCPart := "2023-10-27-14-30-15-123456789"
	// The offset part depends on the local timezone of the machine running the test.
	// We calculate it the same way the function does to get a reliable comparison.
	expectedOffsetPart := testTime.In(time.Local).Format("Z0700")
	expectedResult := expectedUTCPart + expectedOffsetPart

	// 4. Assert that the actual result matches the expected result.
	if actualResult != expectedResult {
		t.Errorf("FormatTimestampWithOffset() = %v, want %v", actualResult, expectedResult)
		t.Logf("This test is dependent on the system's local time zone.")
		t.Logf("Expected UTC part: %s, Expected Offset part: %s", expectedUTCPart, expectedOffsetPart)
	}
}

func TestLoad(t *testing.T) {
	t.Run("No Config File", func(t *testing.T) {
		tempDir := t.TempDir()

		cfg, err := Load(tempDir)
		if err != nil {
			t.Fatalf("Load() with no config file should not return an error, but got: %v", err)
		}

		// Check if it returned the default config
		if cfg.Naming.Prefix != "PGL_Backup_" {
			t.Errorf("expected default prefix 'PGL_Backup_', but got %s", cfg.Naming.Prefix)
		}
	})

	t.Run("Valid Config File", func(t *testing.T) {
		tempDir := t.TempDir()
		confPath := filepath.Join(tempDir, ConfigFileName)
		// Create a config file that is self-consistent: its targetBase must
		// point to the directory it resides in.
		content := fmt.Sprintf(`{"naming": {"prefix": "custom_prefix_"}, "paths": {"targetBase": "%s"}}`, tempDir)
		// Use double backslashes for JSON compatibility on Windows
		content = strings.ReplaceAll(content, `\`, `\\`)
		if err := os.WriteFile(confPath, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write test config file: %v", err)
		}

		cfg, err := Load(tempDir)
		if err != nil {
			t.Fatalf("expected no error when loading valid config, but got: %v", err)
		}

		// Check that the value from the file overrode the default
		if cfg.Naming.Prefix != "custom_prefix_" {
			t.Errorf("expected prefix to be 'custom_prefix_', but got %s", cfg.Naming.Prefix)
		}
		// Check that a default value not in the file is still present
		if cfg.Archive.Incremental.Mode != AutoInterval {
			t.Errorf("expected default archive mode to be auto, but got %v", cfg.Archive.Incremental.Mode)
		}
	})

	t.Run("Malformed Config File", func(t *testing.T) {
		tempDir := t.TempDir()
		confPath := filepath.Join(tempDir, ConfigFileName)
		// Create a malformed JSON file
		content := `{"naming": {"prefix": "custom_prefix_"},}` // Extra comma
		if err := os.WriteFile(confPath, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write test config file: %v", err)
		}

		_, err := Load(tempDir)
		if err == nil {
			t.Fatal("expected an error when loading malformed config, but got nil")
		}
	})

	t.Run("Mismatched TargetBase in Config", func(t *testing.T) {
		loadDir := t.TempDir()
		otherDir := t.TempDir()
		confPath := filepath.Join(loadDir, ConfigFileName)

		// Create a config file where the targetBase points to a different directory.
		content := fmt.Sprintf(`{"paths": {"targetBase": "%s"}}`, otherDir)
		// Use double backslashes for JSON compatibility on Windows
		content = strings.ReplaceAll(content, `\`, `\\`)

		if err := os.WriteFile(confPath, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write test config file: %v", err)
		}

		_, err := Load(loadDir)
		if err == nil {
			t.Fatal("expected an error for mismatched targetBase, but got nil")
		}
	})
}
