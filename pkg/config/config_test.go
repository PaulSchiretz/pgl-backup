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

	t.Run("Invalid RolloverInterval", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Mode = IncrementalMode
		cfg.RolloverInterval = 0
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for zero rollover interval in incremental mode, but got nil")
		}
	})

	t.Run("Invalid Glob Pattern", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Paths.ExcludeFiles = []string{"["} // Invalid glob pattern
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for invalid glob pattern, but got nil")
		}
	})
}

func TestMergeConfigWithFlags(t *testing.T) {
	t.Run("Flag overrides base config", func(t *testing.T) {
		base := NewDefault() // base.Quiet is false
		setFlags := map[string]interface{}{"quiet": true}

		merged := MergeConfigWithFlags(base, setFlags)
		if !merged.Quiet {
			t.Error("expected flag 'quiet=true' to override base 'false'")
		}
	})

	t.Run("Base config is used when flag is not set", func(t *testing.T) {
		base := NewDefault()
		base.Quiet = true // Set a non-default base value
		setFlags := map[string]interface{}{}

		merged := MergeConfigWithFlags(base, setFlags)
		if !merged.Quiet {
			t.Error("expected base 'quiet=true' to be used when flag is not set")
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
		base.Paths.ExcludeFiles = []string{"from_base.txt"}
		setFlags := map[string]interface{}{"exclude-files": []string{"from_flag.txt"}}

		merged := MergeConfigWithFlags(base, setFlags)
		if len(merged.Paths.ExcludeFiles) != 1 || merged.Paths.ExcludeFiles[0] != "from_flag.txt" {
			t.Errorf("expected exclude files from flag to override base, but got %v", merged.Paths.ExcludeFiles)
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
		customCfg.Paths.Source = "/my/custom/source"
		customCfg.Quiet = true
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

		if loadedCfg.Paths.Source != "/my/custom/source" || !loadedCfg.Quiet {
			t.Errorf("Generated config did not contain the custom values. Got source=%s, quiet=%v", loadedCfg.Paths.Source, loadedCfg.Quiet)
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
		if cfg.RolloverInterval != 24*time.Hour {
			t.Errorf("expected default rollover interval, but got %v", cfg.RolloverInterval)
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
