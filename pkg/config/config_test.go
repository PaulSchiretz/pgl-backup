package config

import (
	"os"
	"path/filepath"
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

	t.Run("Invalid NativeEngineWorkers", func(t *testing.T) {
		cfg := newValidConfig(t)
		cfg.Engine.NativeEngineWorkers = 0
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for zero native engine workers, but got nil")
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

func TestLoad(t *testing.T) {
	// Override getConfigPath for testing to control where Load looks for the file.
	originalGetConfigPath := getConfigPath
	t.Cleanup(func() {
		getConfigPath = originalGetConfigPath
	})

	t.Run("No Config File", func(t *testing.T) {
		tempDir := t.TempDir()
		getConfigPath = func() (string, error) {
			return filepath.Join(tempDir, "nonexistent.conf"), nil
		}

		cfg, err := Load()
		if err != nil {
			t.Fatalf("expected no error when config file is missing, but got: %v", err)
		}

		// Check if it returned the default config
		if cfg.Naming.Prefix != "5ive_Backup_" {
			t.Errorf("expected default prefix, but got %s", cfg.Naming.Prefix)
		}
	})

	t.Run("Valid Config File", func(t *testing.T) {
		tempDir := t.TempDir()
		confPath := filepath.Join(tempDir, ConfigFileName)
		// Create a config file with a custom prefix
		content := `{"naming": {"prefix": "custom_prefix_"}}`
		if err := os.WriteFile(confPath, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write test config file: %v", err)
		}

		getConfigPath = func() (string, error) {
			return confPath, nil
		}

		cfg, err := Load()
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

		getConfigPath = func() (string, error) {
			return confPath, nil
		}

		_, err := Load()
		if err == nil {
			t.Fatal("expected an error when loading malformed config, but got nil")
		}
	})
}
