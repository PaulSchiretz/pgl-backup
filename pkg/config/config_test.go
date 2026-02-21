package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/paulschiretz/pgl-backup/pkg/flagparse"
)

func TestDefault(t *testing.T) {
	cfg := Default()
	if cfg.Version == "" {
		t.Error("Default() Version should not be empty")
	}
	if cfg.LogLevel != "info" {
		t.Errorf("Default() LogLevel = %v, want %v", cfg.LogLevel, "info")
	}
	if cfg.Engine.Performance.SyncWorkers != 4 {
		t.Errorf("Default() SyncWorkers = %v, want %v", cfg.Engine.Performance.SyncWorkers, 4)
	}
}

func TestConfig_Validate(t *testing.T) {
	// Helper to create a valid base configuration
	validConfig := func() Config {
		c := Default()
		return c
	}

	tests := []struct {
		name        string
		modify      func(*Config)
		wantErr     bool
		errContains string
	}{
		{
			name: "Valid Config (Incremental)",
			modify: func(c *Config) {
				c.Runtime.Mode = "incremental"
			},
			wantErr: false,
		},
		{
			name: "Valid Config (Snapshot)",
			modify: func(c *Config) {
				c.Runtime.Mode = "snapshot"
			},
			wantErr: false,
		},
		// Incremental Path Checks
		{
			name: "Incremental: Empty Archive Path",
			modify: func(c *Config) {
				c.Runtime.Mode = "incremental"
				c.Paths.Incremental.Archive = ""
			},
			wantErr:     true,
			errContains: "paths.incremental.archive cannot be empty",
		},
		{
			name: "Incremental: Current == Archive",
			modify: func(c *Config) {
				c.Runtime.Mode = "incremental"
				c.Paths.Incremental.Current = "same"
				c.Paths.Incremental.Archive = "same"
			},
			wantErr:     true,
			errContains: "cannot be the same",
		},
		{
			name: "Incremental: Path Separator in Archive",
			modify: func(c *Config) {
				c.Runtime.Mode = "incremental"
				c.Paths.Incremental.Archive = "sub/dir"
			},
			wantErr:     true,
			errContains: "cannot contain path separators",
		},
		// Snapshot Path Checks
		{
			name: "Snapshot: Empty Current Path",
			modify: func(c *Config) {
				c.Runtime.Mode = "snapshot"
				c.Paths.Snapshot.Current = ""
			},
			wantErr:     true,
			errContains: "paths.snapshot.current cannot be empty",
		},
		{
			name: "Snapshot: Archive == Content",
			modify: func(c *Config) {
				c.Runtime.Mode = "snapshot"
				c.Paths.Snapshot.Archive = "same"
				c.Paths.Snapshot.Content = "same"
			},
			wantErr:     true,
			errContains: "cannot be the same",
		},
		// Engine Settings
		{
			name: "Zero Sync Workers",
			modify: func(c *Config) {
				c.Engine.Performance.SyncWorkers = 0
			},
			wantErr:     true,
			errContains: "syncWorkers must be at least 1",
		},
		{
			name: "Negative Retry Count",
			modify: func(c *Config) {
				c.Runtime.Mode = "incremental"
				c.Sync.RetryCount = -1
			},
			wantErr:     true,
			errContains: "retryCount cannot be negative",
		},
		// Glob Patterns
		{
			name: "Invalid Glob Pattern",
			modify: func(c *Config) {
				c.Sync.UserExcludeFiles = []string{"["}
			},
			wantErr:     true,
			errContains: "invalid glob pattern",
		},
		// Retention Policy Checks
		{
			name: "Incremental Retention Enabled with Negative Values",
			modify: func(c *Config) {
				c.Retention.Incremental.Enabled = true
				c.Retention.Incremental.Days = -1
			},
			wantErr:     true,
			errContains: "retention.incremental is enabled but contains negative values",
		},
		{
			name: "Incremental Retention Enabled with Zero Values (Explicit Keep None)",
			modify: func(c *Config) {
				c.Retention.Incremental.Enabled = true
				c.Retention.Incremental.Hours = 0
				c.Retention.Incremental.Days = 0
				c.Retention.Incremental.Weeks = 0
				c.Retention.Incremental.Months = 0
				c.Retention.Incremental.Years = 0
			},
			wantErr: false,
		},
		{
			name: "Incremental Retention Enabled with Valid Positive Values",
			modify: func(c *Config) {
				c.Retention.Incremental.Enabled = true
				c.Retention.Incremental.Hours = 24
				c.Retention.Incremental.Days = 7
				c.Retention.Incremental.Weeks = 0
				c.Retention.Incremental.Months = 0
				c.Retention.Incremental.Years = 0
			},
			wantErr: false,
		},
		{
			name: "Snapshot Retention Enabled with Default Negative Values",
			modify: func(c *Config) {
				c.Retention.Snapshot.Enabled = true
				// Defaults are -1, so this should fail validation.
			},
			wantErr:     true,
			errContains: "retention.snapshot is enabled but contains negative values",
		},
		{
			name: "Snapshot Retention Disabled with Negative Values (Should Pass)",
			modify: func(c *Config) {
				c.Retention.Snapshot.Enabled = false
				c.Retention.Snapshot.Hours = -1
			},
			wantErr: false,
		},
		{
			name: "Snapshot Retention Enabled with Zero Values (Explicit Keep None)",
			modify: func(c *Config) {
				c.Retention.Snapshot.Enabled = true
				c.Retention.Snapshot.Hours = 0
				c.Retention.Snapshot.Days = 0
				c.Retention.Snapshot.Weeks = 0
				c.Retention.Snapshot.Months = 0
				c.Retention.Snapshot.Years = 0
			},
			wantErr: false,
		},
		{
			name: "Snapshot Retention Enabled with Valid Positive Values",
			modify: func(c *Config) {
				c.Retention.Snapshot.Enabled = true
				c.Retention.Snapshot.Hours = 24
				c.Retention.Snapshot.Days = 7
				c.Retention.Snapshot.Weeks = 0
				c.Retention.Snapshot.Months = 0
				c.Retention.Snapshot.Years = 0
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err != nil && tt.errContains != "" {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("Validate() error = %v, want error containing %q", err, tt.errContains)
				}
			}
		})
	}
}

func TestMergeConfigWithFlags(t *testing.T) {
	base := Default()
	base.LogLevel = "info"
	base.Engine.Performance.SyncWorkers = 4
	base.Sync.UserExcludeFiles = []string{"base.txt"}

	tests := []struct {
		name     string
		command  flagparse.Command
		flags    map[string]any
		validate func(*testing.T, Config)
	}{
		{
			name:    "Override LogLevel",
			command: flagparse.Backup,
			flags:   map[string]any{"log-level": "debug"},
			validate: func(t *testing.T, c Config) {
				if c.LogLevel != "debug" {
					t.Errorf("LogLevel = %v, want debug", c.LogLevel)
				}
			},
		},
		{
			name:    "Override SyncWorkers",
			command: flagparse.Backup,
			flags:   map[string]any{"sync-workers": 10},
			validate: func(t *testing.T, c Config) {
				if c.Engine.Performance.SyncWorkers != 10 {
					t.Errorf("SyncWorkers = %v, want 10", c.Engine.Performance.SyncWorkers)
				}
			},
		},
		{
			name:    "Override Mode (Backup Command)",
			command: flagparse.Backup,
			flags:   map[string]any{"mode": "snapshot"},
			validate: func(t *testing.T, c Config) {
				if c.Runtime.Mode != "snapshot" {
					t.Errorf("Mode = %v, want snapshot", c.Runtime.Mode)
				}
			},
		},
		{
			name:    "Override Mode (Prune Command)",
			command: flagparse.Prune,
			flags:   map[string]any{"mode": "snapshot"},
			validate: func(t *testing.T, c Config) {
				if c.Runtime.Mode != "snapshot" {
					t.Errorf("Mode = %v, want snapshot", c.Runtime.Mode)
				}
			},
		},
		{
			name:    "Ignore Mode (Init Command)",
			command: flagparse.Init,
			flags:   map[string]any{"mode": "snapshot"},
			validate: func(t *testing.T, c Config) {
				if c.Runtime.Mode != "any" { // Default is empty
					t.Errorf("Mode = %v, want any (default) because command is Init", c.Runtime.Mode)
				}
			},
		},
		{
			name:    "Override IgnoreCaseMismatch",
			command: flagparse.Backup,
			flags:   map[string]any{"ignore-case-mismatch": true},
			validate: func(t *testing.T, c Config) {
				if !c.Runtime.IgnoreCaseMismatch {
					t.Errorf("IgnoreCaseMismatch = %v, want true", c.Runtime.IgnoreCaseMismatch)
				}
			},
		},
		{
			name:    "Override Slice (UserExcludeFiles)",
			command: flagparse.Backup,
			flags:   map[string]any{"user-exclude-files": []string{"flag.txt"}},
			validate: func(t *testing.T, c Config) {
				if len(c.Sync.UserExcludeFiles) != 1 || c.Sync.UserExcludeFiles[0] != "flag.txt" {
					t.Errorf("UserExcludeFiles = %v, want [flag.txt]", c.Sync.UserExcludeFiles)
				}
			},
		},
		{
			name:    "Override Nested Config (Archive Enabled)",
			command: flagparse.Backup,
			flags:   map[string]any{"archive": false},
			validate: func(t *testing.T, c Config) {
				if c.Archive.Enabled != false {
					t.Errorf("Archive.Enabled = %v, want false", c.Archive.Enabled)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergeConfigWithFlags(tt.command, base, tt.flags)
			tt.validate(t, got)
		})
	}
}

func TestGenerateAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Default()
	cfg.LogLevel = "warn"

	// Test Generate
	if err := Generate(tmpDir, cfg); err != nil {
		t.Fatalf("Generate() error = %v", err)
	}

	configFile := filepath.Join(tmpDir, ConfigFileName)
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		t.Fatalf("Config file not created at %s", configFile)
	}

	// Test Load
	// We must change CWD to tmpDir because Load validates that TargetBase (which is empty in file)
	// matches the load directory.
	wd, _ := os.Getwd()
	defer os.Chdir(wd)
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("Failed to chdir: %v", err)
	}

	loadedCfg, err := Load(tmpDir)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if loadedCfg.LogLevel != "warn" {
		t.Errorf("Loaded LogLevel = %v, want warn", loadedCfg.LogLevel)
	}
}

func TestExcludeHelpers(t *testing.T) {
	cfg := Default()
	cfg.Sync.DefaultExcludeFiles = []string{"default.txt"}
	cfg.Sync.UserExcludeFiles = []string{"user.txt"}
	cfg.Sync.DefaultExcludeDirs = []string{"default_dir"}
	cfg.Sync.UserExcludeDirs = []string{"user_dir"}

	files := cfg.Sync.ExcludeFiles()
	dirs := cfg.Sync.ExcludeDirs()

	hasFile := func(list []string, s string) bool {
		for _, v := range list {
			if v == s {
				return true
			}
		}
		return false
	}

	if !hasFile(files, "pgl-backup.config.json") {
		t.Error("ExcludeFiles() missing system exclude 'pgl-backup.config.json'")
	}
	if !hasFile(files, "default.txt") {
		t.Error("ExcludeFiles() missing default exclude 'default.txt'")
	}
	if !hasFile(files, "user.txt") {
		t.Error("ExcludeFiles() missing user exclude 'user.txt'")
	}

	if !hasFile(dirs, "default_dir") {
		t.Error("ExcludeDirs() missing default exclude 'default_dir'")
	}
	if !hasFile(dirs, "user_dir") {
		t.Error("ExcludeDirs() missing user exclude 'user_dir'")
	}
}

func TestConfig_Validate_Normalization(t *testing.T) {
	c := Default()

	// Set mixed-case values to verify normalization
	c.Runtime.Mode = "Incremental"
	c.Archive.IntervalMode = "Manual"
	c.Compression.Format = "TAR.GZ"
	c.Compression.Level = "Best"
	c.Sync.Engine = "Native"
	c.Runtime.BackupOverwriteBehavior = "Always"
	c.Runtime.RestoreOverwriteBehavior = "If-Newer"

	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	if c.Runtime.Mode != "incremental" {
		t.Errorf("Runtime.Mode not normalized: got %q, want %q", c.Runtime.Mode, "incremental")
	}
	if c.Archive.IntervalMode != "manual" {
		t.Errorf("Archive.IntervalMode not normalized: got %q, want %q", c.Archive.IntervalMode, "manual")
	}
	if c.Compression.Format != "tar.gz" {
		t.Errorf("Compression.Format not normalized: got %q, want %q", c.Compression.Format, "tar.gz")
	}
	if c.Compression.Level != "best" {
		t.Errorf("Compression.Level not normalized: got %q, want %q", c.Compression.Level, "best")
	}
	if c.Sync.Engine != "native" {
		t.Errorf("Sync.Engine not normalized: got %q, want %q", c.Sync.Engine, "native")
	}
	if c.Runtime.BackupOverwriteBehavior != "always" {
		t.Errorf("Runtime.BackupOverwriteBehavior not normalized: got %q, want %q", c.Runtime.BackupOverwriteBehavior, "always")
	}
	if c.Runtime.RestoreOverwriteBehavior != "if-newer" {
		t.Errorf("Runtime.RestoreOverwriteBehavior not normalized: got %q, want %q", c.Runtime.RestoreOverwriteBehavior, "if-newer")
	}
}
