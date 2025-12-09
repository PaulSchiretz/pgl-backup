package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// configFileName is the name of the configuration file.
const ConfigFileName = "pgl-backup.conf"

// MetaFileName is the name of the backup metadata file.
const MetaFileName = ".pgl-backup.meta"

// LockFileName is the name of the lock file created in the target directory.
const LockFileName = ".pgl-backup.lock"

// backupTimeFormat defines the standard, non-configurable time format for backup directory names.
const backupTimeFormat = "2006-01-02-15-04-05"

// GetSystemExcludeFilePatterns returns a slice of file patterns that should
// always be excluded from synchronization for the system to function correctly.
func GetSystemExcludeFilePatterns() []string {
	return []string{MetaFileName, LockFileName, ConfigFileName}
}

type BackupNamingConfig struct {
	Prefix                string `json:"prefix"`
	IncrementalModeSuffix string `json:"incrementalModeSuffix"`
}

type BackupPathConfig struct {
	Source                      string   `json:"source"`
	TargetBase                  string   `json:"targetBase"`
	PreserveSourceDirectoryName bool     `json:"preserveSourceDirectoryName"`
	ExcludeFiles                []string `json:"excludeFiles,omitempty"`
	ExcludeDirs                 []string `json:"excludeDirs,omitempty"`
}

type BackupRetentionPolicyConfig struct {
	Hours  int `json:"hours"`
	Days   int `json:"days"`
	Weeks  int `json:"weeks"`
	Months int `json:"months"`
}

type BackupHooksConfig struct {
	// PreBackup is a list of shell commands to execute before the backup sync begins.
	// SECURITY: These commands are executed as provided. Ensure they are from a trusted source.
	PreBackup []string `json:"preBackup,omitempty"`
	// PostBackup is a list of shell commands to execute before the backup sync begins.
	// SECURITY: These commands are executed as provided. Ensure they are from a trusted source.
	PostBackup []string `json:"postBackup,omitempty"`
}

// BackupMode represents the operational mode of the backup (incremental or snapshot).
type BackupMode int

// Constants for BackupMode, acting as an enum.
const (
	IncrementalMode BackupMode = iota // 0
	SnapshotMode                      // 1
)

var backupModeToString = map[BackupMode]string{IncrementalMode: "incremental", SnapshotMode: "snapshot"}
var stringToBackupMode = invertMap(backupModeToString)

// String returns the string representation of a BackupMode.
func (bm BackupMode) String() string {
	if str, ok := backupModeToString[bm]; ok {
		return str
	}
	return fmt.Sprintf("unknown_backup_mode(%d)", bm)
}

// BackupModeFromString parses a string and returns the corresponding BackupMode.
func BackupModeFromString(s string) (BackupMode, error) {
	if mode, ok := stringToBackupMode[s]; ok {
		return mode, nil
	}
	return 0, fmt.Errorf("invalid BackupMode: %q. Must be 'incremental' or 'snapshot'", s)
}

// MarshalJSON implements the json.Marshaler interface for BackupMode.
func (bm BackupMode) MarshalJSON() ([]byte, error) {
	return json.Marshal(bm.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for BackupMode.
func (bm *BackupMode) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("BackupMode should be a string, got %s", data)
	}

	mode, err := BackupModeFromString(s) // Use the helper for parsing
	if err != nil {
		return err
	}
	*bm = mode
	return nil
}

// SyncEngine represents the file synchronization engine to use.
type SyncEngine int

const (
	// NativeEngine uses the cross-platform Go implementation.
	NativeEngine SyncEngine = iota
	// RobocopyEngine uses the Windows-specific robocopy utility.
	RobocopyEngine
)

var syncEngineToString = map[SyncEngine]string{NativeEngine: "native", RobocopyEngine: "robocopy"}
var stringToSyncEngine = invertMap(syncEngineToString)

// String returns the string representation of a SyncEngine.
func (se SyncEngine) String() string {
	if str, ok := syncEngineToString[se]; ok {
		return str
	}
	return fmt.Sprintf("unknown_sync_engine(%d)", se)
}

// SyncEngineFromString parses a string and returns the corresponding SyncEngine.
func SyncEngineFromString(s string) (SyncEngine, error) {
	if engine, ok := stringToSyncEngine[s]; ok {
		return engine, nil
	}
	return 0, fmt.Errorf("invalid SyncEngine: %q. Must be 'native' or 'robocopy'", s)
}

// MarshalJSON implements the json.Marshaler interface for SyncEngine.
func (se SyncEngine) MarshalJSON() ([]byte, error) {
	return json.Marshal(se.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for SyncEngine.
func (se *SyncEngine) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("SyncEngine should be a string, got %s", data)
	}

	engine, err := SyncEngineFromString(s) // Use the helper for parsing
	if err != nil {
		return err
	}
	*se = engine
	return nil
}

type BackupEngineConfig struct {
	Type                             SyncEngine                    `json:"type"`
	RetryCount                       int                           `json:"retryCount"`
	RetryWaitSeconds                 int                           `json:"retryWaitSeconds"`
	NativeEngineModTimeWindowSeconds int                           `json:"nativeEngineModTimeWindowSeconds" comment:"Time window in seconds to consider file modification times equal. Handles filesystem timestamp precision differences. Default is 1s. 0 means exact match."`
	Performance                      BackupEnginePerformanceConfig `json:"performance,omitempty"`
}

type BackupEnginePerformanceConfig struct {
	SyncWorkers      int `json:"syncWorkers"`
	MirrorWorkers    int `json:"mirrorWorkers" comment:"Number of concurrent workers for file deletions in mirror mode."`
	DeleteWorkers    int `json:"deleteWorkers"`
	CopyBufferSizeKB int `json:"copyBufferSizeKB" comment:"Size of the I/O buffer in kilobytes for file copies. Default is 4096 (4MB)."`
}

type Config struct {
	Mode             BackupMode                  `json:"mode"`
	RolloverInterval time.Duration               `json:"rolloverInterval"`
	Engine           BackupEngineConfig          `json:"engine"` // Keep this for engine-specific settings
	LogLevel         string                      `json:"logLevel"`
	DryRun           bool                        `json:"dryRun"`
	FailFast         bool                        `json:"failFast"`
	Metrics          bool                        `json:"metrics,omitempty"`
	Naming           BackupNamingConfig          `json:"naming"`
	Paths            BackupPathConfig            `json:"paths"`
	RetentionPolicy  BackupRetentionPolicyConfig `json:"retentionPolicy"`
	Hooks            BackupHooksConfig           `json:"hooks,omitempty"`
}

// NewDefault creates and returns a Config struct with sensible default
// values. It dynamically sets the sync engine based on the operating system.
func NewDefault() Config {
	// Default to the native engine on all platforms. It's highly concurrent and generally offers
	// the best performance and consistency with no external dependencies.
	// Power users on Windows can still opt-in to 'robocopy' as a battle-tested alternative.
	return Config{
		Mode:             IncrementalMode, // Default mode
		RolloverInterval: 24 * time.Hour,  // Default rollover interval is daily.
		LogLevel:         "info",          // Default log level.
		DryRun:           false,
		FailFast:         false,
		Metrics:          true, // Default to enabled for detailed performance and file-counting metrics.
		Engine: BackupEngineConfig{
			Type:                             NativeEngine,
			RetryCount:                       3, // Default retries on failure.
			RetryWaitSeconds:                 5, // Default wait time between retries.
			NativeEngineModTimeWindowSeconds: 1, // Set the default to 1 second
			Performance: BackupEnginePerformanceConfig{ // Initialize performance settings here
				SyncWorkers:      runtime.NumCPU(), // Default to the number of CPU cores for file copies.
				MirrorWorkers:    runtime.NumCPU(), // Default to the number of CPU cores for file deletions.
				DeleteWorkers:    4,                // A sensible default for deleting entire backup sets.
				CopyBufferSizeKB: 256,              // Default to 256KB buffer. Keep it between 64KB-4MB
			}},
		Naming: BackupNamingConfig{
			Prefix:                "PGL_Backup_",
			IncrementalModeSuffix: "Current",
		},
		Paths: BackupPathConfig{
			Source:                      "",         // Intentionally empty to force user configuration.
			TargetBase:                  "",         // Intentionally empty to force user configuration.
			PreserveSourceDirectoryName: true,       // Default to preserving the source folder name in the destination.
			ExcludeFiles:                []string{}, // User-defined list of files to exclude.
			ExcludeDirs:                 []string{}, // User-defined list of directories to exclude.
		},
		RetentionPolicy: BackupRetentionPolicyConfig{
			Hours:  24, // N > 0: keep the N most recent hourly backups.
			Days:   7,  // N > 0: keep one backup for each of the last N days.
			Weeks:  4,  // N > 0: keep one backup for each of the last N weeks.
			Months: 12, // N > 0: keep one backup for each of the last N months.
		},
		Hooks: BackupHooksConfig{
			PreBackup:  []string{},
			PostBackup: []string{},
		},
	}
}

// Load attempts to load a configuration from "pgl-backup.conf".
// If the file doesn't exist, it returns the provided default config without an error.
// If the file exists but fails to parse, it returns an error and a zero-value config.
func Load(targetBase string) (Config, error) {
	configPath := filepath.Join(targetBase, ConfigFileName)

	file, err := os.Open(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return NewDefault(), nil // Config file doesn't exist, which is a normal case.
		}
		return Config{}, fmt.Errorf("error opening config file %s: %w", configPath, err)
	}
	defer file.Close()

	plog.Info("Loading configuration from", "path", configPath)
	// Start with default values, then overwrite with the file's content.
	// This makes the config loading resilient to missing fields in the JSON file.
	config := NewDefault()
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("error parsing config file %s: %w", configPath, err)
	}

	// After loading, validate that the targetBase in the config file matches the
	// directory it was loaded from. This prevents using a config file in the wrong directory.
	absLoadDir, err := filepath.Abs(targetBase)
	if err != nil {
		return Config{}, fmt.Errorf("could not determine absolute path for load directory %s: %w", targetBase, err)
	}

	absTargetInConfig, err := filepath.Abs(config.Paths.TargetBase)
	if err != nil {
		return Config{}, fmt.Errorf("could not determine absolute path for targetBase in config %s: %w", config.Paths.TargetBase, err)
	}

	if absLoadDir != absTargetInConfig {
		return Config{}, fmt.Errorf("targetBase in config file (%s) does not match the directory it was loaded from (%s)", absTargetInConfig, absLoadDir)
	}
	return config, nil
}

// Generate creates or overwrites a default pgl-backup.conf file in the specified
// target directory.
func Generate(configToGenerate Config) error {
	configPath := filepath.Join(configToGenerate.Paths.TargetBase, ConfigFileName)
	// Marshal the default config into nicely formatted JSON.
	jsonData, err := json.MarshalIndent(configToGenerate, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal default config to JSON: %w", err)
	}

	// Write the JSON data to the file.
	if err := os.WriteFile(configPath, jsonData, 0664); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	plog.Info("Successfully created default config file", "path", configPath)
	return nil
}

// Validate checks the configuration for logical errors and inconsistencies.
// It performs strict checks, including ensuring the source path is non-empty
// and exists.
func (c *Config) Validate() error {
	// --- Strict Path Validation (Fail-Fast) ---
	if c.Paths.Source == "" {
		return fmt.Errorf("source path cannot be empty")
	}
	if c.Paths.TargetBase == "" {
		return fmt.Errorf("target path cannot be empty")
	}

	// Clean and expand paths for canonical representation before use.
	var err error

	// --- Validate Source Path ---
	if c.Paths.Source != "" {
		c.Paths.Source, err = expandPath(c.Paths.Source)
		if err != nil {
			return fmt.Errorf("could not expand source path: %w", err)
		}
		c.Paths.Source = filepath.Clean(c.Paths.Source)

		// After cleaning and expanding the path, check for existence.
		if _, err := os.Stat(c.Paths.Source); os.IsNotExist(err) {
			return fmt.Errorf("source path '%s' does not exist", c.Paths.Source)
		}
	}

	// --- Validate Target Path ---
	if c.Paths.TargetBase != "" {
		c.Paths.TargetBase, err = expandPath(c.Paths.TargetBase)
		if err != nil {
			return fmt.Errorf("could not expand target path: %w", err)
		}
		c.Paths.TargetBase = filepath.Clean(c.Paths.TargetBase)
	}

	// --- Validate Engine and Mode Settings ---
	if c.Engine.Performance.SyncWorkers < 1 {
		return fmt.Errorf("syncWorkers must be at least 1")
	}
	if c.Engine.Performance.MirrorWorkers < 1 {
		return fmt.Errorf("mirrorWorkers must be at least 1")
	}
	if c.Engine.Performance.DeleteWorkers < 1 {
		return fmt.Errorf("deleteWorkers must be at least 1")
	}
	if c.Engine.RetryCount < 0 {
		return fmt.Errorf("retryCount cannot be negative")
	}
	if c.Engine.RetryWaitSeconds < 0 {
		return fmt.Errorf("retryWaitSeconds cannot be negative")
	}
	if c.Engine.Performance.CopyBufferSizeKB <= 0 {
		return fmt.Errorf("copyBufferSizeKB must be greater than 0")
	}
	if c.Mode == IncrementalMode && c.RolloverInterval <= 0 {
		return fmt.Errorf("rolloverInterval must be a positive duration (e.g., '24h', '90m')")
	}

	if err := validateGlobPatterns("excludeFiles", c.Paths.ExcludeFiles); err != nil {
		return err
	}
	if err := validateGlobPatterns("excludeDirs", c.Paths.ExcludeDirs); err != nil {
		return err
	}
	return nil
}

// LogSummary prints a user-friendly summary of the configuration to the
// provided logger. It respects the 'Quiet' setting.
func (c *Config) LogSummary() {
	logArgs := []interface{}{
		"mode", c.Mode,
		"log_level", c.LogLevel,
		"source", c.Paths.Source,
		"target", c.Paths.TargetBase,
		"sync_engine", c.Engine.Type,
		"dry_run", c.DryRun,
		"sync_workers", c.Engine.Performance.SyncWorkers,
		"mirror_workers", c.Engine.Performance.MirrorWorkers,
		"metrics", c.Metrics,
		"delete_workers", c.Engine.Performance.DeleteWorkers,
		"copy_buffer_kb", c.Engine.Performance.CopyBufferSizeKB,
	}
	if c.Mode == IncrementalMode {
		logArgs = append(logArgs, "rollover_interval", c.RolloverInterval)
	}
	if len(c.Paths.ExcludeFiles) > 0 {
		logArgs = append(logArgs, "exclude_files", strings.Join(c.Paths.ExcludeFiles, ", "))
	}
	if len(c.Paths.ExcludeDirs) > 0 {
		logArgs = append(logArgs, "exclude_dirs", strings.Join(c.Paths.ExcludeDirs, ", "))
	}
	if len(c.Hooks.PreBackup) > 0 {
		logArgs = append(logArgs, "pre_backup_hooks", strings.Join(c.Hooks.PreBackup, "; "))
	}
	if len(c.Hooks.PostBackup) > 0 {
		logArgs = append(logArgs, "post_backup_hooks", strings.Join(c.Hooks.PostBackup, "; "))
	}

	plog.Info("Backup configuration loaded", logArgs...)
}

// expandPath expands the tilde (~) prefix in a path to the user's home directory.
func expandPath(path string) (string, error) {
	if !strings.HasPrefix(path, "~") {
		return path, nil // No tilde, return as-is.
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("could not get user home directory: %w", err)
	}

	// Replace the tilde with the home directory.
	return filepath.Join(home, path[1:]), nil
}

// invertMap takes a map[K]V and returns a map[V]K.
// It's a generic helper for creating reverse lookup maps for enums.
func invertMap[K comparable, V comparable](m map[K]V) map[V]K {
	inv := make(map[V]K, len(m))
	for k, v := range m {
		inv[v] = k
	}
	return inv
}

// validateGlobPatterns checks if a list of strings are valid glob patterns.
func validateGlobPatterns(fieldName string, patterns []string) error {
	for _, pattern := range patterns {
		if _, err := filepath.Match(pattern, ""); err != nil {
			return fmt.Errorf("invalid glob pattern for %s: %q - %w", fieldName, pattern, err)
		}
	}
	return nil
}

// FormatTimestampWithOffset formats a UTC timestamp into a string that includes
// the local timezone offset for user-friendliness, while keeping the base time in UTC.
// Example: 2023-10-27-14-00-00-123456789-0400
func FormatTimestampWithOffset(timestampUTC time.Time) string {
	// We format the UTC time for the timestamp, then format it again in the local
	// timezone just to get the offset string, and combine them.
	mainPartUTC := timestampUTC.Format(backupTimeFormat)
	nanoPartUTC := fmt.Sprintf("%09d", timestampUTC.Nanosecond())
	offsetPartLocal := timestampUTC.In(time.Local).Format("Z0700")

	return fmt.Sprintf("%s-%s%s", mainPartUTC, nanoPartUTC, offsetPartLocal)
}

// MergeConfigWithFlags overlays the configuration values from flags on top of a base
// configuration. It iterates over the setFlags map, which contains only the flags
// explicitly provided by the user on the command line.
func MergeConfigWithFlags(base Config, setFlags map[string]interface{}) Config {
	merged := base

	for name, value := range setFlags {
		switch name {
		case "source":
			merged.Paths.Source = value.(string)
		case "target":
			merged.Paths.TargetBase = value.(string)
		case "mode":
			merged.Mode = value.(BackupMode)
		case "log-level":
			merged.LogLevel = value.(string)
		case "fail-fast":
			merged.FailFast = value.(bool)
		case "metrics":
			merged.Metrics = value.(bool)
		case "dry-run":
			merged.DryRun = value.(bool)
		case "sync-engine":
			merged.Engine.Type = value.(SyncEngine)
		case "sync-workers":
			merged.Engine.Performance.SyncWorkers = value.(int)
		case "mirror-workers":
			merged.Engine.Performance.MirrorWorkers = value.(int)
		case "delete-workers":
			merged.Engine.Performance.DeleteWorkers = value.(int)
		case "retry-count":
			merged.Engine.RetryCount = value.(int)
		case "retry-wait":
			merged.Engine.RetryWaitSeconds = value.(int)
		case "copy-buffer-kb":
			merged.Engine.Performance.CopyBufferSizeKB = value.(int)
		case "exclude-files":
			merged.Paths.ExcludeFiles = value.([]string)
		case "exclude-dirs":
			merged.Paths.ExcludeDirs = value.([]string)
		case "preserve-source-name":
			merged.Paths.PreserveSourceDirectoryName = value.(bool)
		case "pre-backup-hooks":
			merged.Hooks.PreBackup = value.([]string)
		case "post-backup-hooks":
			merged.Hooks.PostBackup = value.([]string)
		}
	}
	return merged
}
