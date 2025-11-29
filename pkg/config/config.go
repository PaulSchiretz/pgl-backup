package config

import (
	"encoding/json"
	"flag"
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
	Type                         SyncEngine `json:"type"`
	NativeEngineWorkers          int        `json:"nativeEngineWorkers"`
	NativeEngineRetryCount       int        `json:"nativeEngineRetryCount"`
	NativeEngineRetryWaitSeconds int        `json:"nativeEngineRetryWaitSeconds"`
}

type Config struct {
	Mode             BackupMode                  `json:"mode"`
	RolloverInterval time.Duration               `json:"rolloverInterval"`
	Engine           BackupEngineConfig          `json:"engine"`
	Quiet            bool                        `json:"quiet"`
	DryRun           bool                        `json:"dryRun"`
	Naming           BackupNamingConfig          `json:"naming"`
	Paths            BackupPathConfig            `json:"paths"`
	RetentionPolicy  BackupRetentionPolicyConfig `json:"retentionPolicy"`
	Hooks            BackupHooksConfig           `json:"hooks,omitempty"`
}

// NewDefault creates and returns a Config struct with sensible default
// values. It dynamically sets the sync engine based on the operating system.
func NewDefault() Config {
	// Default to the native engine on all platforms for consistency and no external dependencies.
	// Power users on Windows can still opt-in to 'robocopy' for potential performance gains.
	return Config{
		Mode:             IncrementalMode, // Default mode
		RolloverInterval: 24 * time.Hour,  // Default rollover interval is daily.
		Quiet:            false,
		DryRun:           false,
		Engine: BackupEngineConfig{
			Type:                         NativeEngine,
			NativeEngineWorkers:          runtime.NumCPU(), // Default to the number of CPU cores.
			NativeEngineRetryCount:       3,                // Default retries on failure.
			NativeEngineRetryWaitSeconds: 5,                // Default wait time between retries.
		},
		Naming: BackupNamingConfig{
			Prefix:                "5ive_Backup_",
			IncrementalModeSuffix: "current",
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
	if c.Engine.NativeEngineWorkers < 1 {
		return fmt.Errorf("nativeEngineWorkers must be at least 1")
	}
	if c.Engine.NativeEngineRetryCount < 0 {
		return fmt.Errorf("nativeEngineRetryCount cannot be negative")
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
	if c.Quiet {
		return
	}
	logArgs := []interface{}{
		"mode", c.Mode,
		"source", c.Paths.Source,
		"target", c.Paths.TargetBase,
		"sync_engine", c.Engine.Type,
		"dry_run", c.DryRun,
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

	plog.Info("Backup run configuration loaded", logArgs...)
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
// configuration (loaded from a file or defaults).
func MergeConfigWithFlags(base, flags Config) Config {
	merged := base

	// A helper to check if a flag was actually set by the user on the command line.
	// This is crucial for merging, as it allows us to distinguish between a flag
	// that was not provided versus a flag that was explicitly set to its default value
	// (e.g., `-quiet=false`). We want to honor the user's explicit choice in all cases.
	isFlagSet := func(name string) bool {
		isSet := false
		flag.Visit(func(f *flag.Flag) { // flag.Visit iterates only over flags that were set.
			if f.Name == name {
				isSet = true
			}
		})
		return isSet
	}

	if isFlagSet("source") {
		merged.Paths.Source = flags.Paths.Source
	}
	if isFlagSet("target") {
		merged.Paths.TargetBase = flags.Paths.TargetBase
	}
	if isFlagSet("mode") {
		merged.Mode = flags.Mode
	}
	if isFlagSet("quiet") {
		merged.Quiet = flags.Quiet
	}
	if isFlagSet("dryrun") {
		merged.DryRun = flags.DryRun
	}
	if isFlagSet("sync-engine") {
		merged.Engine.Type = flags.Engine.Type
	}
	if isFlagSet("native-engine-workers") {
		merged.Engine.NativeEngineWorkers = flags.Engine.NativeEngineWorkers
	}
	if isFlagSet("native-retry-count") {
		merged.Engine.NativeEngineRetryCount = flags.Engine.NativeEngineRetryCount
	}
	if isFlagSet("native-retry-wait") {
		merged.Engine.NativeEngineRetryWaitSeconds = flags.Engine.NativeEngineRetryWaitSeconds
	}
	if isFlagSet("exclude-files") {
		merged.Paths.ExcludeFiles = flags.Paths.ExcludeFiles
	}
	if isFlagSet("exclude-dirs") {
		merged.Paths.ExcludeDirs = flags.Paths.ExcludeDirs
	}
	if isFlagSet("preserve-source-name") {
		merged.Paths.PreserveSourceDirectoryName = flags.Paths.PreserveSourceDirectoryName
	}

	if isFlagSet("pre-backup-hooks") {
		merged.Hooks.PreBackup = flags.Hooks.PreBackup
	}
	if isFlagSet("post-backup-hooks") {
		merged.Hooks.PostBackup = flags.Hooks.PostBackup
	}

	return merged
}
