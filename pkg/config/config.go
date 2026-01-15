package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/lockfile"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// ConfigFileName is the name of the configuration file.
const ConfigFileName = "pgl-backup.config.json"

// backupTimeFormat defines the standard, non-configurable time format for backup directory names.
const backupTimeFormat = "2006-01-02-15-04-05"

// systemExcludeFilePatterns is a slice of file patterns that should
// always be excluded from synchronization for the system to function correctly.
var systemExcludeFilePatterns = []string{metafile.MetaFileName, lockfile.LockFileName, ConfigFileName}

// systemExcludeDirPatterns is a slice of directory patterns that should
// always be excluded from synchronization for the system to function correctly.
var systemExcludeDirPatterns = []string{}

type BackupNamingConfig struct {
	Prefix string `json:"prefix"`
}

type BackupPathSubDirConfig struct {
	Current string `json:"current"`
	Archive string `json:"archive"`
}

type BackupPathConfig struct {
	Source                      string                 `json:"source"`
	TargetBase                  string                 `json:"targetBase"`
	IncrementalSubDirs          BackupPathSubDirConfig `json:"incrementalSubDirs"`
	SnapshotSubDirs             BackupPathSubDirConfig `json:"snapshotSubDirs"`
	ContentSubDir               string                 `json:"contentSubDir"`
	PreserveSourceDirectoryName bool                   `json:"preserveSourceDirectoryName"`
	DefaultExcludeFiles         []string               `json:"defaultExcludeFiles,omitempty"`
	DefaultExcludeDirs          []string               `json:"defaultExcludeDirs,omitempty"`
	// Note: omitempty is intentionally not used for user-configurable slices
	// so that they appear in the generated config file for better discoverability.
	UserExcludeFiles []string `json:"userExcludeFiles"`
	UserExcludeDirs  []string `json:"userExcludeDirs"`
}

type BackupHooksConfig struct {
	// Note: omitempty is intentionally not used so that the hook fields
	// appear in the generated config file for better discoverability.
	// PreBackup is a list of shell commands to execute before the backup sync begins.
	// SECURITY: These commands are executed as provided. Ensure they are from a trusted source.
	PreBackup []string `json:"preBackup"`
	// PostBackup is a list of shell commands to execute before the backup sync begins.
	// SECURITY: These commands are executed as provided. Ensure they are from a trusted source.
	PostBackup []string `json:"postBackup"`
}

// BackupMode represents the operational mode of the backup (incremental or snapshot).
type BackupMode int

// Constants for BackupMode, acting as an enum.
const (
	IncrementalMode BackupMode = iota // 0
	SnapshotMode                      // 1
)

var backupModeToString = map[BackupMode]string{IncrementalMode: "incremental", SnapshotMode: "snapshot"}
var stringToBackupMode = util.InvertMap(backupModeToString)

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
var stringToSyncEngine = util.InvertMap(syncEngineToString)

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
	Type                 SyncEngine                    `json:"type"`
	RetryCount           int                           `json:"retryCount"`
	RetryWaitSeconds     int                           `json:"retryWaitSeconds"`
	ModTimeWindowSeconds int                           `json:"modTimeWindowSeconds" comment:"Time window in seconds to consider file modification times equal. Handles filesystem timestamp precision differences. Default is 1s. 0 means exact match."`
	Performance          BackupEnginePerformanceConfig `json:"performance,omitempty"`
}

type BackupEnginePerformanceConfig struct {
	SyncWorkers     int `json:"syncWorkers"`
	MirrorWorkers   int `json:"mirrorWorkers"`
	DeleteWorkers   int `json:"deleteWorkers"`
	CompressWorkers int `json:"compressWorkers"`
	BufferSizeKB    int `json:"bufferSizeKB" comment:"Size of the I/O buffer in kilobytes for file copies and compression. Default is 256 (256KB)."`
}

// ArchiveIntervalMode represents how the archive interval is determined.
type ArchiveIntervalMode int

const (
	// ManualInterval uses the user-specified interval value directly.
	ManualInterval ArchiveIntervalMode = iota // 0
	// AutoInterval calculates the interval based on the finest-grained retention policy.
	AutoInterval // 1
)

var archiveIntervalModeToString = map[ArchiveIntervalMode]string{ManualInterval: "manual", AutoInterval: "auto"}
var stringToArchiveIntervalMode = util.InvertMap(archiveIntervalModeToString)

// String returns the string representation of a ArchiveIntervalMode.
func (rim ArchiveIntervalMode) String() string {
	if str, ok := archiveIntervalModeToString[rim]; ok {
		return str
	}
	return fmt.Sprintf("unknown_interval_mode(%d)", rim)
}

// ArchiveIntervalModeFromString parses a string and returns the corresponding ArchiveIntervalMode.
func ArchiveIntervalModeFromString(s string) (ArchiveIntervalMode, error) {
	if mode, ok := stringToArchiveIntervalMode[s]; ok {
		return mode, nil
	}
	return 0, fmt.Errorf("invalid ArchiveIntervalMode: %q. Must be 'manual' or 'auto'", s)
}

// MarshalJSON implements the json.Marshaler interface for ArchiveIntervalMode.
func (rim ArchiveIntervalMode) MarshalJSON() ([]byte, error) {
	return json.Marshal(rim.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for ArchiveIntervalMode.
func (rim *ArchiveIntervalMode) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("ArchiveIntervalMode should be a string, got %s", data)
	}
	mode, err := ArchiveIntervalModeFromString(s)
	if err != nil {
		return err
	}
	*rim = mode
	return nil
}

type ArchivePolicyConfig struct {
	Enabled bool `json:"enabled"`
	// IntervalMode determines if the interval is set manually or derived automatically from the retention policy.
	IntervalMode ArchiveIntervalMode `json:"intervalMode"`
	// IntervalSeconds is the duration in seconds after which a new backup archive is created in incremental mode.
	// This is only used when Mode is 'manual'.
	IntervalSeconds int `json:"intervalSeconds,omitempty"`
}

type BackupArchiveConfig struct {
	// Incremental holds the archive policy specific to the incremental backup mode.
	Incremental ArchivePolicyConfig `json:"incremental,omitempty"`
}

type CompressionPolicyConfig struct {
	Enabled bool                   `json:"enabled"`
	Format  pathcompression.Format `json:"format"`
}

type BackupCompressionConfig struct {
	Incremental CompressionPolicyConfig `json:"incremental,omitempty"`
	Snapshot    CompressionPolicyConfig `json:"snapshot,omitempty"`
}

type RetentionPolicyConfig struct {
	Enabled bool `json:"enabled"`
	Hours   int  `json:"hours"`
	Days    int  `json:"days"`
	Weeks   int  `json:"weeks"`
	Months  int  `json:"months"`
	Years   int  `json:"years"`
}

type BackupRetentionConfig struct {
	Incremental RetentionPolicyConfig `json:"incremental,omitempty"`
	Snapshot    RetentionPolicyConfig `json:"snapshot,omitempty"`
}

type Config struct {
	Version     string                  `json:"version"`
	Mode        BackupMode              `json:"mode"`
	Engine      BackupEngineConfig      `json:"engine"` // Keep this for engine-specific settings
	LogLevel    string                  `json:"logLevel"`
	DryRun      bool                    `json:"dryRun"`
	FailFast    bool                    `json:"failFast"`
	Metrics     bool                    `json:"metrics,omitempty"`
	Compression BackupCompressionConfig `json:"compression,omitempty"`
	Naming      BackupNamingConfig      `json:"naming"`
	Paths       BackupPathConfig        `json:"paths"`
	Retention   BackupRetentionConfig   `json:"retention,omitempty"`
	Archive     BackupArchiveConfig     `json:"archive,omitempty"`
	Hooks       BackupHooksConfig       `json:"hooks,omitempty"`
}

// NewDefault creates and returns a Config struct with sensible default
// values. It dynamically sets the sync engine based on the operating system.
func NewDefault(appVersion string) Config {
	// Default to the native engine on all platforms. It's highly concurrent and generally offers
	// the best performance and consistency with no external dependencies.
	// Power users on Windows can still opt-in to 'robocopy' as a battle-tested alternative.
	return Config{
		Version:  appVersion,
		Mode:     IncrementalMode, // Default mode
		LogLevel: "info",          // Default log level.
		DryRun:   false,
		FailFast: false,
		Metrics:  true, // Default to enabled for detailed performance and file-counting metrics.
		Naming: BackupNamingConfig{
			Prefix: "PGL_Backup_",
		},
		Paths: BackupPathConfig{
			Source:     "", // Intentionally empty to force user configuration.
			TargetBase: "", // Intentionally empty to force user configuration.
			IncrementalSubDirs: BackupPathSubDirConfig{
				Current: "PGL_Backup_Incremental_Current", // Default name for the incremental current sub-directory.
				Archive: "PGL_Backup_Incremental_Archive", // Default name for the incremental archive sub-directory.
			},
			SnapshotSubDirs: BackupPathSubDirConfig{
				Current: "PGL_Backup_Snapshot_Current", // Default name for the snapshot current sub-directory.
				Archive: "PGL_Backup_Snapshot_Archive", // Default name for the snapshot archive sub-directory.
			},
			ContentSubDir:               "PGL_Backup_Content", // Default name for the content sub-directory.
			PreserveSourceDirectoryName: true,                 // Default to preserving the source folder name in the destination.
			UserExcludeFiles:            []string{},           // User-defined list of files to exclude.
			UserExcludeDirs:             []string{},           // User-defined list of directories to exclude.
			DefaultExcludeFiles: []string{
				// Common temporary and system files across platforms.
				"*.tmp",       // Temporary files
				"*.temp",      // Temporary files
				"*.swp",       // Vim swap files
				"*.lnk",       // Windows shortcuts
				"~*",          // Files starting with a tilde (often temporary)
				"desktop.ini", // Windows folder customization file
				".DS_Store",   // macOS folder customization file
				"Thumbs.db",   // Windows image thumbnail cache
				"Icon\r",      // macOS custom folder icons
			},
			DefaultExcludeDirs: []string{
				// Common temporary, system, and trash directories.
				"@tmp",                      // Synology temporary folder
				"@eadir",                    // Synology index folder
				".SynologyWorkingDirectory", // Synology Drive temporary folder
				"#recycle",                  // Synology recycle bin
				"$Recycle.Bin",              // Windows recycle bin
			},
		},
		Engine: BackupEngineConfig{
			Type:                 NativeEngine,
			RetryCount:           3, // Default retries on failure.
			RetryWaitSeconds:     5, // Default wait time between retries.
			ModTimeWindowSeconds: 1, // Set the default to 1 second
			Performance: BackupEnginePerformanceConfig{ // Initialize performance settings here
				SyncWorkers:     4,   // Default to 4. Safe for HDDs (prevents thrashing), decent for SSDs.
				MirrorWorkers:   4,   // Default to 4.
				DeleteWorkers:   4,   // A sensible default for deleting entire backup sets.
				CompressWorkers: 1,   // Default to 1. Our compression libraries (pgzip, zstd) use internal parallelism to utilize all cores for a single file. Increasing this might cause oversubscription.
				BufferSizeKB:    256, // Default to 256KB buffer. Keep it between 64KB-4MB
			}},
		Archive: BackupArchiveConfig{
			Incremental: ArchivePolicyConfig{
				Enabled:         true,         // Enabled by default for incremental mode.
				IntervalMode:    AutoInterval, // Default to auto-adjusting the interval based on the retention policy.
				IntervalSeconds: 86400,        // Interval will be calculated by the engine in 'auto' mode. Default 24h.
				// If a user switches to 'manual' mode, they must specify an interval.
			},
		},
		Retention: BackupRetentionConfig{
			Incremental: RetentionPolicyConfig{
				Enabled: true, // Enabled by default for incremental mode.
				Hours:   0,    // Default: No hourly backups.
				Days:    0,    // Default: No daily backups.
				Weeks:   4,    // Default: Keep one backup for each of the last 4 weeks.
				Months:  0,    // Default: No monthly backups.
				Years:   0,    // Default: No yearly backups.
			},
			Snapshot: RetentionPolicyConfig{
				Enabled: false, // Disabled by default to protect snapshots.
				Hours:   0,
				Days:    0,
				Weeks:   0,
				Months:  0,
				Years:   0,
			},
		},
		Compression: BackupCompressionConfig{
			Incremental: CompressionPolicyConfig{
				Enabled: true,
				Format:  pathcompression.TarZst,
			},
			Snapshot: CompressionPolicyConfig{
				Enabled: true,
				Format:  pathcompression.TarZst,
			},
		},
		Hooks: BackupHooksConfig{
			PreBackup:  []string{},
			PostBackup: []string{},
		},
	}
}

// Load attempts to load a configuration from "pgl-backup.config.json".
// If the file doesn't exist, it returns the provided default config without an error.
// If the file exists but fails to parse, it returns an error and a zero-value config.
func Load(appVersion string, targetBase string) (Config, error) {
	configPath := filepath.Join(targetBase, ConfigFileName)

	file, err := os.Open(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return NewDefault(appVersion), nil // Config file doesn't exist, which is a normal case.
		}
		return Config{}, fmt.Errorf("error opening config file %s: %w", configPath, err)
	}
	defer file.Close()

	plog.Info("Loading configuration", "path", configPath)
	// Start with default values, then overwrite with the file's content.
	// This makes the config loading resilient to missing fields in the JSON file.
	// NOTE: if config.Version differes from appVersion we can add a migration step here.
	config := NewDefault(appVersion)
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

	// At this point our config has been migrated if needed so override the version in the struct
	if config.Version != appVersion {
		config.Version = appVersion
	}
	return config, nil
}

// Generate creates or overwrites a default pgl-backup.config.json file in the specified
// target directory.
func Generate(configToGenerate Config) error {
	configPath := filepath.Join(configToGenerate.Paths.TargetBase, ConfigFileName)
	// Marshal the default config into nicely formatted JSON.
	jsonData, err := json.MarshalIndent(configToGenerate, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal default config to JSON: %w", err)
	}

	// Write the JSON data to the file.
	if err := os.WriteFile(configPath, jsonData, util.UserWritableFilePerms); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	plog.Info("Successfully saved config file", "path", configPath)
	return nil
}

// Validate checks the configuration for logical errors and inconsistencies.
// It performs strict checks, including ensuring the source path is non-empty
// and exists.
func (c *Config) Validate(checkSource bool) error {
	// --- Strict Path Validation (Fail-Fast) ---
	if checkSource && c.Paths.Source == "" {
		return fmt.Errorf("source path cannot be empty")
	}
	if c.Paths.TargetBase == "" {
		return fmt.Errorf("target path cannot be empty")
	}

	// Clean and expand paths for canonical representation before use.
	var err error

	// --- Validate Source Path ---
	if c.Paths.Source != "" {
		c.Paths.Source, err = util.ExpandPath(c.Paths.Source)
		if err != nil {
			return fmt.Errorf("could not expand source path: %w", err)
		}
		c.Paths.Source = filepath.Clean(c.Paths.Source)

		// After cleaning and expanding the path, check for existence.
		if checkSource {
			if _, err := os.Stat(c.Paths.Source); os.IsNotExist(err) {
				return fmt.Errorf("source path '%s' does not exist", c.Paths.Source)
			}
		}
	}

	// --- Validate Target Path ---
	if c.Paths.TargetBase != "" {
		c.Paths.TargetBase, err = util.ExpandPath(c.Paths.TargetBase)
		if err != nil {
			return fmt.Errorf("could not expand target path: %w", err)
		}
		c.Paths.TargetBase = filepath.Clean(c.Paths.TargetBase)
	}

	// --- Validate SubDirs ---
	switch c.Mode {
	case IncrementalMode:
		if c.Paths.IncrementalSubDirs.Archive == "" {
			return fmt.Errorf("incrementalSubDirs.archive cannot be empty in incremental mode")
		}
		if c.Paths.IncrementalSubDirs.Current == "" {
			return fmt.Errorf("incrementalSubDirs.current cannot be empty in incremental mode")
		}
		// Disallow path separators to ensure the archives directory is a direct child of the target.
		// This is critical for guaranteeing that the atomic `os.Rename` operation during archive
		// works correctly, as it requires the source and destination to be on the same filesystem.
		if strings.ContainsAny(c.Paths.IncrementalSubDirs.Archive, `\/`) {
			return fmt.Errorf("incrementalSubDirs.archive cannot contain path separators ('/' or '\\')")
		}
		if strings.ContainsAny(c.Paths.IncrementalSubDirs.Current, `\/`) {
			return fmt.Errorf("incrementalSubDirs.current cannot contain path separators ('/' or '\\')")
		}
	case SnapshotMode:
		// Disallow path separators to ensure the snapshots directory is a direct child of the target.
		// This is critical for guaranteeing that the atomic `os.Rename` operation during
		// works correctly, as it requires the source and destination to be on the same filesystem.
		if c.Paths.SnapshotSubDirs.Archive == "" {
			return fmt.Errorf("snapshotSubDirs.archive cannot be empty in snapshot mode")
		}
		if c.Paths.SnapshotSubDirs.Current == "" {
			return fmt.Errorf("snapshotSubDirs.current cannot be empty in snapshot mode")
		}
		// Disallow path separators to ensure the archives directory is a direct child of the target.
		// This is critical for guaranteeing that the atomic `os.Rename` operation during archive
		// works correctly, as it requires the source and destination to be on the same filesystem.
		if strings.ContainsAny(c.Paths.SnapshotSubDirs.Archive, `\/`) {
			return fmt.Errorf("snapshotSubDirs.archive cannot contain path separators ('/' or '\\')")
		}
		if strings.ContainsAny(c.Paths.SnapshotSubDirs.Current, `\/`) {
			return fmt.Errorf("snapshotSubDirs.current cannot contain path separators ('/' or '\\')")
		}
	}

	// Validate ContentSubDir for all modes
	if c.Paths.ContentSubDir == "" {
		return fmt.Errorf("contentSubDir cannot be empty")
	}
	if strings.ContainsAny(c.Paths.ContentSubDir, `\/`) {
		return fmt.Errorf("contentSubDir cannot contain path separators ('/' or '\\')")
	}
	// --- Validate Engine and Mode Settings ---
	if c.Engine.Performance.SyncWorkers < 1 {
		return fmt.Errorf("syncWorkers must be at least 1")
	}
	if c.Engine.Performance.MirrorWorkers < 1 {
		return fmt.Errorf("mirrorWorkers must be at least 1")
	}
	if c.Engine.Performance.CompressWorkers < 1 {
		return fmt.Errorf("compressWorkers must be at least 1")
	}
	if c.Engine.Performance.DeleteWorkers < 1 {
		return fmt.Errorf("deleteWorkers must be at least 1")
	}
	if c.Engine.Performance.BufferSizeKB <= 0 {
		return fmt.Errorf("bufferSizeKB must be greater than 0")
	}

	if c.Engine.RetryCount < 0 {
		return fmt.Errorf("retryCount cannot be negative")
	}
	if c.Engine.RetryWaitSeconds < 0 {
		return fmt.Errorf("retryWaitSeconds cannot be negative")
	}
	if c.Engine.ModTimeWindowSeconds < 0 {
		return fmt.Errorf("modTimeWindowSeconds cannot be negative")
	}

	switch c.Mode {
	case IncrementalMode:
		if c.Archive.Incremental.IntervalMode == ManualInterval && c.Archive.Incremental.IntervalSeconds < 0 {
			return fmt.Errorf("archive.incremental.intervalSeconds cannot be negative when mode is 'manual'. Use '0' to disable archive")
		}
	case SnapshotMode:
	}

	if err := validateGlobPatterns("defaultExcludeFiles", c.Paths.DefaultExcludeFiles); err != nil {
		return err
	}

	if err := validateGlobPatterns("userExcludeFiles", c.Paths.UserExcludeFiles); err != nil {
		return err
	}

	if err := validateGlobPatterns("defaultExcludeDirs", c.Paths.DefaultExcludeDirs); err != nil {
		return err
	}

	if err := validateGlobPatterns("userExcludeDirs", c.Paths.UserExcludeDirs); err != nil {
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
		"compress_workers", c.Engine.Performance.CompressWorkers,
		"delete_workers", c.Engine.Performance.DeleteWorkers,
		"buffer_size_kb", c.Engine.Performance.BufferSizeKB,
		"content_subdir", c.Paths.ContentSubDir,
	}
	switch c.Mode {
	case IncrementalMode:
		logArgs = append(logArgs, "current_subdir", c.Paths.IncrementalSubDirs.Current)
		logArgs = append(logArgs, "archive_subdir", c.Paths.IncrementalSubDirs.Archive)

		if c.Archive.Incremental.Enabled {
			switch c.Archive.Incremental.IntervalMode {
			case ManualInterval:
				archiveSummary := fmt.Sprintf("enabled (m:%s i:%ds)",
					c.Archive.Incremental.IntervalMode,
					c.Archive.Incremental.IntervalSeconds)
				logArgs = append(logArgs, "archive", archiveSummary)
			default:
				archiveSummary := fmt.Sprintf("enabled (m:%s)",
					c.Archive.Incremental.IntervalMode)
				logArgs = append(logArgs, "archive", archiveSummary)
			}
		}

		if c.Compression.Incremental.Enabled {
			compressionSummary := fmt.Sprintf("enabled (f:%s)", c.Compression.Incremental.Format)
			logArgs = append(logArgs, "compression", compressionSummary)
		}

		if c.Retention.Incremental.Enabled {
			retentionSummary := fmt.Sprintf("enabled (h:%d d:%d w:%d m:%d y:%d)",
				c.Retention.Incremental.Hours, c.Retention.Incremental.Days, c.Retention.Incremental.Weeks,
				c.Retention.Incremental.Months, c.Retention.Incremental.Years)
			logArgs = append(logArgs, "retention", retentionSummary)
		}

	case SnapshotMode:
		logArgs = append(logArgs, "current_subdir", c.Paths.SnapshotSubDirs.Current)
		logArgs = append(logArgs, "archive_subdir", c.Paths.SnapshotSubDirs.Archive)

		if c.Compression.Snapshot.Enabled {
			compressionSummary := fmt.Sprintf("enabled (f:%s)", c.Compression.Snapshot.Format)
			logArgs = append(logArgs, "compression", compressionSummary)
		}

		if c.Retention.Snapshot.Enabled {
			snapshotRetentionSummary := fmt.Sprintf("enabled (h:%d d:%d w:%d m:%d y:%d)",
				c.Retention.Snapshot.Hours, c.Retention.Snapshot.Days, c.Retention.Snapshot.Weeks,
				c.Retention.Snapshot.Months, c.Retention.Snapshot.Years)
			logArgs = append(logArgs, "retention", snapshotRetentionSummary)
		}

	}
	if finalExcludeFiles := c.Paths.ExcludeFiles(); len(finalExcludeFiles) > 0 {
		logArgs = append(logArgs, "exclude_files", strings.Join(finalExcludeFiles, ", "))
	}
	if finalExcludeDirs := c.Paths.ExcludeDirs(); len(finalExcludeDirs) > 0 {
		logArgs = append(logArgs, "exclude_dirs", strings.Join(finalExcludeDirs, ", "))
	}
	if len(c.Paths.DefaultExcludeFiles) > 0 {
		logArgs = append(logArgs, "default_exclude_files", strings.Join(c.Paths.DefaultExcludeFiles, ", "))
	}
	if len(c.Paths.DefaultExcludeDirs) > 0 {
		logArgs = append(logArgs, "default_exclude_dirs", strings.Join(c.Paths.DefaultExcludeDirs, ", "))
	}
	if len(c.Hooks.PreBackup) > 0 {
		logArgs = append(logArgs, "pre_backup_hooks", strings.Join(c.Hooks.PreBackup, "; "))
	}
	if len(c.Hooks.PostBackup) > 0 {
		logArgs = append(logArgs, "post_backup_hooks", strings.Join(c.Hooks.PostBackup, "; "))
	}
	plog.Info("Configuration loaded", logArgs...)
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

// ExcludeFiles returns the final, combined slice of file exclusion patterns, including
// non-overridable system patterns, default patterns, and user-configured patterns.
// It automatically handles deduplication.
func (p *BackupPathConfig) ExcludeFiles() []string {
	return util.MergeAndDeduplicate(systemExcludeFilePatterns, p.DefaultExcludeFiles, p.UserExcludeFiles)
}

// ExcludeDirs returns the final, combined slice of directory exclusion patterns, including
// non-overridable system patterns, default patterns, and user-configured patterns.
// It automatically handles deduplication.
func (p *BackupPathConfig) ExcludeDirs() []string {
	return util.MergeAndDeduplicate(systemExcludeDirPatterns, p.DefaultExcludeDirs, p.UserExcludeDirs)
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
		case "compress-workers":
			merged.Engine.Performance.CompressWorkers = value.(int)
		case "retry-count":
			merged.Engine.RetryCount = value.(int)
		case "retry-wait":
			merged.Engine.RetryWaitSeconds = value.(int)
		case "buffer-size-kb":
			merged.Engine.Performance.BufferSizeKB = value.(int)
		case "mod-time-window":
			merged.Engine.ModTimeWindowSeconds = value.(int)
		case "user-exclude-files":
			merged.Paths.UserExcludeFiles = value.([]string)
		case "user-exclude-dirs":
			merged.Paths.UserExcludeDirs = value.([]string)
		case "preserve-source-name":
			merged.Paths.PreserveSourceDirectoryName = value.(bool)
		case "pre-backup-hooks":
			merged.Hooks.PreBackup = value.([]string)
		case "post-backup-hooks":
			merged.Hooks.PostBackup = value.([]string)
		case "archive-incremental":
			merged.Archive.Incremental.Enabled = value.(bool)
		case "archive-incremental-interval-mode":
			merged.Archive.Incremental.IntervalMode = value.(ArchiveIntervalMode)
		case "archive-incremental-interval-seconds":
			merged.Archive.Incremental.IntervalSeconds = value.(int)
		case "compression-incremental":
			merged.Compression.Incremental.Enabled = value.(bool)
		case "compression-incremental-format":
			merged.Compression.Incremental.Format = value.(pathcompression.Format)
		case "compression-snapshot":
			merged.Compression.Snapshot.Enabled = value.(bool)
		case "compression-snapshot-format":
			merged.Compression.Snapshot.Format = value.(pathcompression.Format)
		default:
			plog.Debug("unhandled flag in MergeConfigWithFlags", "flag", name)
		}
	}
	return merged
}
