package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/paulschiretz/pgl-backup/pkg/buildinfo"
	"github.com/paulschiretz/pgl-backup/pkg/flagparse"
	"github.com/paulschiretz/pgl-backup/pkg/lockfile"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// ConfigFileName is the name of the configuration file.
const ConfigFileName = "pgl-backup.config.json"

// systemExcludeFilePatterns is a slice of file patterns that should
// always be excluded from synchronization for the system to function correctly.
var systemExcludeFilePatterns = []string{metafile.MetaFileName, lockfile.LockFileName, ConfigFileName}

// systemExcludeDirPatterns is a slice of directory patterns that should
// always be excluded from synchronization for the system to function correctly.
var systemExcludeDirPatterns = []string{}

type PathsPolicyConfig struct {
	Current         string `json:"current"`
	Archive         string `json:"archive"`
	Content         string `json:"content"`
	BackupDirPrefix string `json:"backupDirPrefix"`
}

type BackupPathsConfig struct {
	Incremental PathsPolicyConfig `json:"incremental,omitempty"`
	Snapshot    PathsPolicyConfig `json:"snapshot,omitempty"`
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

type EnginePerformanceConfig struct {
	SyncWorkers     int `json:"syncWorkers"`
	MirrorWorkers   int `json:"mirrorWorkers"`
	DeleteWorkers   int `json:"deleteWorkers"`
	CompressWorkers int `json:"compressWorkers"`
	BufferSizeKB    int `json:"bufferSizeKB" comment:"Size of the I/O buffer in kilobytes for file copies and compression. Default is 256 (256KB)."`
}

type BackupEngineConfig struct {
	Metrics     bool                    `json:"metrics"`
	FailFast    bool                    `json:"failFast"`
	Performance EnginePerformanceConfig `json:"performance"`
}

type SyncPolicyConfig struct {
	Enabled               bool   `json:"enabled"`
	PreserveSourceDirName bool   `json:"PreserveSourceDirName"`
	Engine                string `json:"engine"`
	RetryCount            int    `json:"retryCount"`
	RetryWaitSeconds      int    `json:"retryWaitSeconds"`
	ModTimeWindowSeconds  int    `json:"modTimeWindowSeconds" comment:"Time window in seconds to consider file modification times equal. Handles filesystem timestamp precision differences. Default is 1s. 0 means exact match."`
}

type BackupSyncConfig struct {
	Incremental         SyncPolicyConfig `json:"incremental,omitempty"`
	Snapshot            SyncPolicyConfig `json:"snapshot,omitempty"`
	DefaultExcludeFiles []string         `json:"defaultExcludeFiles,omitempty"`
	DefaultExcludeDirs  []string         `json:"defaultExcludeDirs,omitempty"`
	// Note: omitempty is intentionally not used for user-configurable slices
	// so that they appear in the generated config file for better discoverability.
	UserExcludeFiles []string `json:"userExcludeFiles"`
	UserExcludeDirs  []string `json:"userExcludeDirs"`
}

type ArchivePolicyConfig struct {
	Enabled bool `json:"enabled"`
	// IntervalMode determines if the interval is set manually or derived automatically from the retention policy.
	IntervalMode string `json:"intervalMode"`
	// IntervalSeconds is the duration in seconds after which a new backup archive is created in incremental mode.
	// This is only used when Mode is 'manual'.
	IntervalSeconds int `json:"intervalSeconds,omitempty"`
}

type BackupArchiveConfig struct {
	Incremental ArchivePolicyConfig `json:"incremental,omitempty"`
	Snapshot    ArchivePolicyConfig `json:"snapshot,omitempty"`
}

type CompressionPolicyConfig struct {
	Enabled bool   `json:"enabled"`
	Format  string `json:"format"`
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

type RuntimeConfig struct {
	Mode   string
	DryRun bool
}

type Config struct {
	Version     string                  `json:"version"`
	Source      string                  `json:"source"`
	TargetBase  string                  `json:"-"` // Never added to config file
	Runtime     RuntimeConfig           `json:"-"` // Never added to config file
	LogLevel    string                  `json:"logLevel"`
	Paths       BackupPathsConfig       `json:"paths"`
	Engine      BackupEngineConfig      `json:"engine"`
	Sync        BackupSyncConfig        `json:"sync"`
	Archive     BackupArchiveConfig     `json:"archive"`
	Retention   BackupRetentionConfig   `json:"retention"`
	Compression BackupCompressionConfig `json:"compression"`
	Hooks       BackupHooksConfig       `json:"hooks"`
}

// NewDefault creates and returns a Config struct with sensible default
// values. It dynamically sets the sync engine based on the operating system.
func NewDefault() Config {
	// Default to the native engine on all platforms. It's highly concurrent and generally offers
	// the best performance and consistency with no external dependencies.
	// Power users on Windows can still opt-in to 'robocopy' as a battle-tested alternative.
	return Config{
		Version:    buildinfo.Version,
		Source:     "",     // Intentionally empty to force user configuration.
		TargetBase: "",     // Intentionally empty to force user configuration.
		LogLevel:   "info", // Default log level.
		Runtime: RuntimeConfig{
			Mode:   "incremental", // Default mode
			DryRun: false,
		},
		Paths: BackupPathsConfig{
			Incremental: PathsPolicyConfig{
				Current:         "PGL_Backup_Incremental_Current", // Default name for the incremental current sub-directory.
				Archive:         "PGL_Backup_Incremental_Archive", // Default name for the incremental archive sub-directory.
				Content:         "PGL_Backup_Content",             // Default name for the incremental content sub-directory.
				BackupDirPrefix: "PGL_Backup_",
			},
			Snapshot: PathsPolicyConfig{
				Current:         "PGL_Backup_Snapshot_Current", // Default name for the snapshot current sub-directory.
				Archive:         "PGL_Backup_Snapshot_Archive", // Default name for the snapshot archive sub-directory.
				Content:         "PGL_Backup_Content",          // Default name for the snapshot content sub-directory.
				BackupDirPrefix: "PGL_Backup_",
			},
		},
		Engine: BackupEngineConfig{
			FailFast: false,
			Metrics:  true, // Default to enabled for detailed performance and file-counting metrics.
			Performance: EnginePerformanceConfig{ // Initialize performance settings here
				SyncWorkers:     4,   // Default to 4. Safe for HDDs (prevents thrashing), decent for SSDs.
				MirrorWorkers:   4,   // Default to 4.
				DeleteWorkers:   4,   // A sensible default for deleting entire backup sets.
				CompressWorkers: 1,   // Default to 1. Our compression libraries (pgzip, zstd) use internal parallelism to utilize all cores for a single file. Increasing this might cause oversubscription.
				BufferSizeKB:    256, // Default to 256KB buffer. Keep it between 64KB-4MB
			}},
		Sync: BackupSyncConfig{
			Incremental: SyncPolicyConfig{
				Enabled:               true, // Enabled by default.
				Engine:                "native",
				RetryCount:            3,    // Default retries on failure.
				RetryWaitSeconds:      5,    // Default wait time between retries.
				ModTimeWindowSeconds:  1,    // Set the default to 1 second
				PreserveSourceDirName: true, // Default to preserving the source folder name in the destination.
			},
			Snapshot: SyncPolicyConfig{
				Enabled:               true, // Enabled by default.
				Engine:                "native",
				RetryCount:            3,    // Default retries on failure.
				RetryWaitSeconds:      5,    // Default wait time between retries.
				ModTimeWindowSeconds:  1,    // Set the default to 1 second
				PreserveSourceDirName: true, // Default to preserving the source folder name in the destination.
			},
			UserExcludeFiles: []string{}, // User-defined list of files to exclude.
			UserExcludeDirs:  []string{}, // User-defined list of directories to exclude.
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
		Archive: BackupArchiveConfig{
			Incremental: ArchivePolicyConfig{
				Enabled:         true,   // Enabled by default for incremental mode.
				IntervalMode:    "auto", // Default to auto-adjusting the interval based on the retention policy.
				IntervalSeconds: 86400,  // Interval will be calculated by the engine in 'auto' mode. Default 24h.
				// If a user switches to 'manual' mode, they must specify an interval.
			},
			Snapshot: ArchivePolicyConfig{
				Enabled:         true,     // Enabled by default for snapshot mode.
				IntervalMode:    "manual", // Manual mode per default.
				IntervalSeconds: 0,        // Always move to archive
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
				Weeks:   4, // Default: Keep one backup for each of the last 4 weeks.
				Months:  0,
				Years:   0,
			},
		},
		Compression: BackupCompressionConfig{
			Incremental: CompressionPolicyConfig{
				Enabled: true,
				Format:  "tar.zst",
			},
			Snapshot: CompressionPolicyConfig{
				Enabled: true,
				Format:  "tar.zst",
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
func Load(targetBase string) (Config, error) {

	absTargetBasePath, err := filepath.Abs(targetBase)
	if err != nil {
		return Config{}, fmt.Errorf("could not determine absolute path for load directory %s: %w", targetBase, err)
	}

	configPath := filepath.Join(absTargetBasePath, ConfigFileName)

	file, err := os.Open(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return NewDefault(), nil // Config file doesn't exist, which is a normal case.
		}
		return Config{}, fmt.Errorf("error opening config file %s: %w", configPath, err)
	}
	defer file.Close()

	plog.Info("Loading configuration", "path", configPath)
	// Start with default values, then overwrite with the file's content.
	// This makes the config loading resilient to missing fields in the JSON file.
	// NOTE: if config.Version differes from appVersion we can add a migration step here.
	config := NewDefault()
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("error parsing config file %s: %w", configPath, err)
	}

	// After loading, validate that if there is a targetBase in the config file it matches the
	// directory it was loaded from. This prevents using a config file in the wrong directory.
	// NOTE: there should never be a targetBase in the config!
	if config.TargetBase != "" {
		absTargetInConfig, err := filepath.Abs(config.TargetBase)
		if err != nil {
			return Config{}, fmt.Errorf("could not determine absolute path for targetBase in config %s: %w", config.TargetBase, err)
		}

		if absTargetBasePath != absTargetInConfig {
			return Config{}, fmt.Errorf("targetBase in config file (%s) does not match the directory it was loaded from (%s)", absTargetInConfig, absTargetBasePath)
		}
	} else {
		// Set the target base
		config.TargetBase = absTargetBasePath
	}

	// At this point our config has been migrated if needed so override the version in the struct
	if config.Version != buildinfo.Version {
		config.Version = buildinfo.Version
	}
	return config, nil
}

// Generate creates or overwrites a default pgl-backup.config.json file in the specified
// target directory.
func Generate(configToGenerate Config) error {
	configPath := filepath.Join(configToGenerate.TargetBase, ConfigFileName)
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
	if checkSource && c.Source == "" {
		return fmt.Errorf("source path cannot be empty")
	}
	if c.TargetBase == "" {
		return fmt.Errorf("target path cannot be empty")
	}

	// Clean and expand paths for canonical representation before use.
	var err error

	// --- Validate Source Path ---
	if c.Source != "" {
		c.Source, err = util.ExpandPath(c.Source)
		if err != nil {
			return fmt.Errorf("could not expand source path: %w", err)
		}
		c.Source = filepath.Clean(c.Source)

		// After cleaning and expanding the path, check for existence.
		if checkSource {
			if _, err := os.Stat(c.Source); os.IsNotExist(err) {
				return fmt.Errorf("source path '%s' does not exist", c.Source)
			}
		}
	}

	// --- Validate Target Path ---
	if c.TargetBase != "" {
		c.TargetBase, err = util.ExpandPath(c.TargetBase)
		if err != nil {
			return fmt.Errorf("could not expand target path: %w", err)
		}
		c.TargetBase = filepath.Clean(c.TargetBase)
	}

	switch c.Runtime.Mode {
	case "incremental":
		if c.Paths.Incremental.Archive == "" {
			return fmt.Errorf("paths.incremental.archive cannot be empty")
		}
		if c.Paths.Incremental.Current == "" {
			return fmt.Errorf("paths.incremental.current cannot be empty")
		}
		if c.Paths.Incremental.Content == "" {
			return fmt.Errorf("paths.incremental.content cannot be empty")
		}
		if c.Paths.Incremental.Current == c.Paths.Incremental.Archive {
			return fmt.Errorf("paths.incremental.current and paths.incremental.archive cannot be the same")
		}
		if c.Paths.Incremental.Current == c.Paths.Incremental.Content {
			return fmt.Errorf("paths.incremental.current and paths.incremental.content cannot be the same")
		}
		if c.Paths.Incremental.Archive == c.Paths.Incremental.Content {
			return fmt.Errorf("paths.incremental.archive and paths.incremental.content cannot be the same")
		}
		// Disallow path separators to ensure the archives directory is a direct child of the target.
		// This is critical for guaranteeing that the atomic `os.Rename` operation during archive
		// works correctly, as it requires the source and destination to be on the same filesystem.
		if strings.ContainsAny(c.Paths.Incremental.Archive, `\/`) {
			return fmt.Errorf("paths.incremental.archive cannot contain path separators ('/' or '\\')")
		}
		if strings.ContainsAny(c.Paths.Incremental.Current, `\/`) {
			return fmt.Errorf("paths.incremental.current cannot contain path separators ('/' or '\\')")
		}
		if strings.ContainsAny(c.Paths.Incremental.Content, `\/`) {
			return fmt.Errorf("paths.incremental.content cannot contain path separators ('/' or '\\')")
		}

		if c.Sync.Incremental.RetryCount < 0 {
			return fmt.Errorf("sync.incremental.retryCount cannot be negative")
		}
		if c.Sync.Incremental.RetryWaitSeconds < 0 {
			return fmt.Errorf("sync.incremental.retryWaitSeconds cannot be negative")
		}
		if c.Sync.Incremental.ModTimeWindowSeconds < 0 {
			return fmt.Errorf("sync.incremental.modTimeWindowSeconds cannot be negative")
		}

		if c.Archive.Incremental.IntervalMode == "manual" && c.Archive.Incremental.IntervalSeconds < 0 {
			return fmt.Errorf("archive.incremental.intervalSeconds cannot be negative when mode is 'manual'.")
		}

	case "snapshot":
		if c.Paths.Snapshot.Archive == "" {
			return fmt.Errorf("paths.snapshot.archive cannot be empty")
		}
		if c.Paths.Snapshot.Current == "" {
			return fmt.Errorf("paths.snapshot.current cannot be empty")
		}
		if c.Paths.Snapshot.Content == "" {
			return fmt.Errorf("paths.snapshot.content cannot be empty")
		}
		if c.Paths.Snapshot.Current == c.Paths.Snapshot.Archive {
			return fmt.Errorf("paths.snapshot.current and paths.snapshot.archive cannot be the same")
		}
		if c.Paths.Snapshot.Current == c.Paths.Snapshot.Content {
			return fmt.Errorf("paths.snapshot.current and paths.snapshot.content cannot be the same")
		}
		if c.Paths.Snapshot.Archive == c.Paths.Snapshot.Content {
			return fmt.Errorf("paths.snapshot.archive and paths.snapshot.content cannot be the same")
		}
		// Disallow path separators to ensure the archives directory is a direct child of the target.
		// This is critical for guaranteeing that the atomic `os.Rename` operation during archive
		// works correctly, as it requires the source and destination to be on the same filesystem.
		if strings.ContainsAny(c.Paths.Snapshot.Archive, `\/`) {
			return fmt.Errorf("paths.snapshot.archive cannot contain path separators ('/' or '\\')")
		}
		if strings.ContainsAny(c.Paths.Snapshot.Current, `\/`) {
			return fmt.Errorf("paths.snapshot.current cannot contain path separators ('/' or '\\')")
		}
		if strings.ContainsAny(c.Paths.Snapshot.Content, `\/`) {
			return fmt.Errorf("paths.snapshot.content cannot contain path separators ('/' or '\\')")
		}

		if c.Sync.Snapshot.RetryCount < 0 {
			return fmt.Errorf("sync.snapshot.retryCount cannot be negative")
		}
		if c.Sync.Snapshot.RetryWaitSeconds < 0 {
			return fmt.Errorf("sync.snapshot.retryWaitSeconds cannot be negative")
		}
		if c.Sync.Snapshot.ModTimeWindowSeconds < 0 {
			return fmt.Errorf("sync.snapshot.modTimeWindowSeconds cannot be negative")
		}

		if c.Archive.Snapshot.IntervalMode == "manual" && c.Archive.Incremental.IntervalSeconds < 0 {
			return fmt.Errorf("archive.snapshot.intervalSeconds cannot be negative when mode is 'manual'.")
		}
	}

	// --- Validate Engine and Mode Settings ---
	if c.Engine.Performance.SyncWorkers < 1 {
		return fmt.Errorf("engine.performance.syncWorkers must be at least 1")
	}
	if c.Engine.Performance.MirrorWorkers < 1 {
		return fmt.Errorf("engine.performance.mirrorWorkers must be at least 1")
	}
	if c.Engine.Performance.CompressWorkers < 1 {
		return fmt.Errorf("engine.performance.compressWorkers must be at least 1")
	}
	if c.Engine.Performance.DeleteWorkers < 1 {
		return fmt.Errorf("engine.performance.deleteWorkers must be at least 1")
	}
	if c.Engine.Performance.BufferSizeKB <= 0 {
		return fmt.Errorf("engine.performance.bufferSizeKB must be greater than 0")
	}

	if err := validateGlobPatterns("defaultExcludeFiles", c.Sync.DefaultExcludeFiles); err != nil {
		return err
	}

	if err := validateGlobPatterns("userExcludeFiles", c.Sync.UserExcludeFiles); err != nil {
		return err
	}

	if err := validateGlobPatterns("defaultExcludeDirs", c.Sync.DefaultExcludeDirs); err != nil {
		return err
	}

	if err := validateGlobPatterns("userExcludeDirs", c.Sync.UserExcludeDirs); err != nil {
		return err
	}
	return nil
}

// LogSummary prints a user-friendly summary of the configuration to the
// provided logger. It respects the 'Quiet' setting.
func (c *Config) LogSummary() {
	logArgs := []interface{}{
		"mode", c.Runtime.Mode,
		"log_level", c.LogLevel,
		"source", c.Source,
		"target", c.TargetBase,
		"dry_run", c.Runtime.DryRun,
		"sync_workers", c.Engine.Performance.SyncWorkers,
		"mirror_workers", c.Engine.Performance.MirrorWorkers,
		"metrics", c.Engine.Metrics,
		"compress_workers", c.Engine.Performance.CompressWorkers,
		"delete_workers", c.Engine.Performance.DeleteWorkers,
		"buffer_size_kb", c.Engine.Performance.BufferSizeKB,
	}
	switch c.Runtime.Mode {
	case "incremental":
		logArgs = append(logArgs, "current_subdir", c.Paths.Incremental.Current)
		logArgs = append(logArgs, "archive_subdir", c.Paths.Incremental.Archive)
		logArgs = append(logArgs, "content_subdir", c.Paths.Incremental.Content)
		if c.Sync.Incremental.Enabled {
			syncSummary := fmt.Sprintf("enabled (e:%s)",
				c.Sync.Incremental.Engine)
			logArgs = append(logArgs, "sync", syncSummary)
		}

		if c.Archive.Incremental.Enabled {
			switch c.Archive.Incremental.IntervalMode {
			case "manual":
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

	case "snapshot":
		logArgs = append(logArgs, "current_subdir", c.Paths.Snapshot.Current)
		logArgs = append(logArgs, "archive_subdir", c.Paths.Snapshot.Archive)
		logArgs = append(logArgs, "content_subdir", c.Paths.Snapshot.Content)
		if c.Sync.Snapshot.Enabled {
			syncSummary := fmt.Sprintf("enabled (e:%s)",
				c.Sync.Snapshot.Engine)
			logArgs = append(logArgs, "sync", syncSummary)
		}

		if c.Archive.Snapshot.Enabled {
			switch c.Archive.Snapshot.IntervalMode {
			case "manual":
				archiveSummary := fmt.Sprintf("enabled (m:%s i:%ds)",
					c.Archive.Snapshot.IntervalMode,
					c.Archive.Snapshot.IntervalSeconds)
				logArgs = append(logArgs, "archive", archiveSummary)
			default:
				archiveSummary := fmt.Sprintf("enabled (m:%s)",
					c.Archive.Snapshot.IntervalMode)
				logArgs = append(logArgs, "archive", archiveSummary)
			}
		}

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
	if finalExcludeFiles := c.Sync.ExcludeFiles(); len(finalExcludeFiles) > 0 {
		logArgs = append(logArgs, "exclude_files", strings.Join(finalExcludeFiles, ", "))
	}
	if finalExcludeDirs := c.Sync.ExcludeDirs(); len(finalExcludeDirs) > 0 {
		logArgs = append(logArgs, "exclude_dirs", strings.Join(finalExcludeDirs, ", "))
	}
	if len(c.Sync.DefaultExcludeFiles) > 0 {
		logArgs = append(logArgs, "default_exclude_files", strings.Join(c.Sync.DefaultExcludeFiles, ", "))
	}
	if len(c.Sync.DefaultExcludeDirs) > 0 {
		logArgs = append(logArgs, "default_exclude_dirs", strings.Join(c.Sync.DefaultExcludeDirs, ", "))
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
func (s *BackupSyncConfig) ExcludeFiles() []string {
	return util.MergeAndDeduplicate(systemExcludeFilePatterns, s.DefaultExcludeFiles, s.UserExcludeFiles)
}

// ExcludeDirs returns the final, combined slice of directory exclusion patterns, including
// non-overridable system patterns, default patterns, and user-configured patterns.
// It automatically handles deduplication.
func (s *BackupSyncConfig) ExcludeDirs() []string {
	return util.MergeAndDeduplicate(systemExcludeDirPatterns, s.DefaultExcludeDirs, s.UserExcludeDirs)
}

// MergeConfigWithFlags overlays the configuration values from flags on top of a base
// configuration. It iterates over the setFlags map, which contains only the flags
// explicitly provided by the user on the command line.
func MergeConfigWithFlags(command flagparse.Command, base Config, setFlags map[string]any) Config {
	merged := base

	for name, value := range setFlags {
		switch name {
		case "source":
			merged.Source = value.(string)
		case "target":
			merged.TargetBase = value.(string)
		case "log-level":
			merged.LogLevel = value.(string)
		case "fail-fast":
			merged.Engine.FailFast = value.(bool)
		case "metrics":
			merged.Engine.Metrics = value.(bool)
		case "mode":
			switch command {
			case flagparse.Backup:
				merged.Runtime.Mode = value.(string)
			default:
			}
		case "dry-run":
			merged.Runtime.DryRun = value.(bool)
		case "sync-workers":
			merged.Engine.Performance.SyncWorkers = value.(int)
		case "mirror-workers":
			merged.Engine.Performance.MirrorWorkers = value.(int)
		case "delete-workers":
			merged.Engine.Performance.DeleteWorkers = value.(int)
		case "compress-workers":
			merged.Engine.Performance.CompressWorkers = value.(int)
		case "buffer-size-kb":
			merged.Engine.Performance.BufferSizeKB = value.(int)
		case "sync-incremental-engine":
			merged.Sync.Incremental.Engine = value.(string)
		case "sync-incremental-retry-count":
			merged.Sync.Incremental.RetryCount = value.(int)
		case "sync-incremental-retry-wait":
			merged.Sync.Incremental.RetryWaitSeconds = value.(int)
		case "sync-incremental-mod-time-window":
			merged.Sync.Incremental.ModTimeWindowSeconds = value.(int)
		case "sync-incremental-preserve-source-dir-name":
			merged.Sync.Incremental.PreserveSourceDirName = value.(bool)
		case "sync-snapshot-engine":
			merged.Sync.Snapshot.Engine = value.(string)
		case "sync-snapshot-retry-count":
			merged.Sync.Snapshot.RetryCount = value.(int)
		case "sync-snapshot-retry-wait":
			merged.Sync.Snapshot.RetryWaitSeconds = value.(int)
		case "sync-snapshot-mod-time-window":
			merged.Sync.Snapshot.ModTimeWindowSeconds = value.(int)
		case "sync-snapshot-preserve-source-dir-name":
			merged.Sync.Snapshot.PreserveSourceDirName = value.(bool)
		case "user-exclude-files":
			merged.Sync.UserExcludeFiles = value.([]string)
		case "user-exclude-dirs":
			merged.Sync.UserExcludeDirs = value.([]string)
		case "pre-backup-hooks":
			merged.Hooks.PreBackup = value.([]string)
		case "post-backup-hooks":
			merged.Hooks.PostBackup = value.([]string)
		case "archive-incremental":
			merged.Archive.Incremental.Enabled = value.(bool)
		case "archive-incremental-interval-mode":
			merged.Archive.Incremental.IntervalMode = value.(string)
		case "archive-incremental-interval-seconds":
			merged.Archive.Incremental.IntervalSeconds = value.(int)
		case "archive-snapshot":
			merged.Archive.Snapshot.Enabled = value.(bool)
		case "archive-snapshot-interval-mode":
			merged.Archive.Snapshot.IntervalMode = value.(string)
		case "archive-snapshot-interval-seconds":
			merged.Archive.Snapshot.IntervalSeconds = value.(int)
		case "compression-incremental":
			merged.Compression.Incremental.Enabled = value.(bool)
		case "compression-incremental-format":
			merged.Compression.Incremental.Format = value.(string)
		case "compression-snapshot":
			merged.Compression.Snapshot.Enabled = value.(bool)
		case "compression-snapshot-format":
			merged.Compression.Snapshot.Format = value.(string)
		default:
			plog.Debug("unhandled flag in MergeConfigWithFlags", "flag", name)
		}
	}
	return merged
}
