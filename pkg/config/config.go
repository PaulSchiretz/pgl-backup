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

type PathConfig struct {
	Current            string `json:"current"`
	Archive            string `json:"archive"`
	Content            string `json:"content"`
	ArchiveEntryPrefix string `json:"archiveEntryPrefix"`
}

type PathsConfig struct {
	Incremental PathConfig `json:"incremental,omitempty"`
	Snapshot    PathConfig `json:"snapshot,omitempty"`
}

type HooksConfig struct {
	// Note: omitempty is intentionally not used so that the hook fields
	// appear in the generated config file for better discoverability.
	// PreBackup is a list of shell commands to execute before the backup sync begins.
	// SECURITY: These commands are executed as provided. Ensure they are from a trusted source.
	PreBackup []string `json:"preBackup"`
	// PostBackup is a list of shell commands to execute after the backup sync completes.
	// SECURITY: These commands are executed as provided. Ensure they are from a trusted source.
	PostBackup []string `json:"postBackup"`
	// PreRestore is a list of shell commands to execute before the restore begins.
	// SECURITY: These commands are executed as provided. Ensure they are from a trusted source.
	PreRestore []string `json:"preRestore"`
	// PostRestore is a list of shell commands to execute after the restore completes.
	// SECURITY: These commands are executed as provided. Ensure they are from a trusted source.
	PostRestore []string `json:"postRestore"`
}

type EnginePerformanceConfig struct {
	SyncWorkers      int   `json:"syncWorkers"`
	MirrorWorkers    int   `json:"mirrorWorkers"`
	DeleteWorkers    int   `json:"deleteWorkers"`
	CompressWorkers  int   `json:"compressWorkers"`
	BufferSizeKB     int64 `json:"bufferSizeKB" comment:"Size of the I/O buffer in kilobytes for file copies and compression. Default is 1024 (1MB)."`
	ReadAheadLimitKB int64 `json:"readAheadLimitKB" comment:"Limit of the I/O readahead in kilobytes for file compression. Default is 262144 (256MB)."`
}

type EngineConfig struct {
	Metrics     bool                    `json:"metrics"`
	FailFast    bool                    `json:"failFast"`
	Performance EnginePerformanceConfig `json:"performance"`
}

type SyncConfig struct {
	Enabled               bool     `json:"enabled"`
	PreserveSourceDirName bool     `json:"preserveSourceDirName"`
	Engine                string   `json:"engine"`
	DisableSafeCopy       bool     `json:"disableSafeCopy"`
	RetryCount            int      `json:"retryCount"`
	RetryWaitSeconds      int      `json:"retryWaitSeconds"`
	ModTimeWindowSeconds  int      `json:"modTimeWindowSeconds" comment:"Time window in seconds to consider file modification times equal. Handles filesystem timestamp precision differences. Default is 1s. 0 means exact match."`
	DefaultExcludeFiles   []string `json:"defaultExcludeFiles,omitempty"`
	DefaultExcludeDirs    []string `json:"defaultExcludeDirs,omitempty"`
	// Note: omitempty is intentionally not used for user-configurable slices
	// so that they appear in the generated config file for better discoverability.
	UserExcludeFiles []string `json:"userExcludeFiles"`
	UserExcludeDirs  []string `json:"userExcludeDirs"`
}

type ArchiveConfig struct {
	Enabled bool `json:"enabled"`
	// IntervalMode determines if the interval is set manually or derived automatically from the retention policy.
	IntervalMode string `json:"intervalMode"`
	// IntervalSeconds is the duration in seconds after which a new backup archive is created in incremental mode.
	// This is only used when Mode is 'manual'.
	IntervalSeconds int `json:"intervalSeconds,omitempty"`
}

type CompressionConfig struct {
	Enabled bool   `json:"enabled"`
	Format  string `json:"format"`
	Level   string `json:"level" comment:"Compression level: 'default', 'fastest', 'better', 'best'."`
}

type RetentionPolicy struct {
	Enabled bool `json:"enabled"`
	Hours   int  `json:"hours"`
	Days    int  `json:"days"`
	Weeks   int  `json:"weeks"`
	Months  int  `json:"months"`
	Years   int  `json:"years"`
}

type RetentionConfig struct {
	Incremental RetentionPolicy `json:"incremental,omitempty"`
	Snapshot    RetentionPolicy `json:"snapshot,omitempty"`
}

type RuntimeConfig struct {
	Mode                     string
	DryRun                   bool
	BackupOverwriteBehavior  string
	ListSort                 string
	RestoreOverwriteBehavior string
	IgnoreCaseMismatch       bool
}

type Config struct {
	Version     string            `json:"version"`
	Runtime     RuntimeConfig     `json:"-"` // Never added to config file
	LogLevel    string            `json:"logLevel"`
	Paths       PathsConfig       `json:"paths"`
	Engine      EngineConfig      `json:"engine"`
	Sync        SyncConfig        `json:"sync"`
	Archive     ArchiveConfig     `json:"archive"`
	Retention   RetentionConfig   `json:"retention"`
	Compression CompressionConfig `json:"compression"`
	Hooks       HooksConfig       `json:"hooks"`
}

// NewDefault creates and returns a Config struct with sensible default
// values. It dynamically sets the sync engine based on the operating system.
func NewDefault() Config {
	// Default to the native engine on all platforms. It's highly concurrent and generally offers
	// the best performance and consistency with no external dependencies.
	return Config{
		Version:  buildinfo.Version,
		LogLevel: "info", // Default log level.
		Runtime: RuntimeConfig{
			Mode:                     "any", // Default mode, 'any' defaults to incremental for backup command
			DryRun:                   false,
			BackupOverwriteBehavior:  "update", // Default to update behavior for backups
			RestoreOverwriteBehavior: "never",  // Default to never overwrite for restores
		},
		Paths: PathsConfig{
			Incremental: PathConfig{
				Current:            "PGL_Backup_Incremental_Current", // Default name for the incremental current sub-directory.
				Archive:            "PGL_Backup_Incremental_Archive", // Default name for the incremental archive sub-directory.
				Content:            "PGL_Backup_Content",             // Default name for the incremental content sub-directory.
				ArchiveEntryPrefix: "PGL_Backup_",
			},
			Snapshot: PathConfig{
				Current:            "PGL_Backup_Snapshot_Current", // Default name for the snapshot current sub-directory.
				Archive:            "PGL_Backup_Snapshot_Archive", // Default name for the snapshot archive sub-directory.
				Content:            "PGL_Backup_Content",          // Default name for the snapshot content sub-directory.
				ArchiveEntryPrefix: "PGL_Backup_",
			},
		},
		Engine: EngineConfig{
			FailFast: false,
			Metrics:  true, // Default to enabled for detailed performance and file-counting metrics.
			Performance: EnginePerformanceConfig{ // Initialize performance settings here
				SyncWorkers:      4,      // Default to 4. Safe for HDDs (prevents thrashing), decent for SSDs.
				MirrorWorkers:    4,      // Default to 4.
				DeleteWorkers:    4,      // A sensible default for deleting entire backup sets.
				CompressWorkers:  4,      // Default to 4.
				BufferSizeKB:     1024,   // Default to 1024KB (1MB) buffer. Keep it between 64KB-4MB
				ReadAheadLimitKB: 262144, // Default to 256MB buffer.
			}},
		Sync: SyncConfig{
			Enabled:               true, // Enabled by default.
			Engine:                "native",
			DisableSafeCopy:       false,      // Default use safe copy (rename/copy)
			RetryCount:            3,          // Default retries on failure.
			RetryWaitSeconds:      5,          // Default wait time between retries.
			ModTimeWindowSeconds:  1,          // Set the default to 1 second
			PreserveSourceDirName: true,       // Default to preserving the source folder name in the destination.
			UserExcludeFiles:      []string{}, // User-defined list of files to exclude.
			UserExcludeDirs:       []string{}, // User-defined list of directories to exclude.
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
		Archive: ArchiveConfig{
			Enabled:         true,   // Enabled by default for incremental mode.
			IntervalMode:    "auto", // Default to auto-adjusting the interval based on the retention policy.
			IntervalSeconds: 86400,  // Interval will be calculated by the engine in 'auto' mode. Default 24h.
		},
		Retention: RetentionConfig{
			Incremental: RetentionPolicy{
				Enabled: true, // Enabled by default for incremental mode.
				Hours:   0,    // Default: No hourly backups.
				Days:    0,    // Default: No daily backups.
				Weeks:   4,    // Default: Keep one backup for each of the last 4 weeks.
				Months:  0,    // Default: No monthly backups.
				Years:   0,    // Default: No yearly backups.
			},
			Snapshot: RetentionPolicy{
				Enabled: false, // Disabled by default to protect snapshots.
				Hours:   -1,
				Days:    -1,
				Weeks:   -1,
				Months:  -1,
				Years:   -1,
			},
		},
		Compression: CompressionConfig{
			Enabled: true,
			Format:  "tar.zst",
			Level:   "default",
		},
		Hooks: HooksConfig{
			PreBackup:   []string{},
			PostBackup:  []string{},
			PreRestore:  []string{},
			PostRestore: []string{},
		},
	}
}

// Load attempts to load a configuration from "pgl-backup.config.json".
// If the file doesn't exist, it returns the provided default config without an error.
// If the file exists but fails to parse, it returns an error and a zero-value config.
func Load(absBasePath string) (Config, error) {

	absConfigFilePath := util.DenormalizePath(filepath.Join(absBasePath, ConfigFileName))

	file, err := os.Open(absConfigFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return NewDefault(), nil // Config file doesn't exist, which is a normal case.
		}
		return Config{}, fmt.Errorf("error opening config file %s: %w", absConfigFilePath, err)
	}
	defer file.Close()

	plog.Info("Loading configuration", "path", absConfigFilePath)
	// Start with default values, then overwrite with the file's content.
	// This makes the config loading resilient to missing fields in the JSON file.
	// NOTE: if config.Version differs from appVersion we can add a migration step here.
	config := NewDefault()
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("error parsing config file %s: %w", absConfigFilePath, err)
	}

	// At this point our config has been migrated if needed so override the version in the struct
	if config.Version != buildinfo.Version {
		config.Version = buildinfo.Version
	}
	return config, nil
}

// Generate creates or overwrites a default pgl-backup.config.json file in the specified
// target directory.
func Generate(absBasePath string, configToGenerate Config) error {
	absConfigFilePath := util.DenormalizePath(filepath.Join(absBasePath, ConfigFileName))
	// Marshal the default config into nicely formatted JSON.
	jsonData, err := json.MarshalIndent(configToGenerate, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal default config to JSON: %w", err)
	}

	// Write the JSON data to the file.
	if err := os.WriteFile(absConfigFilePath, jsonData, util.UserWritableFilePerms); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	plog.Info("Successfully saved config file", "path", absConfigFilePath)
	return nil
}

// Validate checks the configuration for logical errors and inconsistencies.
// It performs strict checks, including ensuring the source path is non-empty
// and exists.
func (c *Config) Validate() error {

	// Normalize enums to lowercase to ensure case-insensitive behavior
	c.Runtime.Mode = strings.ToLower(c.Runtime.Mode)
	c.Archive.IntervalMode = strings.ToLower(c.Archive.IntervalMode)
	c.Compression.Format = strings.ToLower(c.Compression.Format)
	c.Compression.Level = strings.ToLower(c.Compression.Level)
	c.Sync.Engine = strings.ToLower(c.Sync.Engine)
	c.Runtime.BackupOverwriteBehavior = strings.ToLower(c.Runtime.BackupOverwriteBehavior)
	c.Runtime.ListSort = strings.ToLower(c.Runtime.ListSort)
	c.Runtime.RestoreOverwriteBehavior = strings.ToLower(c.Runtime.RestoreOverwriteBehavior)

	// --- Validate Shared Settings ---
	if c.Sync.RetryCount < 0 {
		return fmt.Errorf("sync.retryCount cannot be negative")
	}
	if c.Sync.RetryWaitSeconds < 0 {
		return fmt.Errorf("sync.retryWaitSeconds cannot be negative")
	}
	if c.Sync.ModTimeWindowSeconds < 0 {
		return fmt.Errorf("sync.modTimeWindowSeconds cannot be negative")
	}
	if c.Archive.IntervalMode == "manual" && c.Archive.IntervalSeconds < 0 {
		return fmt.Errorf("archive.intervalSeconds cannot be negative when mode is 'manual'.")
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

		if strings.EqualFold(c.Paths.Incremental.Current, c.Paths.Incremental.Archive) {
			return fmt.Errorf("paths.incremental.current and paths.incremental.archive cannot be the same")
		}
		if strings.EqualFold(c.Paths.Incremental.Current, c.Paths.Incremental.Content) {
			return fmt.Errorf("paths.incremental.current and paths.incremental.content cannot be the same")
		}
		if strings.EqualFold(c.Paths.Incremental.Archive, c.Paths.Incremental.Content) {
			return fmt.Errorf("paths.incremental.archive and paths.incremental.content cannot be the same")
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

		if strings.EqualFold(c.Paths.Snapshot.Current, c.Paths.Snapshot.Archive) {
			return fmt.Errorf("paths.snapshot.current and paths.snapshot.archive cannot be the same")
		}
		if strings.EqualFold(c.Paths.Snapshot.Current, c.Paths.Snapshot.Content) {
			return fmt.Errorf("paths.snapshot.current and paths.snapshot.content cannot be the same")
		}
		if strings.EqualFold(c.Paths.Snapshot.Archive, c.Paths.Snapshot.Content) {
			return fmt.Errorf("paths.snapshot.archive and paths.snapshot.content cannot be the same")
		}
	}

	// --- Validate Engine and Mode Settings ---
	if c.Engine.Performance.SyncWorkers < 1 {
		return fmt.Errorf("engine.performance.syncWorkers must be at least 1")
	}
	if c.Engine.Performance.MirrorWorkers < 1 {
		return fmt.Errorf("engine.performance.mirrorWorkers must be at least 1")
	}
	if c.Engine.Performance.DeleteWorkers < 1 {
		return fmt.Errorf("engine.performance.deleteWorkers must be at least 1")
	}
	if c.Engine.Performance.CompressWorkers < 1 {
		return fmt.Errorf("engine.performance.compressWorkers must be at least 1")
	}
	if c.Engine.Performance.BufferSizeKB <= 0 {
		return fmt.Errorf("engine.performance.bufferSizeKB must be greater than 0")
	}
	if c.Engine.Performance.ReadAheadLimitKB < 0 { // 0 means disabled
		return fmt.Errorf("engine.performance.readAheadLimitKB must be at least 0")
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

	// Validate Retention Policies
	// We strictly enforce that if retention is enabled, all values must be non-negative.
	// This prevents the dangerous situation where a user enables retention (e.g. for snapshots)
	// but forgets to configure the values (leaving them at the default -1).
	// By failing fast, we force the user to be explicit about their retention desires.
	// NOTE: We always validate snapshot and incremental policies regardless of the mode, cause the prune command uses both
	if c.Retention.Incremental.Enabled {
		if c.Retention.Incremental.Hours < 0 || c.Retention.Incremental.Days < 0 || c.Retention.Incremental.Weeks < 0 || c.Retention.Incremental.Months < 0 || c.Retention.Incremental.Years < 0 {
			return fmt.Errorf("retention.incremental is enabled but contains negative values. All values must be >= 0")
		}
	}
	if c.Retention.Snapshot.Enabled {
		if c.Retention.Snapshot.Hours < 0 || c.Retention.Snapshot.Days < 0 || c.Retention.Snapshot.Weeks < 0 || c.Retention.Snapshot.Months < 0 || c.Retention.Snapshot.Years < 0 {
			return fmt.Errorf("retention.snapshot is enabled but contains negative values. All values must be >= 0")
		}
	}
	return nil
}

// LogSummary prints a user-friendly summary of the configuration to the
// provided logger. It respects the 'Quiet' setting.
func (c *Config) LogSummary(command flagparse.Command, absBasePath, absSourcePath, absTargetPath, uuid string) {
	logArgs := []any{
		"log_level", c.LogLevel,
		"base", absBasePath,
		"dry_run", c.Runtime.DryRun,
	}

	if command != flagparse.Init {
		logArgs = append(logArgs, "metrics", c.Engine.Metrics)
	}

	switch command {
	case flagparse.Backup:
		logArgs = append(logArgs, "source", absSourcePath)
		logArgs = append(logArgs, "mode", c.Runtime.Mode)
		logArgs = append(logArgs, "sync_workers", c.Engine.Performance.SyncWorkers)
		logArgs = append(logArgs, "mirror_workers", c.Engine.Performance.MirrorWorkers)
		logArgs = append(logArgs, "delete_workers", c.Engine.Performance.DeleteWorkers)
		logArgs = append(logArgs, "delete_workers", c.Engine.Performance.CompressWorkers)
		logArgs = append(logArgs, "buffer_size_kb", c.Engine.Performance.BufferSizeKB)
		logArgs = append(logArgs, "readahead_limit_kb", c.Engine.Performance.ReadAheadLimitKB)
		logArgs = append(logArgs, "overwrite", c.Runtime.BackupOverwriteBehavior)

		if c.Sync.Enabled {
			syncSummary := fmt.Sprintf("enabled (e:%s)", c.Sync.Engine)
			logArgs = append(logArgs, "sync", syncSummary)
		}

		if c.Compression.Enabled {
			if command == flagparse.Backup {
				compressionSummary := fmt.Sprintf("enabled (f:%s l:%s)", c.Compression.Format, c.Compression.Level)
				logArgs = append(logArgs, "compression", compressionSummary)
			} else {
				logArgs = append(logArgs, "compression", "enabled")
			}
		}

		switch c.Runtime.Mode {
		case "incremental":
			logArgs = append(logArgs, "current_subdir", c.Paths.Incremental.Current)
			logArgs = append(logArgs, "archive_subdir", c.Paths.Incremental.Archive)
			logArgs = append(logArgs, "content_subdir", c.Paths.Incremental.Content)

			if c.Archive.Enabled {
				switch c.Archive.IntervalMode {
				case "manual":
					archiveSummary := fmt.Sprintf("enabled (m:%s i:%ds)",
						c.Archive.IntervalMode,
						c.Archive.IntervalSeconds)
					logArgs = append(logArgs, "archive", archiveSummary)
				default:
					archiveSummary := fmt.Sprintf("enabled (m:%s)",
						c.Archive.IntervalMode)
					logArgs = append(logArgs, "archive", archiveSummary)
				}
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

		if len(c.Hooks.PreBackup) > 0 {
			logArgs = append(logArgs, "pre_backup_hooks", strings.Join(c.Hooks.PreBackup, "; "))
		}
		if len(c.Hooks.PostBackup) > 0 {
			logArgs = append(logArgs, "post_backup_hooks", strings.Join(c.Hooks.PostBackup, "; "))
		}

	case flagparse.Restore:
		logArgs = append(logArgs, "target", absTargetPath)
		logArgs = append(logArgs, "uuid", uuid)
		logArgs = append(logArgs, "mode", c.Runtime.Mode)
		logArgs = append(logArgs, "sync_workers", c.Engine.Performance.SyncWorkers)
		logArgs = append(logArgs, "buffer_size_kb", c.Engine.Performance.BufferSizeKB)
		logArgs = append(logArgs, "readahead_limit_kb", c.Engine.Performance.ReadAheadLimitKB)
		logArgs = append(logArgs, "overwrite", c.Runtime.RestoreOverwriteBehavior)

		if c.Sync.Enabled {
			syncSummary := fmt.Sprintf("enabled (e:%s)", c.Sync.Engine)
			logArgs = append(logArgs, "sync", syncSummary)
		}

		if c.Compression.Enabled {
			if command == flagparse.Backup {
				compressionSummary := fmt.Sprintf("enabled (f:%s l:%s)", c.Compression.Format, c.Compression.Level)
				logArgs = append(logArgs, "compression", compressionSummary)
			} else {
				logArgs = append(logArgs, "compression", "enabled")
			}
		}

		if finalExcludeFiles := c.Sync.ExcludeFiles(); len(finalExcludeFiles) > 0 {
			logArgs = append(logArgs, "exclude_files", strings.Join(finalExcludeFiles, ", "))
		}
		if finalExcludeDirs := c.Sync.ExcludeDirs(); len(finalExcludeDirs) > 0 {
			logArgs = append(logArgs, "exclude_dirs", strings.Join(finalExcludeDirs, ", "))
		}

		if len(c.Hooks.PreRestore) > 0 {
			logArgs = append(logArgs, "pre_restore_hooks", strings.Join(c.Hooks.PreRestore, "; "))
		}
		if len(c.Hooks.PostRestore) > 0 {
			logArgs = append(logArgs, "post_restore_hooks", strings.Join(c.Hooks.PostRestore, "; "))
		}

	case flagparse.Prune:
		logArgs = append(logArgs, "mode", c.Runtime.Mode)
		logArgs = append(logArgs, "delete_workers", c.Engine.Performance.DeleteWorkers)

		if c.Retention.Incremental.Enabled {
			retentionSummary := fmt.Sprintf("enabled (h:%d d:%d w:%d m:%d y:%d)",
				c.Retention.Incremental.Hours, c.Retention.Incremental.Days, c.Retention.Incremental.Weeks,
				c.Retention.Incremental.Months, c.Retention.Incremental.Years)
			logArgs = append(logArgs, "retention_incremental", retentionSummary)
		}
		if c.Retention.Snapshot.Enabled {
			snapshotRetentionSummary := fmt.Sprintf("enabled (h:%d d:%d w:%d m:%d y:%d)",
				c.Retention.Snapshot.Hours, c.Retention.Snapshot.Days, c.Retention.Snapshot.Weeks,
				c.Retention.Snapshot.Months, c.Retention.Snapshot.Years)
			logArgs = append(logArgs, "retention_snapshot", snapshotRetentionSummary)
		}
	case flagparse.List:
		logArgs = append(logArgs, "mode", c.Runtime.Mode)
		logArgs = append(logArgs, "sort", c.Runtime.ListSort)
	}
	plog.Info("Configuration loaded", logArgs...)

	if c.Sync.Enabled && c.Sync.DisableSafeCopy && (command == flagparse.Backup || command == flagparse.Restore) {
		plog.Warn("Safe Copy is DISABLED. Atomicity is not guaranteed. Power loss during backup/restore may result in corrupt files.")
	}
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
func (s *SyncConfig) ExcludeFiles() []string {
	return util.MergeAndDeduplicate(systemExcludeFilePatterns, s.DefaultExcludeFiles, s.UserExcludeFiles)
}

// ExcludeDirs returns the final, combined slice of directory exclusion patterns, including
// non-overridable system patterns, default patterns, and user-configured patterns.
// It automatically handles deduplication.
func (s *SyncConfig) ExcludeDirs() []string {
	return util.MergeAndDeduplicate(systemExcludeDirPatterns, s.DefaultExcludeDirs, s.UserExcludeDirs)
}

// MergeConfigWithFlags overlays the configuration values from flags on top of a base
// configuration. It iterates over the setFlags map, which contains only the flags
// explicitly provided by the user on the command line.
func MergeConfigWithFlags(command flagparse.Command, base Config, setFlags map[string]any) Config {
	merged := base

	for name, value := range setFlags {
		switch name {
		case "log-level":
			merged.LogLevel = value.(string)
		case "fail-fast":
			merged.Engine.FailFast = value.(bool)
		case "metrics":
			merged.Engine.Metrics = value.(bool)
		case "mode":
			if command != flagparse.Init {
				merged.Runtime.Mode = value.(string)
			}
		case "sort":
			merged.Runtime.ListSort = value.(string)
		case "dry-run":
			merged.Runtime.DryRun = value.(bool)
		case "overwrite":
			switch command {
			case flagparse.Restore:
				merged.Runtime.RestoreOverwriteBehavior = value.(string)
			case flagparse.Backup:
				merged.Runtime.BackupOverwriteBehavior = value.(string)
			}
		case "sync-workers":
			merged.Engine.Performance.SyncWorkers = value.(int)
		case "mirror-workers":
			merged.Engine.Performance.MirrorWorkers = value.(int)
		case "delete-workers":
			merged.Engine.Performance.DeleteWorkers = value.(int)
		case "compress-workers":
			merged.Engine.Performance.CompressWorkers = value.(int)
		case "buffer-size-kb":
			merged.Engine.Performance.BufferSizeKB = value.(int64)
		case "readahead-limit-kb":
			merged.Engine.Performance.ReadAheadLimitKB = value.(int64)
		case "sync-engine":
			merged.Sync.Engine = value.(string)
		case "sync-disable-safe-copy":
			merged.Sync.DisableSafeCopy = value.(bool)
		case "sync-retry-count":
			merged.Sync.RetryCount = value.(int)
		case "sync-retry-wait":
			merged.Sync.RetryWaitSeconds = value.(int)
		case "sync-mod-time-window":
			merged.Sync.ModTimeWindowSeconds = value.(int)
		case "sync-preserve-source-dir-name":
			merged.Sync.PreserveSourceDirName = value.(bool)
		case "user-exclude-files":
			merged.Sync.UserExcludeFiles = value.([]string)
		case "user-exclude-dirs":
			merged.Sync.UserExcludeDirs = value.([]string)
		case "pre-backup-hooks":
			merged.Hooks.PreBackup = value.([]string)
		case "post-backup-hooks":
			merged.Hooks.PostBackup = value.([]string)
		case "pre-restore-hooks":
			merged.Hooks.PreRestore = value.([]string)
		case "post-restore-hooks":
			merged.Hooks.PostRestore = value.([]string)
		case "archive":
			merged.Archive.Enabled = value.(bool)
		case "archive-interval-mode":
			merged.Archive.IntervalMode = value.(string)
		case "archive-interval-seconds":
			merged.Archive.IntervalSeconds = value.(int)
		case "retention-incremental":
			merged.Retention.Incremental.Enabled = value.(bool)
		case "retention-incremental-hours":
			merged.Retention.Incremental.Hours = value.(int)
		case "retention-incremental-days":
			merged.Retention.Incremental.Days = value.(int)
		case "retention-incremental-weeks":
			merged.Retention.Incremental.Weeks = value.(int)
		case "retention-incremental-months":
			merged.Retention.Incremental.Months = value.(int)
		case "retention-incremental-years":
			merged.Retention.Incremental.Years = value.(int)
		case "retention-snapshot":
			merged.Retention.Snapshot.Enabled = value.(bool)
		case "retention-snapshot-hours":
			merged.Retention.Snapshot.Hours = value.(int)
		case "retention-snapshot-days":
			merged.Retention.Snapshot.Days = value.(int)
		case "retention-snapshot-weeks":
			merged.Retention.Snapshot.Weeks = value.(int)
		case "retention-snapshot-months":
			merged.Retention.Snapshot.Months = value.(int)
		case "retention-snapshot-years":
			merged.Retention.Snapshot.Years = value.(int)
		case "compression":
			merged.Compression.Enabled = value.(bool)
		case "compression-format":
			merged.Compression.Format = value.(string)
		case "compression-level":
			merged.Compression.Level = value.(string)
		case "ignore-case-mismatch":
			merged.Runtime.IgnoreCaseMismatch = value.(bool)
		default:
			plog.Debug("unhandled flag in MergeConfigWithFlags", "flag", name)
		}
	}
	return merged
}
