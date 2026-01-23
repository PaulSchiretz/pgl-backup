package planner

import (
	"fmt"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/patharchive"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/pathretention"
	"github.com/paulschiretz/pgl-backup/pkg/pathsync"
	"github.com/paulschiretz/pgl-backup/pkg/preflight"
)

type BackupPlan struct {
	Mode     Mode
	DryRun   bool
	FailFast bool
	Metrics  bool

	Paths PathKeys

	Preflight   *preflight.Plan
	Sync        *pathsync.Plan
	Archive     *patharchive.Plan
	Retention   *pathretention.Plan
	Compression *pathcompression.CompressPlan

	PreBackupHooks  []string
	PostBackupHooks []string
}

type RestorePlan struct {
	Mode     Mode
	DryRun   bool
	FailFast bool
	Metrics  bool

	Paths PathKeys

	Preflight  *preflight.Plan
	Sync       *pathsync.Plan
	Extraction *pathcompression.ExtractPlan

	PreRestoreHooks  []string
	PostRestoreHooks []string
}

type PrunePlan struct {
	DryRun   bool
	FailFast bool
	Metrics  bool

	PathsIncremental PathKeys
	PathsSnapshot    PathKeys

	Preflight            *preflight.Plan
	RetentionIncremental *pathretention.Plan
	RetentionSnapshot    *pathretention.Plan
}

type PathKeys struct {
	RelCurrentPathKey string
	RelArchivePathKey string
	RelContentPathKey string
	BackupDirPrefix   string
}

func GenerateBackupPlan(cfg config.Config) (*BackupPlan, error) {

	// Global Flags
	dryRun := cfg.Runtime.DryRun
	failFast := cfg.Engine.FailFast
	metrics := cfg.Engine.Metrics

	mode, err := ParseMode(cfg.Runtime.Mode)
	if err != nil {
		return nil, err
	}

	// Identify which config block to use based on mode
	var (
		pathCfg         config.PathConfig
		retentionPolicy config.RetentionPolicy
	)

	switch mode {
	case Incremental:
		pathCfg = cfg.Paths.Incremental
		retentionPolicy = cfg.Retention.Incremental
	case Snapshot:
		pathCfg = cfg.Paths.Snapshot
		retentionPolicy = cfg.Retention.Snapshot
	default:
		return nil, fmt.Errorf("unsupported mode: %s", mode)
	}

	// Parse Sync Settings (Shared)
	syncEngine, err := pathsync.ParseEngine(cfg.Sync.Engine)
	if err != nil {
		return nil, err
	}

	syncExcludeFiles := cfg.Sync.ExcludeFiles()
	syncExcludeDirs := cfg.Sync.ExcludeDirs()

	// Parse Compression Settings (Shared)
	compressionFormat, err := pathcompression.ParseFormat(cfg.Compression.Format)
	if err != nil {
		return nil, err
	}

	compressionLevel, err := pathcompression.ParseLevel(cfg.Compression.Level)
	if err != nil {
		return nil, err
	}

	// Parse Overwrite Settings (Shared)
	syncOverwriteBehavior, err := pathsync.ParseOverwriteBehavior(cfg.Runtime.BackupOverwriteBehavior)
	if err != nil {
		return nil, err
	}

	// Prepare Archive Plan based on Mode
	var archivePlan *patharchive.Plan
	if mode == Incremental {
		// Incremental: Use configured archive settings and retention constraints
		archiveIntervalMode, err := patharchive.ParseIntervalMode(cfg.Archive.IntervalMode)
		if err != nil {
			return nil, err
		}

		var archiveIntervalConstraints patharchive.IntervalModeConstraints
		if retentionPolicy.Enabled {
			archiveIntervalConstraints = patharchive.IntervalModeConstraints{
				Hours:  retentionPolicy.Hours,
				Days:   retentionPolicy.Days,
				Weeks:  retentionPolicy.Weeks,
				Months: retentionPolicy.Months,
				Years:  retentionPolicy.Years,
			}
		}

		archivePlan = &patharchive.Plan{
			Enabled:         cfg.Archive.Enabled,
			IntervalSeconds: cfg.Archive.IntervalSeconds,
			IntervalMode:    archiveIntervalMode,
			Constraints:     archiveIntervalConstraints,
			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		}
	} else {
		// Snapshot: Always enabled, manual mode, 0 interval (immediate)
		archivePlan = &patharchive.Plan{
			Enabled:         true,
			IntervalSeconds: 0,
			IntervalMode:    patharchive.Manual,
			Constraints:     patharchive.IntervalModeConstraints{},
			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		}
	}

	// finish the plan
	return &BackupPlan{

		Mode:     mode,
		DryRun:   dryRun,
		Metrics:  metrics,
		FailFast: failFast,

		PreBackupHooks:  cfg.Hooks.PreBackup,
		PostBackupHooks: cfg.Hooks.PostBackup,

		Paths: PathKeys{
			RelCurrentPathKey: pathCfg.Current,
			RelArchivePathKey: pathCfg.Archive,
			RelContentPathKey: pathCfg.Content,
			BackupDirPrefix:   pathCfg.BackupDirPrefix,
		},
		Preflight: &preflight.Plan{
			SourceAccessible:   true,
			TargetAccessible:   true,
			TargetWriteable:    true,
			CaseMismatch:       true,
			PathNesting:        true,
			EnsureTargetExists: true,
			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		Sync: &pathsync.Plan{
			Enabled:               cfg.Sync.Enabled,
			ModeIdentifier:        mode.String(),
			Engine:                syncEngine,
			ExcludeDirs:           syncExcludeDirs,
			ExcludeFiles:          syncExcludeFiles,
			PreserveSourceDirName: cfg.Sync.PreserveSourceDirName,
			Mirror:                true,

			RetryCount:        cfg.Sync.RetryCount,
			RetryWait:         time.Duration(cfg.Sync.RetryWaitSeconds) * time.Second,
			ModTimeWindow:     time.Duration(cfg.Sync.ModTimeWindowSeconds) * time.Second,
			OverwriteBehavior: syncOverwriteBehavior,

			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		Archive: archivePlan,
		Retention: &pathretention.Plan{
			Enabled: retentionPolicy.Enabled,
			Hours:   retentionPolicy.Hours,
			Days:    retentionPolicy.Days,
			Weeks:   retentionPolicy.Weeks,
			Months:  retentionPolicy.Months,
			Years:   retentionPolicy.Years,
			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		Compression: &pathcompression.CompressPlan{
			Enabled: cfg.Compression.Enabled,
			Format:  compressionFormat,
			Level:   compressionLevel,
			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
	}, nil
}

func GenerateRestorePlan(cfg config.Config) (*RestorePlan, error) {
	// Global Flags
	dryRun := cfg.Runtime.DryRun
	failFast := cfg.Engine.FailFast
	metrics := cfg.Engine.Metrics

	mode, err := ParseMode(cfg.Runtime.Mode)
	if err != nil {
		return nil, err
	}

	var pathCfg config.PathConfig
	switch mode {
	case Incremental:
		pathCfg = cfg.Paths.Incremental
	case Snapshot:
		pathCfg = cfg.Paths.Snapshot
	default:
		return nil, fmt.Errorf("unsupported mode: %s", mode)
	}

	// Parse Sync Settings (Shared)
	syncEngine, err := pathsync.ParseEngine(cfg.Sync.Engine)
	if err != nil {
		return nil, err
	}

	syncExcludeFiles := cfg.Sync.ExcludeFiles()
	syncExcludeDirs := cfg.Sync.ExcludeDirs()

	// Parse Overwrite Settings (Shared)
	syncOverwriteBehavior, err := pathsync.ParseOverwriteBehavior(cfg.Runtime.RestoreOverwriteBehavior)
	if err != nil {
		return nil, err
	}

	extractOverwriteBehavior, err := pathcompression.ParseOverwriteBehavior(cfg.Runtime.RestoreOverwriteBehavior)
	if err != nil {
		return nil, err
	}

	// finish the plan
	return &RestorePlan{

		Mode:     mode,
		DryRun:   dryRun,
		Metrics:  metrics,
		FailFast: failFast,

		Paths: PathKeys{
			RelCurrentPathKey: pathCfg.Current,
			RelArchivePathKey: pathCfg.Archive,
			RelContentPathKey: pathCfg.Content,
			BackupDirPrefix:   pathCfg.BackupDirPrefix,
		},

		PreRestoreHooks:  cfg.Hooks.PreRestore,
		PostRestoreHooks: cfg.Hooks.PostRestore,

		Preflight: &preflight.Plan{
			SourceAccessible:   true,
			TargetAccessible:   true,
			TargetWriteable:    true,
			CaseMismatch:       true,
			PathNesting:        true,
			EnsureTargetExists: true,
			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		Sync: &pathsync.Plan{
			Enabled:               cfg.Sync.Enabled,
			ModeIdentifier:        mode.String(),
			Engine:                syncEngine,
			ExcludeDirs:           syncExcludeDirs,
			ExcludeFiles:          syncExcludeFiles,
			PreserveSourceDirName: false, // Force false for restore to avoid creating PGL_Backup_Content subdir
			Mirror:                false, // Force false for restore, we don't want to delete anything in the users retore target

			RetryCount:        cfg.Sync.RetryCount,
			RetryWait:         time.Duration(cfg.Sync.RetryWaitSeconds) * time.Second,
			ModTimeWindow:     time.Duration(cfg.Sync.ModTimeWindowSeconds) * time.Second,
			OverwriteBehavior: syncOverwriteBehavior,

			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		Extraction: &pathcompression.ExtractPlan{
			Enabled:           cfg.Compression.Enabled,
			OverwriteBehavior: extractOverwriteBehavior,
			ModTimeWindow:     time.Duration(cfg.Sync.ModTimeWindowSeconds) * time.Second,
			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
	}, nil
}

func GeneratePrunePlan(cfg config.Config) (*PrunePlan, error) {

	// Global Flags
	dryRun := cfg.Runtime.DryRun
	failFast := cfg.Engine.FailFast
	metrics := cfg.Engine.Metrics

	// finish the plan
	return &PrunePlan{
		DryRun:   dryRun,
		Metrics:  metrics,
		FailFast: failFast,

		Preflight: &preflight.Plan{
			SourceAccessible:   false,
			TargetAccessible:   true,
			TargetWriteable:    true,
			CaseMismatch:       false,
			PathNesting:        false,
			EnsureTargetExists: false,

			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},

		PathsIncremental: PathKeys{
			RelCurrentPathKey: cfg.Paths.Incremental.Current,
			RelArchivePathKey: cfg.Paths.Incremental.Archive,
			RelContentPathKey: cfg.Paths.Incremental.Content,
			BackupDirPrefix:   cfg.Paths.Incremental.BackupDirPrefix,
		},

		RetentionIncremental: &pathretention.Plan{
			Enabled: cfg.Retention.Incremental.Enabled,
			Hours:   cfg.Retention.Incremental.Hours,
			Days:    cfg.Retention.Incremental.Days,
			Weeks:   cfg.Retention.Incremental.Weeks,
			Months:  cfg.Retention.Incremental.Months,
			Years:   cfg.Retention.Incremental.Years,

			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},

		PathsSnapshot: PathKeys{
			RelCurrentPathKey: cfg.Paths.Snapshot.Current,
			RelArchivePathKey: cfg.Paths.Snapshot.Archive,
			RelContentPathKey: cfg.Paths.Snapshot.Content,
			BackupDirPrefix:   cfg.Paths.Snapshot.BackupDirPrefix,
		},

		RetentionSnapshot: &pathretention.Plan{
			Enabled: cfg.Retention.Snapshot.Enabled,
			Hours:   cfg.Retention.Snapshot.Hours,
			Days:    cfg.Retention.Snapshot.Days,
			Weeks:   cfg.Retention.Snapshot.Weeks,
			Months:  cfg.Retention.Snapshot.Months,
			Years:   cfg.Retention.Snapshot.Years,

			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
	}, nil
}
