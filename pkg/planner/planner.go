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
		pathsCfg       config.PathsPolicyConfig
		syncCfg        config.SyncPolicyConfig
		archiveCfg     config.ArchivePolicyConfig
		retentionCfg   config.RetentionPolicyConfig
		compressionCfg config.CompressionPolicyConfig
	)

	switch mode {
	case Incremental:
		pathsCfg = cfg.Paths.Incremental
		syncCfg = cfg.Sync.Incremental
		archiveCfg = cfg.Archive.Incremental
		retentionCfg = cfg.Retention.Incremental
		compressionCfg = cfg.Compression.Incremental
	case Snapshot:
		pathsCfg = cfg.Paths.Snapshot
		syncCfg = cfg.Sync.Snapshot
		archiveCfg = cfg.Archive.Snapshot
		retentionCfg = cfg.Retention.Snapshot
		compressionCfg = cfg.Compression.Snapshot
	default:
		return nil, fmt.Errorf("unsupported mode: %s", mode)
	}

	// Parse values
	archiveIntervalMode, err := patharchive.ParseIntervalMode(archiveCfg.IntervalMode)
	if err != nil {
		return nil, err
	}

	var archiveIntervalConstraints patharchive.IntervalModeConstraints
	if retentionCfg.Enabled {
		archiveIntervalConstraints = patharchive.IntervalModeConstraints{
			Hours:  retentionCfg.Hours,
			Days:   retentionCfg.Days,
			Weeks:  retentionCfg.Weeks,
			Months: retentionCfg.Months,
			Years:  retentionCfg.Years,
		}
	} else {
		archiveIntervalConstraints = patharchive.IntervalModeConstraints{
			Hours:  0,
			Days:   0,
			Weeks:  0,
			Months: 0,
			Years:  0,
		}
	}

	syncEngine, err := pathsync.ParseEngine(syncCfg.Engine)
	if err != nil {
		return nil, err
	}

	syncExcludeFiles := cfg.Sync.ExcludeFiles()
	syncExcludeDirs := cfg.Sync.ExcludeDirs()

	compressionFormat, err := pathcompression.ParseFormat(compressionCfg.Format)
	if err != nil {
		return nil, err
	}

	compressionLevel, err := pathcompression.ParseLevel(compressionCfg.Level)
	if err != nil {
		return nil, err
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
			RelCurrentPathKey: pathsCfg.Current,
			RelArchivePathKey: pathsCfg.Archive,
			RelContentPathKey: pathsCfg.Content,
			BackupDirPrefix:   pathsCfg.BackupDirPrefix,
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
			Enabled:               syncCfg.Enabled,
			ModeIdentifier:        mode.String(),
			Engine:                syncEngine,
			ExcludeDirs:           syncExcludeDirs,
			ExcludeFiles:          syncExcludeFiles,
			PreserveSourceDirName: syncCfg.PreserveSourceDirName,
			Mirror:                true,

			RetryCount:    syncCfg.RetryCount,
			RetryWait:     time.Duration(syncCfg.RetryWaitSeconds) * time.Second,
			ModTimeWindow: time.Duration(syncCfg.ModTimeWindowSeconds) * time.Second,

			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		Archive: &patharchive.Plan{
			Enabled:         archiveCfg.Enabled,
			IntervalSeconds: archiveCfg.IntervalSeconds,
			IntervalMode:    archiveIntervalMode,
			Constraints:     archiveIntervalConstraints,
			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		Retention: &pathretention.Plan{
			Enabled: retentionCfg.Enabled,
			Hours:   retentionCfg.Hours,
			Days:    retentionCfg.Days,
			Weeks:   retentionCfg.Weeks,
			Months:  retentionCfg.Months,
			Years:   retentionCfg.Years,
			// Global Flags
			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		Compression: &pathcompression.CompressPlan{
			Enabled: compressionCfg.Enabled,
			Format:  compressionFormat,
			Level:   compressionLevel,
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
