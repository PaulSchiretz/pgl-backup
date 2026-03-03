package planner

import (
	"fmt"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/hook"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/pathretention"
	"github.com/paulschiretz/pgl-backup/pkg/pathrotation"
	"github.com/paulschiretz/pgl-backup/pkg/pathsync"
	"github.com/paulschiretz/pgl-backup/pkg/preflight"
)

type BackupPlan struct {
	Mode  Mode
	Paths PathKeys

	Preflight   *preflight.Plan
	Sync        *pathsync.Plan
	Rotation    *pathrotation.Plan
	Retention   *pathretention.Plan
	Compression *pathcompression.CompressPlan

	HookRunner *hook.Plan
}

type ListPlan struct {
	Mode      Mode
	DryRun    bool
	SortOrder SortOrder
	FailFast  bool
	Metrics   bool

	PathsIncremental PathKeys
	PathsSnapshot    PathKeys

	Preflight *preflight.Plan
}

type RestorePlan struct {
	Mode Mode

	PathsIncremental PathKeys
	PathsSnapshot    PathKeys

	Preflight  *preflight.Plan
	Sync       *pathsync.Plan
	Extraction *pathcompression.ExtractPlan

	HookRunner *hook.Plan
}

type PrunePlan struct {
	Mode Mode

	PathsIncremental PathKeys
	PathsSnapshot    PathKeys

	Preflight            *preflight.Plan
	RetentionIncremental *pathretention.Plan
	RetentionSnapshot    *pathretention.Plan
}

type PathKeys struct {
	RelCurrentPathKey  string
	RelArchivePathKey  string
	ArchiveEntryPrefix string
	RelStagePathKey    string
	StageEntryPrefix   string
	RelContentPathKey  string
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

	// mode needs to be set eather to incremental or snapshot
	switch mode {
	case Incremental:
		pathCfg = cfg.Paths.Incremental
		retentionPolicy = cfg.Retention.Incremental
	case Snapshot:
		pathCfg = cfg.Paths.Snapshot
		retentionPolicy = cfg.Retention.Snapshot
	default:
		return nil, fmt.Errorf("unsupported mode for backup: %s", mode)
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
	var rotationPlan *pathrotation.Plan
	if mode == Incremental {
		// Incremental: Use configured archive settings and retention constraints
		archiveIntervalMode, err := pathrotation.ParseArchiveIntervalMode(cfg.Archive.IntervalMode)
		if err != nil {
			return nil, err
		}

		var archiveIntervalConstraints pathrotation.ArchiveIntervalModeConstraints
		if retentionPolicy.Enabled {
			archiveIntervalConstraints = pathrotation.ArchiveIntervalModeConstraints{
				Hours:  retentionPolicy.Hours,
				Days:   retentionPolicy.Days,
				Weeks:  retentionPolicy.Weeks,
				Months: retentionPolicy.Months,
				Years:  retentionPolicy.Years,
			}
		}

		rotationPlan = &pathrotation.Plan{
			ArchiveEnabled:         cfg.Archive.Enabled,
			ArchiveIntervalSeconds: cfg.Archive.IntervalSeconds,
			ArchiveIntervalMode:    archiveIntervalMode,
			ArchiveConstraints:     archiveIntervalConstraints,
			DryRun:                 dryRun,
			FailFast:               failFast,
			Metrics:                metrics,
		}
	} else {
		// Snapshot: Always enabled, manual mode, 0 interval (immediate)
		rotationPlan = &pathrotation.Plan{
			ArchiveEnabled:         true,
			ArchiveIntervalSeconds: 0,
			ArchiveIntervalMode:    pathrotation.Manual,
			ArchiveConstraints:     pathrotation.ArchiveIntervalModeConstraints{},
			DryRun:                 dryRun,
			FailFast:               failFast,
			Metrics:                metrics,
		}
	}

	// finish the plan
	return &BackupPlan{

		Mode: mode,

		Paths: PathKeys{
			RelCurrentPathKey:  pathCfg.Current,
			RelArchivePathKey:  pathCfg.Archive,
			ArchiveEntryPrefix: pathCfg.ArchiveEntryPrefix,
			RelStagePathKey:    pathCfg.Stage,
			StageEntryPrefix:   pathCfg.StageEntryPrefix,
			RelContentPathKey:  pathCfg.Content,
		},
		Preflight: &preflight.Plan{
			SourceAccessible:   true,
			TargetAccessible:   true,
			TargetWriteable:    true,
			CaseMismatch:       !cfg.Runtime.IgnoreCaseMismatch,
			PathNesting:        true,
			EnsureTargetExists: true,

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

			SafeCopy:          cfg.Sync.SafeCopy,
			SequentialWrite:   cfg.Sync.SequentialWrite,
			RetryCount:        cfg.Sync.RetryCount,
			RetryWait:         time.Duration(cfg.Sync.RetryWaitSeconds) * time.Second,
			ModTimeWindow:     time.Duration(cfg.Sync.ModTimeWindowSeconds) * time.Second,
			OverwriteBehavior: syncOverwriteBehavior,

			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		Rotation: rotationPlan,
		Retention: &pathretention.Plan{
			Enabled: retentionPolicy.Enabled,
			Hours:   retentionPolicy.Hours,
			Days:    retentionPolicy.Days,
			Weeks:   retentionPolicy.Weeks,
			Months:  retentionPolicy.Months,
			Years:   retentionPolicy.Years,

			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		Compression: &pathcompression.CompressPlan{
			Enabled: cfg.Compression.Enabled,
			Format:  compressionFormat,
			Level:   compressionLevel,

			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		HookRunner: &hook.Plan{
			Enabled:          cfg.Hooks.Enabled && (len(cfg.Hooks.PreBackup) > 0 || len(cfg.Hooks.PostBackup) > 0),
			PreHookCommands:  cfg.Hooks.PreBackup,
			PostHookCommands: cfg.Hooks.PostBackup,
			DryRun:           dryRun,
			FailFast:         failFast,
			Metrics:          metrics,
		},
	}, nil
}

func GenerateListPlan(cfg config.Config) (*ListPlan, error) {

	// Global Flags
	dryRun := cfg.Runtime.DryRun
	failFast := cfg.Engine.FailFast
	metrics := cfg.Engine.Metrics

	mode, err := ParseMode(cfg.Runtime.Mode)
	if err != nil {
		return nil, err
	}

	listSort := cfg.Runtime.ListSort
	if listSort == "" {
		listSort = "desc"
	}
	sortOrder, err := ParseSortOrder(listSort)
	if err != nil {
		return nil, err
	}

	// finish the plan
	return &ListPlan{
		Mode:      mode,
		SortOrder: sortOrder,
		DryRun:    dryRun,
		Metrics:   metrics,
		FailFast:  failFast,

		Preflight: &preflight.Plan{
			SourceAccessible:   false,
			TargetAccessible:   true,
			TargetWriteable:    false,
			CaseMismatch:       false,
			PathNesting:        false,
			EnsureTargetExists: false,

			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},

		PathsIncremental: PathKeys{
			RelCurrentPathKey:  cfg.Paths.Incremental.Current,
			RelArchivePathKey:  cfg.Paths.Incremental.Archive,
			ArchiveEntryPrefix: cfg.Paths.Incremental.ArchiveEntryPrefix,
			RelStagePathKey:    cfg.Paths.Incremental.Stage,
			StageEntryPrefix:   cfg.Paths.Incremental.StageEntryPrefix,
			RelContentPathKey:  cfg.Paths.Incremental.Content,
		},

		PathsSnapshot: PathKeys{
			RelCurrentPathKey:  cfg.Paths.Snapshot.Current,
			RelArchivePathKey:  cfg.Paths.Snapshot.Archive,
			ArchiveEntryPrefix: cfg.Paths.Snapshot.ArchiveEntryPrefix,
			RelStagePathKey:    cfg.Paths.Snapshot.Stage,
			StageEntryPrefix:   cfg.Paths.Snapshot.StageEntryPrefix,
			RelContentPathKey:  cfg.Paths.Snapshot.Content,
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
		Mode: mode,

		PathsIncremental: PathKeys{
			RelCurrentPathKey:  cfg.Paths.Incremental.Current,
			RelArchivePathKey:  cfg.Paths.Incremental.Archive,
			ArchiveEntryPrefix: cfg.Paths.Incremental.ArchiveEntryPrefix,
			RelStagePathKey:    cfg.Paths.Incremental.Stage,
			StageEntryPrefix:   cfg.Paths.Incremental.StageEntryPrefix,
			RelContentPathKey:  cfg.Paths.Incremental.Content,
		},

		PathsSnapshot: PathKeys{
			RelCurrentPathKey:  cfg.Paths.Snapshot.Current,
			RelArchivePathKey:  cfg.Paths.Snapshot.Archive,
			ArchiveEntryPrefix: cfg.Paths.Snapshot.ArchiveEntryPrefix,
			RelStagePathKey:    cfg.Paths.Snapshot.Stage,
			StageEntryPrefix:   cfg.Paths.Snapshot.StageEntryPrefix,
			RelContentPathKey:  cfg.Paths.Snapshot.Content,
		},

		Preflight: &preflight.Plan{
			SourceAccessible:   true,
			TargetAccessible:   true,
			TargetWriteable:    true,
			CaseMismatch:       !cfg.Runtime.IgnoreCaseMismatch,
			PathNesting:        true,
			EnsureTargetExists: true,

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

			SafeCopy:          cfg.Sync.SafeCopy,
			SequentialWrite:   cfg.Sync.SequentialWrite,
			RetryCount:        cfg.Sync.RetryCount,
			RetryWait:         time.Duration(cfg.Sync.RetryWaitSeconds) * time.Second,
			ModTimeWindow:     time.Duration(cfg.Sync.ModTimeWindowSeconds) * time.Second,
			OverwriteBehavior: syncOverwriteBehavior,

			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		Extraction: &pathcompression.ExtractPlan{
			Enabled:           cfg.Compression.Enabled,
			OverwriteBehavior: extractOverwriteBehavior,
			ModTimeWindow:     time.Duration(cfg.Sync.ModTimeWindowSeconds) * time.Second,

			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
		HookRunner: &hook.Plan{
			Enabled:          cfg.Hooks.Enabled && (len(cfg.Hooks.PreRestore) > 0 || len(cfg.Hooks.PostRestore) > 0),
			PreHookCommands:  cfg.Hooks.PreRestore,
			PostHookCommands: cfg.Hooks.PostRestore,
			DryRun:           dryRun,
			FailFast:         failFast,
			Metrics:          metrics,
		},
	}, nil
}

func GeneratePrunePlan(cfg config.Config) (*PrunePlan, error) {

	// Global Flags
	dryRun := cfg.Runtime.DryRun
	failFast := cfg.Engine.FailFast
	metrics := cfg.Engine.Metrics

	mode, err := ParseMode(cfg.Runtime.Mode)
	if err != nil {
		return nil, err
	}

	// finish the plan
	return &PrunePlan{
		Mode: mode,
		Preflight: &preflight.Plan{
			SourceAccessible:   false,
			TargetAccessible:   true,
			TargetWriteable:    true,
			CaseMismatch:       false,
			PathNesting:        false,
			EnsureTargetExists: false,

			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},

		PathsIncremental: PathKeys{
			RelCurrentPathKey:  cfg.Paths.Incremental.Current,
			RelArchivePathKey:  cfg.Paths.Incremental.Archive,
			ArchiveEntryPrefix: cfg.Paths.Incremental.ArchiveEntryPrefix,
			RelStagePathKey:    cfg.Paths.Incremental.Stage,
			StageEntryPrefix:   cfg.Paths.Incremental.StageEntryPrefix,
			RelContentPathKey:  cfg.Paths.Incremental.Content,
		},

		RetentionIncremental: &pathretention.Plan{
			Enabled: cfg.Retention.Incremental.Enabled,
			Hours:   cfg.Retention.Incremental.Hours,
			Days:    cfg.Retention.Incremental.Days,
			Weeks:   cfg.Retention.Incremental.Weeks,
			Months:  cfg.Retention.Incremental.Months,
			Years:   cfg.Retention.Incremental.Years,

			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},

		PathsSnapshot: PathKeys{
			RelCurrentPathKey:  cfg.Paths.Snapshot.Current,
			RelArchivePathKey:  cfg.Paths.Snapshot.Archive,
			ArchiveEntryPrefix: cfg.Paths.Snapshot.ArchiveEntryPrefix,
			RelStagePathKey:    cfg.Paths.Snapshot.Stage,
			StageEntryPrefix:   cfg.Paths.Snapshot.StageEntryPrefix,
			RelContentPathKey:  cfg.Paths.Snapshot.Content,
		},

		RetentionSnapshot: &pathretention.Plan{
			Enabled: cfg.Retention.Snapshot.Enabled,
			Hours:   cfg.Retention.Snapshot.Hours,
			Days:    cfg.Retention.Snapshot.Days,
			Weeks:   cfg.Retention.Snapshot.Weeks,
			Months:  cfg.Retention.Snapshot.Months,
			Years:   cfg.Retention.Snapshot.Years,

			DryRun:   dryRun,
			FailFast: failFast,
			Metrics:  metrics,
		},
	}, nil
}
