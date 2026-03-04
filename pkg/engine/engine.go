package engine

import (
	"context"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/hook"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/pathretention"
	"github.com/paulschiretz/pgl-backup/pkg/pathrotation"
	"github.com/paulschiretz/pgl-backup/pkg/pathsync"
	"github.com/paulschiretz/pgl-backup/pkg/preflight"
)

// Validator is an interface for the preflight leaf package.
type Validator interface {
	Run(ctx context.Context, absSourcePath, absTargetPath string, p *preflight.Plan, timestampUTC time.Time) error
}

// Syncer is an interface for the sync leaf package.
type Syncer interface {
	Sync(ctx context.Context, absBasePath, absSourcePath, relCurrentPathKey, relContentPathKey string, p *pathsync.Plan, timestampUTC time.Time) (metafile.MetafileInfo, error)
	Restore(ctx context.Context, absBasePath string, relContentPathKey string, toRestore metafile.MetafileInfo, absRestoreTargetPath string, p *pathsync.Plan, timestampUTC time.Time) error
}

// Rotator is an interface for the rotation leaf package.
// Responsible for turning the the 'current' state into a permanent historical record.
type Rotator interface {
	ShouldArchive(ctx context.Context, toArchive metafile.MetafileInfo, p *pathrotation.Plan, timestampUTC time.Time) (bool, error)
	Archive(ctx context.Context, absBasePath, relArchivePathKey, archiveEntryPrefix string, toArchive metafile.MetafileInfo, p *pathrotation.Plan, timestampUTC time.Time) (metafile.MetafileInfo, error)
	Stage(ctx context.Context, absBasePath, relStagePathKey, stageEntryPrefix string, toStage metafile.MetafileInfo, p *pathrotation.Plan, timestampUTC time.Time) (metafile.MetafileInfo, error)
	Unstage(ctx context.Context, absBasePath string, stagedInfo metafile.MetafileInfo, p *pathrotation.Plan, timestampUTC time.Time) error
	CleanupStagingPath(ctx context.Context, absBasePath, relStagePathKey string, p *pathrotation.Plan, timestampUTC time.Time) error
}

// Retainer is an interface for the retention leaf package.
type Retainer interface {
	Prune(ctx context.Context, absBasePath string, toPrune []metafile.MetafileInfo, p *pathretention.Plan, timestampUTC time.Time) error
}

type Compressor interface {
	Compress(ctx context.Context, absBasePath, relContentPathKey string, toCompress metafile.MetafileInfo, p *pathcompression.CompressPlan, timestampUTC time.Time) error
	Extract(ctx context.Context, absBasePath string, toExtract metafile.MetafileInfo, absExtractTargetPath string, p *pathcompression.ExtractPlan, timestampUTC time.Time) error
}

type HookRunner interface {
	RunPreHook(ctx context.Context, hookName string, p *hook.Plan, timestampUTC time.Time) error
	RunPostHook(ctx context.Context, hookName string, p *hook.Plan, timestampUTC time.Time) error
}

// Runner is the central orchestrator.
type Runner struct {
	validator  Validator
	syncer     Syncer
	rotator    Rotator
	retainer   Retainer
	compressor Compressor
	hookRunner HookRunner
}

// NewRunner creates a new engine instance.
func NewRunner(v Validator, hr HookRunner, s Syncer, ro Rotator, r Retainer, c Compressor) *Runner {
	return &Runner{
		validator:  v,
		syncer:     s,
		rotator:    ro,
		retainer:   r,
		compressor: c,
		hookRunner: hr,
	}
}
