package engine

import (
	"context"
	"os/exec"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/patharchive"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/pathretention"
	"github.com/paulschiretz/pgl-backup/pkg/pathsync"
	"github.com/paulschiretz/pgl-backup/pkg/preflight"
)

// Syncer is an interface for the preflight leaf package.
type Validator interface {
	Run(ctx context.Context, absSourcePath, absTargetPath string, p *preflight.Plan, timestampUTC time.Time) error
}

// Syncer is an interface for the sync leaf package.
type Syncer interface {
	Sync(ctx context.Context, absBasePath, absSourcePath, relCurrentPathKey, relContentPathKey string, p *pathsync.Plan, timestampUTC time.Time) (metafile.MetafileInfo, error)
	Restore(ctx context.Context, absBasePath string, relContentPathKey string, toRestore metafile.MetafileInfo, absRestoreTargetPath string, p *pathsync.Plan) error
}

// Archiver is an interface for the archive leaf package.
// Responsible for turning the the 'current' state into a permanent historical record.
type Archiver interface {
	Archive(ctx context.Context, absBasePath, relArchivePathKey, archiveEntryPrefix string, toArchive metafile.MetafileInfo, p *patharchive.Plan, timestampUTC time.Time) (metafile.MetafileInfo, error)
}

// Retainer is an interface for the retention leaf package.
type Retainer interface {
	Prune(ctx context.Context, absBasePath string, toPrune []metafile.MetafileInfo, p *pathretention.Plan, timestampUTC time.Time) error
}

type Compressor interface {
	Compress(ctx context.Context, absBasePath, relContentPathKey string, toCompress metafile.MetafileInfo, p *pathcompression.CompressPlan, timestampUTC time.Time) error
	Extract(ctx context.Context, absBasePath string, toExtract metafile.MetafileInfo, absExtractTargetPath string, p *pathcompression.ExtractPlan, timestampUTC time.Time) error
}

// Runner is the central orchestrator.
type Runner struct {
	validator  Validator
	syncer     Syncer
	archiver   Archiver
	retainer   Retainer
	compressor Compressor

	// hookCommandExecutor allows mocking os/exec for testing hooks.
	hookCommandExecutor func(ctx context.Context, name string, arg ...string) *exec.Cmd
}

// NewRunner creates a new engine instance.
func NewRunner(v Validator, s Syncer, a Archiver, r Retainer, c Compressor) *Runner {
	return &Runner{
		validator:           v,
		syncer:              s,
		archiver:            a,
		retainer:            r,
		compressor:          c,
		hookCommandExecutor: exec.CommandContext,
	}
}

// SetHookCommandExecutor sets the command executor for hooks.
// This is intended for testing purposes only to mock os/exec.
func (r *Runner) SetHookCommandExecutor(executor func(ctx context.Context, name string, arg ...string) *exec.Cmd) {
	r.hookCommandExecutor = executor
}
