package pathsync

import (
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
)

type Plan struct {
	Enabled               bool
	ModeIdenifier         string
	Engine                Engine
	PreserveSourceDirName bool

	RetryCount    int
	RetryWait     time.Duration
	ModTimeWindow time.Duration // The time window to consider file modification times equal.

	ExcludeFiles []string
	ExcludeDirs  []string
	ResultInfo   metafile.MetafileInfo

	// Global Flags
	DryRun   bool
	FailFast bool
	Metrics  bool
}
